use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;
use serde::{ Serialize, Deserialize };
use std::time::{ Duration, SystemTime, UNIX_EPOCH };
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{ UnboundedSender, UnboundedReceiver };

use crate::server::{ Server, ClientProperties, ClientStats };

#[ derive( Serialize, Clone ) ]
pub struct IcyProperties {
	pub uagent: Option< String >,
	pub public: bool,
	pub name: Option< String >,
	pub description: Option< String >,
	pub url: Option< String >,
	pub genre: Option< String >,
	pub bitrate: Option< String >,
	pub content_type: String
}

impl IcyProperties {
	pub fn new( content_type: String ) -> IcyProperties {
		IcyProperties{
			uagent: None,
			public: false,
			name: None,
			description: None,
			url: None,
			genre: None,
			bitrate: None,
			content_type
		}
	}
}

#[ derive( Serialize, Clone ) ]
pub struct IcyMetadata {
	pub title: Option< String >,
	pub url: Option< String >
}

// TODO Add something determining if a source is a relay, or any other kind of source, for that matter
// TODO Implement hidden sources
pub struct Source {
	// Is setting the mountpoint in the source really useful, since it's not like the source has any use for it
	pub mountpoint: String,
	pub properties: IcyProperties,
	pub metadata: Option< IcyMetadata >,
	pub metadata_vec: Vec< u8 >,
	pub clients: HashMap< Uuid, Arc< RwLock< Client > > >,
	pub burst_buffer: Vec< u8 >,
	pub stats: RwLock< SourceStats >,
	pub fallback: Option< String >,
	// Not really sure how else to signal when to disconnect the source
	pub disconnect_flag: bool
}

impl Source {
	pub fn new( mountpoint: String, properties: IcyProperties ) -> Source {
		Source {
			mountpoint,
			properties,
			metadata: None,
			metadata_vec: vec![ 0 ],
			clients: HashMap::new(),
			burst_buffer: Vec::new(),
			stats: RwLock::new( SourceStats {
				start_time: {
					if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
						time.as_secs()
					} else {
						0
					}
				},
				bytes_read: 0,
				peak_listeners: 0
			} ),
			fallback: None,
			disconnect_flag: false
		}
	}
}

#[ derive( Serialize, Deserialize, Clone ) ]
pub struct SourceStats {
	pub start_time: u64,
	pub bytes_read: usize,
	pub peak_listeners: usize
}

pub struct Client {
	pub source: RwLock< String >,
	pub sender: RwLock< UnboundedSender< Arc< Vec< u8 > > > >,
	pub receiver: RwLock< UnboundedReceiver< Arc< Vec< u8 > > > >,
	pub buffer_size: RwLock< usize >,
	pub properties: ClientProperties,
	pub stats: RwLock< ClientStats >
}

pub fn populate_properties( properties: &mut IcyProperties, headers: &[ httparse::Header< '_ > ] ) {
	for header in headers {
		let name = header.name.to_lowercase();
		let name = name.as_str();
		let val = std::str::from_utf8( header.value ).unwrap_or( "" );

		// There's a nice list here: https://github.com/ben221199/MediaCast
		// Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
		match name {
			"user-agent" => properties.uagent = Some( val.to_string() ),
			"ice-public" | "icy-pub" | "x-audiocast-public" | "icy-public" => properties.public = val.parse::< usize >().unwrap_or( 0 ) == 1,
			"ice-name" | "icy-name" | "x-audiocast-name" => properties.name = Some( val.to_string() ),
			"ice-description" | "icy-description" | "x-audiocast-description" => properties.description = Some( val.to_string() ),
			"ice-url" | "icy-url" | "x-audiocast-url" => properties.url = Some( val.to_string() ),
			"ice-genre" | "icy-genre" | "x-audiocast-genre" => properties.genre = Some( val.to_string() ),
			"ice-bitrate" | "icy-br" | "x-audiocast-bitrate" => properties.bitrate = Some( val.to_string() ),
			_ => (),
		}
	}
}

pub async fn mount_source(sock: &mut TcpStream, arc: &Arc< RwLock< Source > >, server: &Arc< RwLock< Server > >) {
    let ( queue_size, burst_size, source_timeout );
    {
        let source = arc.read().await;
        let serv   = server.read().await;
        queue_size = serv.properties.limits.queue_size;
        let tuple = {
            if let Some( limit ) = serv.properties.limits.source_limits.get( &source.mountpoint ) {
                ( limit.burst_size, limit.source_timeout )
            } else {
                ( serv.properties.limits.burst_size, serv.properties.limits.header_timeout )
            }
        };
        burst_size     = tuple.0;
        source_timeout = tuple.1;
    }

    let mut buf = [ 0; 1024 ];
    // Listen for bytes
    while {
        // Read the incoming stream data until it closes
        let read = match timeout( Duration::from_millis( source_timeout ), async {
            match sock.read( &mut buf ).await {
                Ok( n ) => n,
                Err( e ) => {
                    println!( "An error occured while reading stream data from source {}: {}", arc.read().await.mountpoint, e );
                    0
                }
            }
        } ).await {
            Ok( n ) => n,
            Err( _ ) => {
                println!( "A relay timed out: {}", arc.read().await.mountpoint );
                0
            }
        };

        if read != 0 {
            // Get the slice
            let mut slice: Vec< u8 > = Vec::new();
            slice.extend_from_slice( &buf[ .. read  ] );

            // TODO Add metadata

            broadcast_to_clients( &arc, slice, queue_size, burst_size ).await;
            arc.read().await.stats.write().await.bytes_read += read;
        }

        // Check if the source needs to be disconnected
        read != 0 && !arc.read().await.disconnect_flag
    }  {}

    let mut source = arc.write().await;
    let fallback = source.fallback.clone();
    if let Some( fallback_id ) = fallback {
        if let Some( fallback_source ) = server.read().await.sources.get( &fallback_id ) {
            println!( "Moving listeners from {} to {}", source.mountpoint, fallback_id );
            let mut fallback = fallback_source.write().await;
            for ( uuid, client ) in source.clients.drain() {
                *client.read().await.source.write().await = fallback_id.clone();
                fallback.clients.insert( uuid, client );
            }
        } else {
            println!( "No fallback source {} found! Disconnecting listeners on {}", fallback_id, source.mountpoint );
            for cli in source.clients.values() {
                // Send an empty vec to signify the channel is closed
                drop( cli.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
            }
        }
    } else {
        // Disconnect each client by sending an empty buffer
        println!( "Disconnecting listeners on {}", source.mountpoint );
        for cli in source.clients.values() {
            // Send an empty vec to signify the channel is closed
            drop( cli.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
        }
    }

    // Clean up and remove the source
    let mut serv = server.write().await;
    serv.sources.remove( &source.mountpoint );
    serv.stats.session_bytes_read += source.stats.read().await.bytes_read;

}

async fn broadcast_to_clients( source: &Arc< RwLock< Source > >, data: Vec< u8 >, queue_size: usize, burst_size: usize ) {
	// Remove these later
	let mut dropped: Vec< Uuid > = Vec::new();

	let read = data.len();
	let arc_slice = Arc::new( data );

	// Keep the write lock for the duration of the function, since a race condition with the burst on connect buffer is not wanted
	let mut locked = source.write().await;

	// Broadcast to all listeners
	for ( uuid, cli ) in &locked.clients {
		let client = cli.read().await;
		let mut buf_size = client.buffer_size.write().await;
		let queue = client.sender.write().await;
		if read + ( *buf_size ) > queue_size || queue.send( arc_slice.clone() ).is_err() {
			dropped.push( *uuid );
		} else {
			( *buf_size ) += read;
		}
	}

	// Fill the burst on connect buffer
	if burst_size > 0 {
		let burst_buf = &mut locked.burst_buffer;
		burst_buf.extend_from_slice( &arc_slice );
		// Trim it if it's larger than the allowed size
		if burst_buf.len() > burst_size {
			burst_buf.drain( .. burst_buf.len() - burst_size );
		}
	}

	// Remove clients who have been kicked or disconnected
	for uuid in dropped {
		if let Some( client ) = locked.clients.remove( &uuid ) {
			drop( client.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
		}
	}
}

/**
 * Get a vector containing n and the padded data
 */
pub fn get_metadata_vec( metadata: &Option< IcyMetadata > ) -> Vec< u8 > {
	let mut subvec = vec![ 0 ];
	if let Some( icy_metadata ) = metadata {
		subvec.extend_from_slice( b"StreamTitle='" );
		if let Some( title ) = &icy_metadata.title {
			subvec.extend_from_slice( title.as_bytes() );
		}
		subvec.extend_from_slice( b"';StreamUrl='" );
		if let Some( url ) = &icy_metadata.url {
			subvec.extend_from_slice( url.as_bytes() );
		}
		subvec.extend_from_slice( b"';" );

		// Calculate n
		let len = subvec.len() - 1;
		subvec[ 0 ] = {
			let down = len >> 4;
			let remainder = len & 0b1111;
			if remainder > 0 {
				// Pad with zeroes
				subvec.append( &mut vec![ 0; 16 - remainder ] );
				down + 1
			} else {
				down
			}
		} as u8;
	}

	subvec
}
