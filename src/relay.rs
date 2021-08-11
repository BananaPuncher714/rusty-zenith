use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::net::TcpStream;
use tokio::time::timeout;
use std::error::Error;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use std::time::Duration;
use serde::Deserialize;
use httparse::Status;
use std::io::ErrorKind;

use crate::server::{ Server, get_header };
use crate::config::MasterServer;
use crate::stream::read_http_message;
use crate::icecast::{ populate_properties, IcyProperties, Source, mount_source };


async fn master_server_mountpoints( server: &Arc< RwLock< Server > >, master_info: &MasterServer ) -> Result< Vec<String>, Box< dyn Error > > {
	// Get all master mountpoints
    let ( server_id, header_timeout, http_max_len ) = {
		let properties = &server.read().await.properties;
		( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length )
	};
	let mut sock = TcpStream::connect( format!( "{}:{}", master_info.host, master_info.port ) ).await?;
	sock.write_all( format!( "GET /api/serverinfo HTTP/1.0\r\nUser-Agent: {}\r\nConnection: Closed\r\n\r\n", server_id ).as_bytes() ).await?;

	let mut message = Vec::new();
	// read headers from client
	timeout( Duration::from_millis( header_timeout ), read_http_message( &mut sock, &mut message, http_max_len ) ).await??;

	let mut headers = [ httparse::EMPTY_HEADER; 32 ];
	let mut res = httparse::Response::new( &mut headers );

	if res.parse( &message )? == Status::Partial {
		return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete response" ) ) );
	}

	let mut len = match get_header( "Content-Length", res.headers ) {
		Some( val ) => {
			let parsed = std::str::from_utf8( val )?;
			parsed.parse::< usize >()?
		},
		None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "No Content-Length specified" ) ) )
	};

	match res.code {
		Some( 200 ) => (),
		Some( code ) => return Err( Box::new( std::io::Error::new( ErrorKind::Other, format!( "Invalid response: {}", code ) ) ) ),
		None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Missing response code" ) ) )
	}

	let source_timeout = server.read().await.properties.limits.source_timeout;

	let mut json_slice = Vec::new();
	let mut buf = [ 0; 512 ];
	while len != 0 {
		// Read the incoming stream data until it closes
		let read = timeout( Duration::from_millis( source_timeout ), sock.read( &mut buf ) ).await??;

		// Not guaranteed but most likely EOF or some premature closure
		if read == 0 {
			return Err( Box::new( std::io::Error::new( ErrorKind::UnexpectedEof, "Response body is less than specified" ) ) )
		} else if read > len {
			// Read too much?
			return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Response body is larger than specified" ) ) );
		} else {
			len -= read;
			json_slice.extend_from_slice( &buf[ .. read ] );
		}
	}

	#[ derive( Deserialize ) ]
	struct MasterMounts { mounts: Vec< String > }

	// we either will found mounts or client is not an icecast node?
	Ok( serde_json::from_slice::< MasterMounts >( &json_slice )?.mounts )
}

#[ allow( clippy::map_entry ) ]
async fn relay_mountpoint( server: Arc< RwLock< Server > >, master_server: MasterServer, mount: String ) -> Result< (), Box< dyn Error > > {
    let ( server_id, header_timeout, http_max_len ) = {
		let properties = &server.read().await.properties;
		( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length )
	};
	let host = master_server.host;
	let port = master_server.port;

	let mut sock = TcpStream::connect( format!("{}:{}", host, port ) ).await?;
	sock.write_all( format!( "GET /{} HTTP/1.0\r\nUser-Agent: {}\r\nConnection: Closed\r\n\r\n", mount, server_id ).as_bytes() ).await?;

	let mut buf = Vec::new();
	// read headers from server
	timeout( Duration::from_millis( header_timeout ), read_http_message( &mut sock, &mut buf, http_max_len ) ).await??;

	let mut headers = [ httparse::EMPTY_HEADER; 32 ];
	let mut res = httparse::Response::new( &mut headers );

    if res.parse( &buf )? == Status::Partial {
		return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete response" ) ) );
	}

	match res.code {
		Some( 200 ) => (),
        Some( code ) => return Err( Box::new( std::io::Error::new( ErrorKind::Other, format!( "Invalid response: {}", code ) ) ) ),
        None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Missing response code" ) ) )
	}

	// checking if our peer is really an icecast server
    if get_header( "icy-name", res.headers ).is_none() {
		return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Is this a valid icecast stream?" ) ) );
	}

	// Sources must have a content type
	let mut properties = match get_header( "Content-Type", res.headers ) {
        Some( content_type ) => IcyProperties::new( std::str::from_utf8( content_type )?.to_string() ),
		None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "No Content-Type provided" ) ) )
	};

	// Parse the headers for the source properties
	populate_properties( &mut properties, res.headers );
	properties.uagent = Some( server_id );

	let source = Source::new( mount.to_string(), properties );

	// TODO This code is almost an exact replica of the one used for regular source handling, although with a few differences
	let mut serv = server.write().await;
	// Check if the mountpoint is already in use
	let path = source.mountpoint.clone();
	// Not sure what clippy wants, https://rust-lang.github.io/rust-clippy/master/#map_entry
	// The source is not needed if the map already has one. TODO Try using try_insert if it's stable in the future
	if serv.sources.contains_key( &path ) {
		// The error handling in this program is absolutely awful
		Err( Box::new( std::io::Error::new( ErrorKind::Other, "A source with the same mountpoint already exists" ) ) )
	} else {
		if serv.relay_count >= master_server.relay_limit || serv.sources.len() >= serv.properties.limits.total_sources {
			return Err( Box::new( std::io::Error::new( ErrorKind::Other, "The server relay limit has been reached" ) ) );
		} else if serv.sources.len() >= serv.properties.limits.total_sources {
			return Err( Box::new( std::io::Error::new( ErrorKind::Other, "The server total source limit has been reached" ) ) );
		}

		// Add to the server
		let arc = Arc::new( RwLock::new( source ) );
		serv.sources.insert( path, arc.clone() );
		serv.relay_count += 1;
		drop( serv );

        println!( "Mounted relay on {}", arc.read().await.mountpoint );
        mount_source(&mut sock, &arc, &server).await;
        println!( "Unmounted relay {}", arc.read().await.mountpoint );

        let mut serv = server.write().await;
        serv.relay_count -= 1;

		Ok( () )
	}
}

pub async fn slave_node( server: Arc< RwLock< Server > >, master_server: MasterServer ) {
	/*
		Master-slave polling
		We will retrieve mountpoints from master node every update_interval and mount them in slave node.
		If mountpoint already exists in slave node (ie. a source uses same mountpoint that also exists in master node),
		then we will ignore that mountpoint from master
	*/
	loop {
		// first we retrieve mountpoints from master
		let mut mounts = Vec::new();
		match master_server_mountpoints( &server, &master_server ).await {
			Ok( v ) => mounts.extend( v ),
			Err( e ) => println!( "Error while fetching mountpoints from {}:{}: {}", master_server.host, master_server.port, e )
		}

		for mount in mounts {
			let path = {
				// Remove the trailing '/'
				if mount.ends_with( '/' ) {
					let mut chars = mount.chars();
					chars.next_back();
					chars.collect()
				} else {
					mount.to_string()
				}
			};

			// Check if the path contains 'admin' or 'api'
			// TODO Allow for custom stream directory, such as http://example.com/stream/radio
			if path == "/admin" ||
					path.starts_with( "/admin/" ) ||
					path == "/api" ||
					path.starts_with( "/api/" ) {
				println!( "Attempted to mount a relay at an invalid mountpoint: {}", path );
				continue;
			}

			if server.read().await.sources.contains_key( &path ) {
				// mount already exists
				continue;
			}

			// trying to mount all mounts from master
			let server_clone = server.clone();
			let master_clone = master_server.clone();
			tokio::spawn( async move {
                let host = master_clone.host.clone();
				let port = master_clone.port;
				if let Err( e ) = relay_mountpoint( server_clone, master_clone, path.clone() ).await {
					println!( "An error occured while relaying {} from {}:{}: {}", path, host, port, e );
				}
			} );
		}

		// update interval
		tokio::time::sleep(tokio::time::Duration::from_secs( master_server.update_interval ) ).await;
	}
}
