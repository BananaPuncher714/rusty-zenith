use httpdate::fmt_http_date;
use regex::Regex;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{ BufWriter, ErrorKind, Write };
use std::net::{ SocketAddr, IpAddr, Ipv4Addr };
use std::path::Path;
use std::sync::Arc;
use std::time::{ Duration, SystemTime };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use tokio::net::{ TcpListener, TcpStream };
use tokio::sync::RwLock;
use tokio::sync::mpsc::{ UnboundedSender, UnboundedReceiver, unbounded_channel };
use tokio::time::timeout;
use uuid::Uuid;

// Default constants
const PORT: u16 = 8000;
// The default interval in bytes between icy metadata chunks
// The metaint cannot be changed per client once the response has been sent
// https://thecodeartist.blogspot.com/2013/02/shoutcast-internet-radio-protocol.html
const METAINT: usize = 16_000;
// Server that gets sent in the header
const SERVER_ID: &str = "Rusty Zenith 0.1.0";
// Contact information
const ADMIN: &str = "admin@localhost";
// Public facing domain/address
const HOST: &str = "localhost";
// Geographic location. Icecast included it in their settings, so why not
const LOCATION: &str = "1.048596";

// How many sources can be connected, in total
const SOURCES: usize = 4;
// How many clients can be connected, in total
const CLIENTS: usize = 400;
// How many bytes a client can have queued until they get disconnected
const QUEUE_SIZE: usize = 102400;
// How many bytes to send to the client all at once when they first connect
// Useful for filling up the client buffer quickly, but also introduces some delay
const BURST_SIZE: usize = 65536;
// How long in milliseconds a client has to send a complete request
const HEADER_TIMEOUT: u64 = 15_000;
// How long in milliseconds a source has to send something before being disconnected
const SOURCE_TIMEOUT: u64 = 10_000;

#[ derive( Clone ) ]
struct Query {
	field: String,
	value: String
}

// TODO Add stats
struct Client {
	sender: RwLock< UnboundedSender< Arc< Vec< u8 > > > >,
	receiver: RwLock< UnboundedReceiver< Arc< Vec< u8 > > > >,
	buffer_size: RwLock< usize >,
	properties: ClientProperties
}

#[ derive( Clone ) ]
struct ClientProperties {
	id: Uuid,
	uagent: Option< String >,
	metadata: bool
}

struct IcyProperties {
	uagent: Option< String >,
	public: bool,
	name: Option< String >,
	description: Option< String >,
	url: Option< String >,
	genre: Option< String >,
	bitrate: Option< String >,
	content_type: String
}

#[ derive( Clone ) ]
struct IcyMetadata {
	title: Option< String >,
	url: Option< String >
}

impl IcyProperties {
	fn new( content_type: String ) -> IcyProperties {
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

// TODO Add stats
struct Source {
	// Is setting the mountpoint in the source really useful, since it's not like the source has any use for it
	mountpoint: String,
	properties: IcyProperties,
	metadata: Option< IcyMetadata >,
	metadata_vec: Vec< u8 >,
	clients: HashMap< Uuid, Arc< RwLock< Client > > >,
	burst_buffer: Vec< u8 >
}

// TODO Add permissions
#[ derive( Serialize, Deserialize, Clone ) ]
struct Credential {
	username: String,
	password: String
}

#[ derive( Serialize, Deserialize, Copy, Clone ) ]
struct ServerLimits {
	#[ serde( default = "default_property_limits_clients" ) ]
	clients: usize,
	#[ serde( default = "default_property_limits_sources" ) ]
	sources: usize,
	#[ serde( default = "default_property_limits_queue_size" ) ]
	queue_size: usize,
	#[ serde( default = "default_property_limits_burst_size" ) ]
	burst_size: usize,
	#[ serde( default = "default_property_limits_header_timeout" ) ]
	header_timeout: u64,
	#[ serde( default = "default_property_limits_source_timeout" ) ]
	source_timeout: u64
}

#[ derive( Serialize, Deserialize, Clone ) ]
struct ServerProperties {
	#[ serde( default = "default_property_port" ) ]
	port: u16,
	#[ serde( default = "default_property_metaint" ) ]
	metaint: usize,
	#[ serde( default = "default_property_server_id" ) ]
	server_id: String,
	#[ serde( default = "default_property_admin" ) ]
	admin: String,
	#[ serde( default = "default_property_host" ) ]
	host: String,
	#[ serde( default = "default_property_location" ) ]
	location: String,
	#[ serde( default = "default_property_limits" ) ]
	limits: ServerLimits,
	#[ serde( default = "default_property_users" ) ]
	users: Vec< Credential >
}

impl ServerProperties {
	fn new() -> ServerProperties {
		ServerProperties {
			port: default_property_port(),
			metaint: default_property_metaint(),
			server_id: default_property_server_id(),
			admin: default_property_admin(),
			host: default_property_host(),
			location: default_property_location(),
			limits: default_property_limits(),
			users: default_property_users()
		}
	}
}

// TODO Add stats
struct Server {
	sources: HashMap< String, Arc< RwLock< Source > > >,
	clients: HashMap< Uuid, ClientProperties >,
	properties: ServerProperties
}

impl Server {
	fn new( properties: ServerProperties ) -> Server {
		Server{
			sources: HashMap::new(),
			clients: HashMap::new(),
			properties
		}
	}
}

async fn handle_connection( server: Arc< RwLock< Server > >, mut stream: TcpStream ) -> Result< (), Box< dyn Error > > {
	let mut buf = Vec::new();
	let mut buffer = [ 0; 512 ];

	let header_timeout = server.read().await.properties.limits.header_timeout;

	// Add a timeout
	match timeout( Duration::from_millis( header_timeout ), async {
		// Get the header
		while {
			let mut headers = [ httparse::EMPTY_HEADER; 32 ];
			let mut req = httparse::Request::new( &mut headers );
			match stream.read( &mut buffer ).await {
				Ok( read ) => buf.extend_from_slice( &buffer[ .. read ] ),
				Err( e ) => {
					println!( "An error occured while reading a request: {}", e );
					return Err( e )
				}
			}
			
			match req.parse( &buf ) {
				Ok( res ) => res.is_partial(),
				Err( e ) => {
					println!( "An error occured while parsing a request: {}", e );
					return Err( std::io::Error::new( ErrorKind::Other, "Failed to parse an invalid request" ) )
				}
			}
		} {}
		
		Ok( () )
	} ).await {
		Ok( res ) => {
			if res.is_err() {
				return Ok( () )
			}
		}
		Err( _ ) => {
			println!( "An incoming request failed to complete in time" );
			return Ok( () )
		}
	}

	let mut headers = [ httparse::EMPTY_HEADER; 32 ];
	let mut req = httparse::Request::new( &mut headers );
	let body_offset = req.parse( &buf ).unwrap().unwrap();

	let method = req.method.unwrap();
	let ( base_path, queries ) = extract_queries( req.path.unwrap() );
	let path = path_clean::clean( base_path );
	
	let server_id = {
		server.read().await.properties.server_id.clone()
	};
	
	match method {
		// Some info about the protocol is provided here: https://gist.github.com/ePirat/adc3b8ba00d85b7e3870
		"SOURCE" => {
			// Check for authorization
			if let Some( ( name, pass ) ) = get_basic_auth( req.headers ) {
				if !validate_user( &server.read().await.properties, name, pass ) {
					// Invalid user/pass provided
					send_unauthorized( &mut stream, server_id, Some( ( "text/plain; charset=urf-8", "Invalid credentials" ) ) ).await?;
					return Ok( () )
				}
			} else {
				// No auth, return and close
				send_unauthorized( &mut stream, server_id, Some( ( "text/plain; charset=urf-8", "You need to authenticate" ) ) ).await?;
				return Ok( () )
			}
			
			// http://example.com/radio == http://example.com/radio/
			// Not sure if this is done client-side prior to sending the request though
			let path = {
				// Remove the trailing '/'
				if path.ends_with( '/' ) {
					let mut chars = path.chars();
					chars.next_back();
					chars.collect()
				} else {
					path
				}
			};
			
			// Check if the path contains 'admin' or 'api'
			// TODO Allow for custom stream directory, such as http://example.com/stream/radio
			if path == "/admin" ||
					path.starts_with( "/admin/" ) ||
					path == "/api" ||
					path.starts_with( "/api/" ) {
				send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}
			
			// Check if it is valid
			// For now this assumes the stream directory is /
			let dir = Path::new( &path );
			if let Some( parent ) = dir.parent() {
				if let Some( parent_str ) = parent.to_str() {
					if parent_str != "/" {
						send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
						return Ok( () )
					}
				} else {
					send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
					return Ok( () )
				}
			} else {
				send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}
			
			// Sources must have a content type
			// Maybe the type that is served should be checked?
			let mut properties = match get_header( "Content-Type", req.headers ) {
				Some( content_type ) => IcyProperties::new( std::str::from_utf8( content_type ).unwrap().to_string() ),
				None => {
					send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "No Content-type given" ) ) ).await?;
					return Ok( () )
				}
			};
			
			let mut serv = server.write().await;
			// Check if the mountpoint is already in use
			if serv.sources.contains_key( &path ) {
				send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}
			
			// Check if the max number of sources has been reached
			if serv.sources.len() > serv.properties.limits.sources {
				send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Too many sources connected" ) ) ).await?;
				return Ok( () )
			}
			
			// Give an 200 OK response
			send_ok( &mut stream, server_id, None ).await?;
			
			// Parse the headers for the source properties
			populate_properties( &mut properties, req.headers );
			
			// Create the source
			let source = Source {
				mountpoint: path.clone(),
				properties,
				metadata: None,
				metadata_vec: vec![ 0 ],
				clients: HashMap::new(),
				burst_buffer: Vec::new()
			};
			
			let queue_size = serv.properties.limits.queue_size;
			let burst_size = serv.properties.limits.burst_size;
			
			// Add to the server
			let arc = Arc::new( RwLock::new( source ) );
			serv.sources.insert( path.clone(), arc.clone() );
			let source_timeout = serv.properties.limits.header_timeout;
			drop( serv );
			
			println!( "Mounted source on {}", path );
			
			if buf.len() < body_offset {
				let slice = &buf[ body_offset .. ];
				broadcast_to_clients( &arc, slice.to_vec(), queue_size, burst_size ).await;
			}
			
			// Listen for bytes
			while {
				// Read the incoming stream data until it closes
				let mut buf = [ 0; 1024 ];
				let read = match timeout( Duration::from_millis( source_timeout ), async {
					match stream.read( &mut buf ).await {
						Ok( n ) => n,
						Err( e ) => {
							println!( "An error occured while reading stream data: {}", e );
							0
						}
					}
				} ).await {
					Ok( n ) => n,
					Err( _ ) => {
						println!( "A source timed out: {}", path );
						0
					}
				};
				
				if read != 0 {
					// Get the slice
					let mut slice: Vec< u8 > = Vec::new();
					slice.extend_from_slice( &buf[ .. read  ] );
					
					broadcast_to_clients( &arc, slice, queue_size, burst_size ).await;
				}
				
				read != 0
			}  {}
			
			// Clean up and remove the source
			let mut serv = server.write().await;
			// TODO Add a fallback mount
			for cli in arc.read().await.clients.values() {
				// Send an empty vec to signify the channel is closed
				drop( cli.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
			}
			
			serv.sources.remove( &path );
			println!( "Unmounted source {}", path );
		}
		"PUT" => {
			// TODO Implement the PUT method
			// I don't know any sources that use this method that I could easily get my hands on
			// VLC only uses SHOUT
			// For now, return a 405
			stream.write_all( b"HTTP/1.0 405 Method Not Allowed\r\n" ).await?;
			stream.write_all( ( format!( "Server: {}\r\n", server_id ) ).as_bytes() ).await?;
			stream.write_all( b"Connection: Close\r\n" ).await?;
			stream.write_all( b"Allow: GET, SOURCE\r\n" ).await?;
			stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
			stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
			stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
			stream.write_all( b"Pragma: no-cache\r\n\r\n" ).await?;
			stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
		}
		"GET" => {
			let source_id = path.to_owned();
			let source_id = {
				// Remove the trailing '/'
				if source_id.ends_with( '/' ) {
					let mut chars = source_id.chars();
					chars.next_back();
					chars.collect()
				} else {
					source_id
				}
			};
			
			let mut serv = server.write().await;
			let source_option = serv.sources.get( &source_id );
			// Check if the source is valid
			if let Some( source_lock ) = source_option {
				let mut source = source_lock.write().await;
				
				// Check if the max number of listeners has been reached
				if serv.clients.len() > serv.properties.limits.clients {
					send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Too many listeners connected" ) ) ).await?;
					return Ok( () )
				}
				
				// Check if metadata is enabled
				let meta_enabled = get_header( "Icy-MetaData", req.headers ).unwrap_or( b"0" ) == b"1";
				
				// Reply with a 200 OK
				send_listener_ok( &mut stream, server_id, &source.properties, meta_enabled, serv.properties.metaint ).await?;
				
				// Create a client
				// Get a valid UUID
				let client_id = {
					let mut unique = Uuid::new_v4();
					// Hopefully this doesn't take until the end of time
					while serv.clients.contains_key( &unique ) {
						unique = Uuid::new_v4();
					}
					unique
				};
	
				let ( sender, receiver ) = unbounded_channel::< Arc< Vec< u8 > > >();
				let properties = ClientProperties {
					id: client_id,
					uagent: {
						if let Some( arr ) = get_header( "User-Agent", req.headers ) {
							if let Ok( parsed ) = std::str::from_utf8( arr ) {
								Some( parsed.to_string() )
							} else {
								None
							}
						} else {
							None
						}
					},
					metadata: meta_enabled
				};
				let client = Client {
					sender: RwLock::new( sender ),
					receiver: RwLock::new( receiver ),
					buffer_size: RwLock::new( 0 ),
					properties: properties.clone()
				};
	
				
				if let Some( agent ) = &client.properties.uagent {
					println!( "User {} started listening on {} with user-agent {}", client_id, source_id, agent );
				} else {
					println!( "User {} started listening on {}", client_id, source_id );
				}
				if meta_enabled {
					println!( "User {} has icy metadata enabled", client_id );
				}
				
				// Get the metaint
				let metalen = serv.properties.metaint;
				
				let arc_client = Arc::new( RwLock::new( client ) );
				
				// Keep track of how many bytes have been sent
				let mut sent_count = 0;
				// Send the burst on connect buffer
				let burst_buf = &source.burst_buffer;
				if !burst_buf.is_empty() {
					match {
						if meta_enabled {
							let meta_vec = source.metadata_vec.clone();
							write_to_client( &mut stream, &mut sent_count, metalen, burst_buf, &meta_vec ).await
						} else {
							stream.write_all( &burst_buf ).await
						}
					} {
						Ok( _ ) => (),
						Err( e ) if e.kind() == ErrorKind::ConnectionReset || e.kind() == ErrorKind::ConnectionAborted => (),
						Err( e ) => {
							println!( "An error occured while sending the burst on connect buffer: {}", e );
							return Ok( () )
						}
					}
				}
				
				// Add the client id to the list of clients attached to the source
				source.clients.insert( client_id, arc_client.clone() );
				// No more need for source
				drop( source );
				// Add our client
				serv.clients.insert( client_id, properties );
				drop( serv );
				
				loop {
					// Receive whatever bytes, then send to the client
					let client = arc_client.read().await;
					let mut queue = client.receiver.write().await;
					let res = queue.recv().await;
					// Check if the channel is still alive
					if let Some( read ) = res {
						// If an empty buffer has been sent, then disconnect the client
						if read.len() > 0 {
							// Decrease the internal buffer
							*client.buffer_size.write().await -= read.len();
							match {
								if meta_enabled {
									let meta_vec = {
										let serv = server.read().await;
										if let Some( source_lock ) = serv.sources.get( &source_id ) {
											let source = source_lock.read().await;
											
											source.metadata_vec.clone()
										} else {
											vec![ 0 ]
										}
									};
									
									write_to_client( &mut stream, &mut sent_count, metalen, &read.to_vec(), &meta_vec ).await
								} else {
									stream.write_all( &read.to_vec() ).await
								}
							} {
								Ok( _ ) => (),
								Err( e ) if e.kind() == ErrorKind::ConnectionReset || e.kind() == ErrorKind::ConnectionAborted => break,
								Err( e ) => {
									println!( "An error occured while streaming: {}", e );
									break
								}
							}
						} else {
							break;
						}
					} else {
						// The sender has been dropped
						// The listener has been kicked
						break;
					}
				}
				
				// Close the message queue
				arc_client.read().await.receiver.write().await.close();
				
				println!( "User {} has disconnected", client_id );
				
				let mut serv = server.write().await;
				// Remove the client information from the list of clients
				serv.clients.remove( &client_id );
				drop( serv );
			} else {
				// Figure out what the request wants
				// /admin/metadata for updating the metadata
				// Anything else is not vanilla or unimplemented
				// Return a 404 otherwise
				
				match path.as_str() {
					"/admin/metadata" => {
						// Check for authorization
						if let Some( ( name, pass ) ) = get_basic_auth( req.headers ) {
							// For testing purposes right now
							// TODO Add proper configuration
							if !validate_user( &serv.properties, name, pass ) {
								send_unauthorized( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
							return Ok( () )
						}
						
						// Authentication passed
						// Now check the query fields
						// Takes in mode, mount, song and url
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mode", "mount", "song", "url" ], &queries )[ .. ].as_ref() {
								[ Some( mode ), Some( mount ), song, url ] if mode == "updinfo" => {
									match serv.sources.get( mount ) {
										Some( source ) => {
											println!( "Updated source {} metadata with title '{}' and url '{}'", mount, song.as_ref().unwrap_or( &"".to_string() ), url.as_ref().unwrap_or( &"".to_string() ) );
											let mut source = source.write().await;
											source.metadata = match ( song, url ) {
												( None, None ) => None,
												_ => Some( IcyMetadata {
													title: song.clone(),
													url: url.clone()
												} ),
											};
											source.metadata_vec = get_metadata_vec( &source.metadata );
											send_ok( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Success" ) ) ).await?;
										}
										None => send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
									}
								}
								_ => (),
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					// Return 404
					_ => send_not_found( &mut stream, server_id, Some( ( "text/html; charset=utf-8", "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>" ) ) ).await?,
				}
			}
		}
		_ => {
			// Unknown
			stream.write_all( b"HTTP/1.0 405 Method Not Allowed\r\n" ).await?;
			stream.write_all( ( format!( "Server: {}\r\n", server_id ) ).as_bytes() ).await?;
			stream.write_all( b"Connection: Close\r\n" ).await?;
			stream.write_all( b"Allow: GET, SOURCE\r\n" ).await?;
			stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
			stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
			stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
			stream.write_all( b"Pragma: no-cache\r\n" ).await?;
			stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
		}
	}
	
	Ok( () )
}

async fn broadcast_to_clients( source: &Arc< RwLock< Source > >, data: Vec< u8 >, queue_size: usize, burst_size: usize ) {
	// Remove these later
	let mut dropped: Vec< Uuid > = Vec::new();
	
	let read = data.len();
	let arc_slice = Arc::new( data );
	
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

async fn write_to_client( stream: &mut TcpStream, sent_count: &mut usize, metalen: usize, data: &[ u8 ], metadata: &[ u8 ] ) -> Result< (), std::io::Error > {
	let new_sent = *sent_count + data.len();
	// Consume it here or something
	let send: &[ u8 ];
	// Create a new vector to hold our data, if we need one
	let mut inserted: Vec< u8 > = Vec::new();
	// Check if we need to send the metadata
	if new_sent > metalen {
		// Insert the current range
		let mut index = metalen - *sent_count;
		if index > 0 {
			inserted.extend_from_slice( &data[ .. index ] );
		}
		while index < data.len() {
			inserted.extend_from_slice( metadata );
			
			// Add the data
			let end = std::cmp::min( data.len(), index + metalen );
			if index != end {
				inserted.extend_from_slice( &data[ index .. end ] );
				index = end;
			}
		}
		
		// Update the total sent amount and send
		*sent_count = new_sent % metalen;
		send = &inserted;
	} else {
		// Copy over the new amount
		*sent_count = new_sent;
		send = data;
	}
	
	stream.write_all( &send ).await
}

async fn send_listener_ok( stream: &mut TcpStream, id: String, properties: &IcyProperties, meta_enabled: bool, metaint: usize ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "Content-Type: {}\r\n", properties.content_type ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n" ).await?;

	// If metaint is enabled
	if meta_enabled {
		stream.write_all( ( format!( "icy-metaint:{}\r\n", metaint ) ).as_bytes() ).await?;
	}

	// Properties or default
	if let Some( br ) = properties.bitrate.as_ref() {
		stream.write_all( ( format!( "icy-br:{}\r\n", br ) ).as_bytes() ).await?;
	}
	stream.write_all( ( format!( "icy-description:{}\r\n", properties.description.as_ref().unwrap_or( &"Unknown".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-genre:{}\r\n", properties.genre.as_ref().unwrap_or( &"Undefined".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-name:{}\r\n", properties.name.as_ref().unwrap_or( &"Unnamed Station".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-pub:{}\r\n", properties.public as usize ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-url:{}\r\n\r\n", properties.url.as_ref().unwrap_or( &"Unknown".to_string() ) ) ).as_bytes() ).await?;
	
	Ok( () )
}

async fn send_not_found( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 404 File Not Found\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if let Some( ( content_type, text ) ) = message {
		stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
		stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( ( _, text ) ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_ok( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if let Some( ( content_type, text ) ) = message {
		stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
		stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( ( _, text ) ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_bad_request( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 400 Bad Request\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if let Some( ( content_type, text ) ) = message {
		stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
		stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( ( _, text ) ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_forbidden( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 403 Forbidden\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if let Some( ( content_type, text ) ) = message {
		stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
		stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( ( _, text ) ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_unauthorized( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 401 Authorization Required\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if let Some( ( content_type, text ) ) = message {
		stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
		stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
	}
	stream.write_all( b"WWW-Authenticate: Basic realm=\"Icy Server\"\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( ( _, text ) ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

/**
 * Get a vector containing n and the padded data
 */
fn get_metadata_vec( metadata: &Option< IcyMetadata > ) -> Vec< u8 > {
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

fn extract_queries( url: &str ) -> ( &str, Option< Vec< Query > > ) {
	if let Some( ( path, last ) ) = url.split_once( "?" ) {
		let mut queries: Vec< Query > = Vec::new();
		for field in last.split( '&' ) {
			// decode doesn't treat + as a space
			if let Some( ( name, value ) ) = field.replace( "+", " " ).split_once( '=' ) {
				let name = urlencoding::decode( name );
				let value = urlencoding::decode( value );
				
				if let Ok( field ) = name{
					if let Ok( value ) = value {
						queries.push( Query{ field, value } );
					}					
				}
			}
		}
		
		( path, Some( queries ) )
	} else {
		( url, None )
	}
}

fn populate_properties( properties: &mut IcyProperties, headers: &[ httparse::Header< '_ > ] ) {
	for header in headers {
		let name = header.name;
		let val = std::str::from_utf8( header.value ).unwrap_or( "" );
		
		// There's a nice list here: https://github.com/ben221199/MediaCast
		// Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
		match name {
			"User-Agent" => properties.uagent = Some( val.to_string() ),
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

fn get_header< 'a >( key: &str, headers: &[ httparse::Header< 'a > ] ) -> Option< &'a [ u8 ] > {
	for header in headers {
		if header.name == key {
			return Some( header.value )
		}
	}
	None
}

fn get_basic_auth( headers: &[ httparse::Header ] ) -> Option< ( String, String ) > {
	if let Some( auth ) = get_header( "Authorization", headers ) {
		let reg = Regex::new( r"^Basic ((?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)$" ).unwrap();
		if let Some( capture ) = reg.captures( std::str::from_utf8( &auth ).unwrap() ) {
			if let Some( ( name, pass ) ) = std::str::from_utf8( &base64::decode( &capture[ 1 ] ).unwrap() ).unwrap().split_once( ":" ) {
				return Some( ( String::from( name ), String::from( pass ) ) )
			}
		}
	}
	None
}

fn get_queries_for( keys: Vec< &str >, queries: &[ Query ] ) -> Vec< Option< String > > {
	let mut results = vec![ None; keys.len() ];
	
	for query in queries {
		let field = query.field.as_str();
		for ( i, key ) in keys.iter().enumerate() {
			if &field == key {
				results[ i ] = Some( query.value.to_string() );
			}
		}
	}
	
	results
}

// TODO Add some sort of permission system
fn validate_user( properties: &ServerProperties, username: String, password: String ) -> bool {
	for cred in &properties.users {
		if cred.username == username && cred.password == password {
			return true;
		}
	}
	false
}

// Serde default deserialization values
fn default_property_port() -> u16 { PORT }
fn default_property_metaint() -> usize { METAINT }
fn default_property_server_id() -> String { SERVER_ID.to_string() }
fn default_property_admin() -> String { ADMIN.to_string() }
fn default_property_host() -> String { HOST.to_string() }
fn default_property_location() -> String { LOCATION.to_string() }
fn default_property_users() -> Vec< Credential > { vec![ Credential{ username: "admin".to_string(), password: "hackme".to_string() }, Credential { username: "source".to_string(), password: "hackme".to_string() } ] }
fn default_property_limits() -> ServerLimits { ServerLimits{
	clients: default_property_limits_clients(),
	sources: default_property_limits_sources(),
	queue_size: default_property_limits_queue_size(),
	burst_size: default_property_limits_burst_size(),
	header_timeout: default_property_limits_header_timeout(),
	source_timeout: default_property_limits_source_timeout()
} }
fn default_property_limits_clients() -> usize { CLIENTS }
fn default_property_limits_sources() -> usize { SOURCES }
fn default_property_limits_queue_size() -> usize { QUEUE_SIZE }
fn default_property_limits_burst_size() -> usize { BURST_SIZE }
fn default_property_limits_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_property_limits_source_timeout() -> u64 { SOURCE_TIMEOUT }

#[ tokio::main ]
async fn main() {
	// TODO Log everything somehow or something
	let mut properties = ServerProperties::new();

	let args: Vec< String > = std::env::args().collect();
	let config_location = {
		if args.len() > 1 {
			args[ 1 ].clone()
		} else {
			match std::env::current_dir() {
				Ok( mut buf ) => {
					buf.push( "config" );
					buf.set_extension( "json" );
					if let Some( string ) = buf.as_path().to_str() {
						string.to_string()
					} else {
						"config.json".to_string()
					}
				}
				Err( _ ) => "config.json".to_string(),
			}
		}
	};
	println!( "Using config path {}", config_location );
	
	match std::fs::read_to_string( &config_location ) {
		Ok( contents ) => {
			println!( "Attempting to parse the config" );
			match serde_json::from_str( &contents.as_str() ) {
				Ok( prop ) => properties = prop,
				Err( e ) => println!( "An error occured while parsing the config: {}", e ),
			}
		}
		Err( e ) if e.kind() == std::io::ErrorKind::NotFound => {
			println!( "The config file was not found! Attempting to save to file" );
		}
		Err( e ) => println!( "An error occured while trying to read the config: {}", e ),
	}
	
	// Create or update the current config
	match File::create( &config_location ) {
		Ok( file ) => {
			match serde_json::to_string_pretty( &properties ) {
				Ok( config ) => {
					let mut writer = BufWriter::new( file );
					if let Err( e ) = writer.write_all( config.as_bytes() ) {
						println!( "An error occured while writing to the config file: {}", e );
					}
				}
				Err( e ) => println!( "An error occured while trying to serialize the server properties: {}", e ),
			}
		}
		Err( e ) => println!( "An error occured while to create the config file: {}", e ),
	}
	
	println!( "Using PORT           : {}", properties.port );
	println!( "Using METAINT        : {}", properties.metaint );
	println!( "Using SERVER ID      : {}", properties.server_id );
	println!( "Using ADMIN          : {}", properties.admin );
	println!( "Using HOST           : {}", properties.host );
	println!( "Using LOCATION       : {}", properties.location );
	println!( "Using CLIENT LIMIT   : {}", properties.limits.clients );
	println!( "Using SOURCE LIMIT   : {}", properties.limits.sources );
	println!( "Using QUEUE SIZE     : {}", properties.limits.queue_size );
	println!( "Using BURST SIZE     : {}", properties.limits.burst_size );
	println!( "Using HEADER TIMEOUT : {}", properties.limits.header_timeout );
	println!( "Using SOURCE TIMEOUT : {}", properties.limits.source_timeout );
	
	if properties.users.is_empty() {
		println!( "At least one user must be configured in the config!" );
	} else {
		println!( "{} users registered", properties.users.len() );
		println!( "Attempting to bind to port {}", properties.port );
		match TcpListener::bind( SocketAddr::new( IpAddr::V4( Ipv4Addr::new( 127, 0, 0, 1 ) ), properties.port ) ).await {
			Ok( listener ) => {
				let server = Arc::new( RwLock::new( Server::new( properties ) ) );
				
				println!( "Listening..." );				
				loop {
					match listener.accept().await {
						Ok( ( socket, addr ) ) => {
							let server_clone = server.clone();
							
							tokio::spawn( async move {
								if let Err( e ) = handle_connection( server_clone, socket ).await {
									println!( "An error occured while handling a connection from {}: {}", addr, e );
								}
							} );
						}
						Err( e ) => println!( "An error occured while accepting a connection: {}", e ),
					}
				}
			}
			Err( e ) => println!( "Unable to bind to port: {}", e ),
		}
	}
}
