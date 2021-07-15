use httpdate::fmt_http_date;
use regex::Regex;
use serde::{ Deserialize, Serialize };
use serde_json::{ json, Value };
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{ BufWriter, ErrorKind, Write };
use std::net::{ SocketAddr, IpAddr, Ipv4Addr };
use std::path::Path;
use std::sync::Arc;
use std::time::{ Duration, SystemTime, UNIX_EPOCH };
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
// Description of the internet radio
const DESCRIPTION: &str = "Yet Another Internet Radio";

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

struct Client {
	source: RwLock< String >,
	sender: RwLock< UnboundedSender< Arc< Vec< u8 > > > >,
	receiver: RwLock< UnboundedReceiver< Arc< Vec< u8 > > > >,
	buffer_size: RwLock< usize >,
	properties: ClientProperties,
	stats: RwLock< ClientStats >
}

#[ derive( Serialize, Clone ) ]
struct ClientProperties {
	id: Uuid,
	uagent: Option< String >,
	metadata: bool
}

#[ derive( Serialize, Clone ) ]
struct ClientStats {
	start_time: u64,
	bytes_sent: usize
}

#[ derive( Serialize, Clone ) ]
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

#[ derive( Serialize, Clone ) ]
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

#[ derive( Serialize, Deserialize, Clone ) ]
struct SourceLimits {
	#[ serde( default = "default_property_limits_clients" ) ]
	clients: usize,
	#[ serde( default = "default_property_limits_burst_size" ) ]
	burst_size: usize,
	#[ serde( default = "default_property_limits_source_timeout" ) ]
	source_timeout: u64
}

struct Source {
	// Is setting the mountpoint in the source really useful, since it's not like the source has any use for it
	mountpoint: String,
	properties: IcyProperties,
	metadata: Option< IcyMetadata >,
	metadata_vec: Vec< u8 >,
	clients: HashMap< Uuid, Arc< RwLock< Client > > >,
	burst_buffer: Vec< u8 >,
	stats: RwLock< SourceStats >,
	fallback: Option< String >,
	// Not really sure how else to signal when to disconnect the source
	disconnect_flag: bool
}

#[ derive( Serialize, Deserialize, Clone ) ]
struct SourceStats {
	start_time: u64,
	bytes_read: usize,
	peak_listeners: usize
}

// TODO Add permissions
#[ derive( Serialize, Deserialize, Clone ) ]
struct Credential {
	username: String,
	password: String
}

#[ derive( Serialize, Deserialize, Clone ) ]
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
	source_timeout: u64,
	#[ serde( default = "default_property_limits_source_limits" ) ]
	source_limits: HashMap< String, SourceLimits >
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
	#[ serde( default = "default_property_description" ) ]
	description: String,
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
			description: default_property_description(),
			limits: default_property_limits(),
			users: default_property_users()
		}
	}
}

#[ derive( Serialize, Deserialize, Clone ) ]
struct ServerStats {
	start_time: u64,
	peak_listeners: usize,
	session_bytes_sent: usize,
	session_bytes_read: usize,
}

impl ServerStats {
	fn new() -> ServerStats {
		ServerStats {
			start_time: 0,
			peak_listeners: 0,
			session_bytes_sent: 0,
			session_bytes_read: 0
		}
	}
}

// TODO Add stats
struct Server {
	sources: HashMap< String, Arc< RwLock< Source > > >,
	clients: HashMap< Uuid, ClientProperties >,
	properties: ServerProperties,
	stats: ServerStats
}

impl Server {
	fn new( properties: ServerProperties ) -> Server {
		Server{
			sources: HashMap::new(),
			clients: HashMap::new(),
			properties,
			stats: ServerStats::new()
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
		Ok( Err( _ ) ) => return Ok( () ),
		Ok( _ ) => (),
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
			
			let source_stats = SourceStats {
				start_time: {
					if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
						time.as_secs()
					} else {
						0
					}
				},
				bytes_read: 0,
				peak_listeners: 0
			};
			
			// Create the source
			let source = Source {
				mountpoint: path.clone(),
				properties,
				metadata: None,
				metadata_vec: vec![ 0 ],
				clients: HashMap::new(),
				burst_buffer: Vec::new(),
				stats: RwLock::new( source_stats ),
				fallback: None,
				disconnect_flag: false
			};
			
			let queue_size = serv.properties.limits.queue_size;
			let ( burst_size, source_timeout ) =  {
				if let Some( limit ) = serv.properties.limits.source_limits.get( &path ) {
					( limit.burst_size, limit.source_timeout )
				} else {
					( serv.properties.limits.burst_size, serv.properties.limits.header_timeout )
				}
			};
			
			// Add to the server
			let arc = Arc::new( RwLock::new( source ) );
			serv.sources.insert( path, arc.clone() );
			drop( serv );
			
			println!( "Mounted source on {}", arc.read().await.mountpoint );
			
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
						println!( "A source timed out: {}", arc.read().await.mountpoint );
						0
					}
				};
				
				if read != 0 {
					// Get the slice
					let mut slice: Vec< u8 > = Vec::new();
					slice.extend_from_slice( &buf[ .. read  ] );
					
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

			println!( "Unmounted source {}", source.mountpoint );
		}
		"PUT" => {
			// TODO Implement the PUT method
			// I don't know any sources that use this method that I could easily get my hands on
			// VLC only uses SOURCE
			// For now, return a 405
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
				let too_many_clients = {
					if let Some( limit ) = serv.properties.limits.source_limits.get( &source_id ) {
						source.clients.len() >= limit.clients
					} else {
						false
					}
				};
				if serv.clients.len() >= serv.properties.limits.clients || too_many_clients {
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
				let stats = ClientStats {
					start_time: {
						if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
							time.as_secs()
						} else {
							0
						}
					},
					bytes_sent: 0
				};
				let client = Client {
					source: RwLock::new( source_id ),
					sender: RwLock::new( sender ),
					receiver: RwLock::new( receiver ),
					buffer_size: RwLock::new( 0 ),
					properties: properties.clone(),
					stats: RwLock::new( stats )
				};
	
				
				if let Some( agent ) = &client.properties.uagent {
					println!( "User {} started listening on {} with user-agent {}", client_id, client.source.read().await, agent );
				} else {
					println!( "User {} started listening on {}", client_id, client.source.read().await );
				}
				if meta_enabled {
					println!( "User {} has icy metadata enabled", client_id );
				}
				
				// Get the metaint
				let metalen = serv.properties.metaint;
				
				// Keep track of how many bytes have been sent
				let mut sent_count = 0;
				
				// Get a copy of the burst buffer and metadata
				let burst_buf = source.burst_buffer.clone();
				let metadata_copy = source.metadata_vec.clone();
				
				let arc_client = Arc::new( RwLock::new( client ) );
				// Add the client id to the list of clients attached to the source
				source.clients.insert( client_id, arc_client.clone() );
				
				{
					let mut source_stats = source.stats.write().await;
					source_stats.peak_listeners = std::cmp::max( source_stats.peak_listeners, source.clients.len() );
				}
				
				// No more need for source
				drop( source );
				
				// Add our client
				serv.clients.insert( client_id, properties );
				
				// Set the max amount of listeners
				serv.stats.peak_listeners = std::cmp::max( serv.stats.peak_listeners, serv.clients.len() );
				
				drop( serv );
				
				// Send the burst on connect buffer
				let burst_success = {
					if !burst_buf.is_empty() {
						match {
							if meta_enabled {
								write_to_client( &mut stream, &mut sent_count, metalen, &burst_buf, &metadata_copy ).await
							} else {
								stream.write_all( &burst_buf ).await
							}
						} {
							Ok( _ ) => {
								arc_client.read().await.stats.write().await.bytes_sent += burst_buf.len();
								true
							}
							Err( _ ) => false,
						}
					} else {
						true
					}
				};
				drop( metadata_copy );
				drop( burst_buf );
				
				if burst_success {
					loop {
						// Receive whatever bytes, then send to the client
						let client = arc_client.read().await;
						let res = client.receiver.write().await.recv().await;
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
											if let Some( source_lock ) = serv.sources.get( &*client.source.read().await ) {
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
									Ok( _ ) => arc_client.read().await.stats.write().await.bytes_sent += read.len(),
									Err( _ ) => break,
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
				}
				
				// Close the message queue
				arc_client.read().await.receiver.write().await.close();
				
				println!( "User {} has disconnected", client_id );
				
				let mut serv = server.write().await;
				// Remove the client information from the list of clients
				serv.clients.remove( &client_id );
				serv.stats.session_bytes_sent += arc_client.read().await.stats.read().await.bytes_sent;
				drop( serv );
			} else {
				// Figure out what the request wants
				// /admin/metadata for updating the metadata
				// /admin/listclients for viewing the clients for a particular source
				// /admin/fallbacks for setting the fallback of a particular source
				// /admin/moveclients for moving listeners from one source to another
				// /admin/killclient for disconnecting a client
				// /admin/killsource for disconnecting a source
				// /admin/listmounts for listing all mounts available
				// Anything else is not vanilla or unimplemented
				// Return a 404 otherwise
				
				// Drop the write lock
				drop( serv );
				
				// Paths
				match path.as_str() {
					"/admin/metadata" => {
						let serv = server.read().await;
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
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/listclients" => {
						let serv = server.read().await;
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
						
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount" ], &queries )[ .. ].as_ref() {
								[ Some( mount ) ] => {
									if let Some( source ) = serv.sources.get( mount ) {
										let mut clients: HashMap< Uuid, Value > = HashMap::new();
										
										for client in source.read().await.clients.values() {
											let client = client.read().await;
											let properties = client.properties.clone();
											
											let value = json!( {
												"user_agent": properties.uagent,
												"metadata_enabled": properties.metadata,
												"stats": &*client.stats.read().await
											} );
										
											clients.insert( properties.id, value );
										}
										
										if let Ok( serialized ) = serde_json::to_string( &clients ) {
											send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
										} else {
											send_internal_error( &mut stream, server_id, None ).await?;
										}
									} else {
										send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/fallbacks" => {
						let serv = server.read().await;
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
						
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount", "fallback" ], &queries )[ .. ].as_ref() {
								[ Some( mount ), fallback ] => {
									if let Some( source ) = serv.sources.get( mount ) {
										source.write().await.fallback = fallback.clone();
										
										if let Some( fallback ) = fallback {
											println!( "Set the fallback for {} to {}", mount, fallback );
										} else {
											println!( "Unset the fallback for {}", mount );
										}
										send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
									} else {
										send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/moveclients" => {
						let serv = server.read().await;
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
						
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount", "destination" ], &queries )[ .. ].as_ref() {
								[ Some( mount ), Some( dest ) ] => {
									match ( serv.sources.get( mount ), serv.sources.get( dest ) ) {
										( Some( source ), Some( destination ) ) => {
											let mut from = source.write().await;
											let mut to = destination.write().await;
											
											for ( uuid, client ) in from.clients.drain() {
												*client.read().await.source.write().await = to.mountpoint.clone();
												to.clients.insert( uuid, client );
											}
											
											println!( "Moved clients from {} to {}", mount, dest );
											send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
										}
										_ => send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/killclient" => {
						let serv = server.read().await;
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
						
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount", "id" ], &queries )[ .. ].as_ref() {
								[ Some( mount ), Some( uuid_str ) ] => {
									match ( serv.sources.get( mount ), Uuid::parse_str( uuid_str ) ) {
										( Some( source ), Ok( uuid ) ) => {
											if let Some( client ) = source.read().await.clients.get( &uuid ) {
												drop( client.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
												println!( "Killing client {}", uuid );
												send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
											} else {
												send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid id" ) ) ).await?;
											}
										}
										( None, _ ) => send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
										( Some( _ ), Err( _ ) ) => send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid id" ) ) ).await?,
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/killsource" => {
						let serv = server.read().await;
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
						
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount" ], &queries )[ .. ].as_ref() {
								[ Some( mount ) ] => {
									if let Some( source ) = serv.sources.get( mount ) {
										source.write().await.disconnect_flag = true;
										
										println!( "Killing source {}", mount );
										send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
									} else {
										send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/admin/listmounts" => {
						let serv = server.read().await;
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
						
						let mut sources: HashMap< String, Value > = HashMap::new();
						
						for source in serv.sources.values() {
							let source = source.read().await;

							let value = json!( {
								"fallback": source.fallback,
								"metadata": source.metadata,
								"properties": source.properties,
								"stats": &*source.stats.read().await,
								"clients": source.clients.keys().cloned().collect::< Vec< Uuid > >()
							} );

							sources.insert( source.mountpoint.clone(), value );
						}

						if let Ok( serialized ) = serde_json::to_string( &sources ) {
							send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
						} else {
							send_internal_error( &mut stream, server_id, None ).await?;
						}
					}
					"/api/serverinfo" => {
						let serv = server.read().await;
						
						let info = json!( {
							"mounts": serv.sources.keys().cloned().collect::< Vec< String > >(),
							"properties": {
								"server_id": serv.properties.server_id,
								"admin": serv.properties.admin,
								"host": serv.properties.host,
								"location": serv.properties.location,
								"description": serv.properties.description
							},
							"stats": {
								"start_time": serv.stats.start_time,
								"peak_listeners": serv.stats.peak_listeners
							},
							"current_listeners": serv.clients.len()
						} );
						
						if let Ok( serialized ) = serde_json::to_string( &info ) {
							send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
						} else {
							send_internal_error( &mut stream, server_id, None ).await?;
						}
					}
					"/api/mountinfo" => {
						if let Some( queries ) = queries {
							match get_queries_for( vec![ "mount" ], &queries )[ .. ].as_ref() {
								[ Some( mount ) ] => {
									let serv = server.read().await;
									if let Some( source ) = serv.sources.get( mount ) {
										let source = source.read().await;
										let properties = &source.properties;
										let stats = &source.stats.read().await;
										
										let info = json!( {
											"metadata": source.metadata,
											"properties": {
												"name": properties.name,
												"description": properties.description,
												"url": properties.url,
												"genre": properties.genre,
												"bitrate": properties.bitrate,
												"content_type": properties.content_type
											},
											"stats": {
												"start_time": stats.start_time,
												"peak_listeners": stats.peak_listeners
											},
											"current_listeners": source.clients.len()
										} );
										
										if let Ok( serialized ) = serde_json::to_string( &info ) {
											send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
										} else {
											send_internal_error( &mut stream, server_id, None ).await?;
										}
									} else {
										send_forbidden( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
									}
								}
								_ => send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
							}
						} else {
							// Bad request
							send_bad_request( &mut stream, server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
						}
					}
					"/api/stats" => {
						let server = server.read().await;
						let stats = &server.stats;
						let mut total_bytes_sent = stats.session_bytes_sent;
						let mut total_bytes_read = stats.session_bytes_read;
						for source in server.sources.values(){
							let source = source.read().await;
							total_bytes_read += source.stats.read().await.bytes_read;
							for clients in source.clients.values(){
								total_bytes_sent += clients.read().await.stats.read().await.bytes_sent;
							}
						}
						
						let epoch = {
							if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
								time.as_secs()
							} else {
								0
							}
						};

						let response = json!( {
							"uptime": epoch - stats.start_time,
							"peak_listeners": stats.peak_listeners,
							"session_bytes_read": total_bytes_read,
							"session_bytes_sent": total_bytes_sent
						} );

						send_ok( &mut stream, server_id, Some( ( "application/json; charset=utf-8", &response.to_string() ) ) ).await?;

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

async fn send_internal_error( stream: &mut TcpStream, id: String, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 500 Internal Server Error\r\n" ).await?;
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
fn default_property_description() -> String { DESCRIPTION.to_string() }
fn default_property_users() -> Vec< Credential > { vec![ Credential{ username: "admin".to_string(), password: "hackme".to_string() }, Credential { username: "source".to_string(), password: "hackme".to_string() } ] }
fn default_property_limits() -> ServerLimits { ServerLimits{
	clients: default_property_limits_clients(),
	sources: default_property_limits_sources(),
	queue_size: default_property_limits_queue_size(),
	burst_size: default_property_limits_burst_size(),
	header_timeout: default_property_limits_header_timeout(),
	source_timeout: default_property_limits_source_timeout(),
	source_limits: default_property_limits_source_limits()
} }
fn default_property_limits_clients() -> usize { CLIENTS }
fn default_property_limits_sources() -> usize { SOURCES }
fn default_property_limits_queue_size() -> usize { QUEUE_SIZE }
fn default_property_limits_burst_size() -> usize { BURST_SIZE }
fn default_property_limits_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_property_limits_source_timeout() -> u64 { SOURCE_TIMEOUT }
fn default_property_limits_source_mountpoint() -> String { "/radio".to_string() }
fn default_property_limits_source_limits() -> HashMap< String, SourceLimits > {
	let mut map = HashMap::new();
	map.insert( default_property_limits_source_mountpoint(), SourceLimits {
		clients: default_property_limits_clients(),
		burst_size: default_property_limits_burst_size(),
		source_timeout: default_property_limits_source_timeout()
	} );
	map
}	

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
	println!( "Using DESCRIPTION    : {}", properties.description );
	println!( "Using CLIENT LIMIT   : {}", properties.limits.clients );
	println!( "Using SOURCE LIMIT   : {}", properties.limits.sources );
	println!( "Using QUEUE SIZE     : {}", properties.limits.queue_size );
	println!( "Using BURST SIZE     : {}", properties.limits.burst_size );
	println!( "Using HEADER TIMEOUT : {}", properties.limits.header_timeout );
	println!( "Using SOURCE TIMEOUT : {}", properties.limits.source_timeout );
	for ( mount, limit ) in &properties.limits.source_limits {
		println!( "Using limits for {}:", mount );
		println!( "      CLIENT LIMIT   : {}", limit.clients );
		println!( "      SOURCE TIMEOUT : {}", limit.source_timeout );
		println!( "      BURST SIZE     : {}", limit.burst_size );
	}
	
	if properties.users.is_empty() {
		println!( "At least one user must be configured in the config!" );
	} else {
		println!( "{} users registered", properties.users.len() );
		println!( "Attempting to bind to port {}", properties.port );
		match TcpListener::bind( SocketAddr::new( IpAddr::V4( Ipv4Addr::new( 127, 0, 0, 1 ) ), properties.port ) ).await {
			Ok( listener ) => {
				let server = Arc::new( RwLock::new( Server::new( properties ) ) );
				
				if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
					println!( "The server has started on {}", fmt_http_date( SystemTime::now() ) );
					server.write().await.stats.start_time = time.as_secs();
				} else {
					println!( "Unable to capture when the server started!" );
				}
				
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
