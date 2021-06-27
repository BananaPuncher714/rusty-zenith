extern crate base64;
extern crate bus;
extern crate httparse;
extern crate regex;
extern crate serde_json;
extern crate tokio;
extern crate urlencoding;

use bus::{ Bus, BusReader };
use httpdate::fmt_http_date;
use regex::Regex;
use serde::{ Deserialize, Serialize };
use std::collections::{ HashMap, HashSet };
use std::error::Error;
use std::fs::File;
use std::io::{ BufWriter, Write };
use std::net::{ SocketAddr, IpAddr, Ipv4Addr };
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use tokio::net::{ TcpListener, TcpStream };
use tokio::sync::{ Mutex, RwLock };
use uuid::Uuid;

const PORT: u16 = 1900;
const METAINT: usize = 16_000;
const SERVER_ID: &str = "Rusty Zenith 0.1.0";
const ADMIN: &str = "admin@localhost";
const HOST: &str = "localhost";
const LOCATION: &str = "1.048596";

#[ derive( Clone ) ]
struct Query {
	field: String,
	value: String
}

struct Client {
	reader: RwLock< BusReader< Vec< u8 > > >,
	id: Uuid,
	metadata: bool,
	uagent: Option< String >
}

struct IcyProperties {
	uagent: Option< String >,
	public: bool,
	name: Option< String >,
	description: Option< String >,
	url: Option< String >,
	genre: Option< String >,
	bitrate: Option< String >
}

#[ derive( Clone ) ]
struct IcyMetadata {
	title: Option< String >,
	url: Option< String >
}

impl IcyProperties {
	fn new() -> IcyProperties {
		IcyProperties{
			uagent: None,
			public: false,
			name: None,
			description: None,
			url: None,
			genre: None,
			bitrate: None
		}
	}
}

struct Source {
	mountpoint: String,
	properties: IcyProperties,
	content_type: String,
	metadata: Option< IcyMetadata >,
	bus: RwLock< Bus< Vec< u8 > > >,
	clients: HashSet< Uuid >
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
	location: String
}

impl ServerProperties {
	fn new() -> ServerProperties {
		ServerProperties {
			port: PORT,
			metaint: METAINT,
			server_id: SERVER_ID.to_string(),
			admin: ADMIN.to_string(),
			host: HOST.to_string(),
			location: LOCATION.to_string()
		}
	}
}

struct Server {
	sources: HashMap< String, Arc< RwLock< Source > > >,
	clients: HashMap< Uuid, Arc< RwLock< Client > > >,
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

async fn handle_connection( server: Arc< Mutex< Server > >, mut stream: TcpStream ) -> Result< (), Box< dyn Error > > {
	let mut buf = Vec::new();
	let mut buffer = [ 0; 512 ];
	
	// Get the header
	while {
		let mut headers = [ httparse::EMPTY_HEADER; 16 ];
		let mut req = httparse::Request::new( &mut headers );
		match stream.read( &mut buffer ).await {
			Ok( read ) => buf.extend_from_slice( &buffer[ .. read ] ),
			Err( e ) => {
				println!( "An error occured while reading a request: {}", e );
				return Err( Box::new( e ) );
			}
		}
		
		req.parse( &buf )?.is_partial()
	} {}

	let mut headers = [ httparse::EMPTY_HEADER; 16 ];
	let mut req = httparse::Request::new( &mut headers );
	let _offset = req.parse( &buf ).unwrap();

	let method = req.method.unwrap();
	let ( base_path, queries ) = extract_queries( req.path.unwrap() );
	let path = path_clean::clean( base_path );
	
	println!( "Received headers with method {} and path {}", method, base_path );

	let server_id = {
		server.lock().await.properties.server_id.clone()
	};

	if method == "SOURCE" {
		println!( "Received a SOURCE request!" );
		// Check for authorization
		if let Some( ( name, pass ) ) = get_basic_auth( req.headers ) {
			// For testing purposes right now
			// TODO Add proper configuration
			if name != "source" || pass != "hackme" {
				send_unauthorized( &mut stream, server_id, None ).await?;
				return Ok( () )
			}
		} else {
			// No auth, return and close
			send_unauthorized( &mut stream, server_id, None ).await?;
			return Ok( () )
		}
		
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
		if path == "/admin" ||
				path.starts_with( "/admin/" ) ||
				path == "/api" ||
				path.starts_with( "/api/" ) {
			send_forbidden( &mut stream, server_id, Some( "Invalid mountpoint" ) ).await?;
			return Ok( () )
		}
		
		println!( "Source is attempting to use mountpoint {}", path );
		// Check if it is valid
		let dir = Path::new( &path );
		if let Some( parent ) = dir.parent() {
			if let Some( parent_str ) = parent.to_str() {
				if parent_str != "/" {
					send_forbidden( &mut stream, server_id, Some( "Invalid mountpoint" ) ).await?;
					return Ok( () )
				}
			} else {
				send_forbidden( &mut stream, server_id, Some( "Invalid mountpoint" ) ).await?;
				return Ok( () )
			}
		} else {
			send_forbidden( &mut stream, server_id, Some( "Invalid mountpoint" ) ).await?;
			return Ok( () )
		}
		
		let content_type_option = get_header( "Content-Type", req.headers );
		if content_type_option.is_none() {
			send_forbidden( &mut stream, server_id, Some( "No Content-type given" ) ).await?;
			return Ok( () )
		}
		
		let mut serv = server.lock().await;
		// Check if the mountpoint is already in use
		if serv.sources.contains_key( &path ) {
			send_forbidden( &mut stream, server_id, Some( "Invalid mountpoint" ) ).await?;
			return Ok( () )
		}
		
		// Give an 200 OK response
		send_ok( &mut stream, server_id, None ).await?;
		
		// Parse the headers for the source properties
		let source = Source {
			mountpoint: path.clone(),
			properties: get_properties( req.headers ),
			content_type: std::str::from_utf8( content_type_option.unwrap() ).unwrap().to_string(),
			metadata: None,
//			metadata: Some( IcyMetadata {
//				title: Some( "NieR:Automata Piano Collections - 1.1 – Weight of the World // 榊原　大".to_string() ),
//				url: Some( "https://shop.r10s.jp/book/cabinet/6196/4988601466196.jpg".to_string() )
//			} ),
			bus: RwLock::new( Bus::new( 512 ) ),
			clients: HashSet::new()
		};
		
		// Add to the server
		let arc = Arc::new( RwLock::new( source ) );
		serv.sources.insert( path.clone(), arc.clone() );
		drop( serv );
		
		println!( "Successfully mounted source {}", path );
		
		// Listen for bytes
		while {
			// Read the incoming stream data until it closes
			let mut buf = [ 0; 1024 ];
			let read = match stream.read( &mut buf ).await {
				Ok( n ) => n,
				Err( e ) => {
					println!( "An error occured while reading stream data: {}", e );
					0
				}
			};
			
			if read != 0 {
				// Get the slice
				let mut slice: Vec< u8 > = Vec::new();
				slice.extend_from_slice( &buf[ .. read  ] );
				
				// Broadcast to all listeners
				arc.read().await.bus.write().await.broadcast( slice );
			}
			
			read != 0
		}  {}
		
		println!( "Cleaning up source {}", path );
		// Clean up and remove the source
		let mut serv = server.lock().await;
		serv.sources.remove( &path );
		println!( "Removed source {}", path );
	} else if method == "PUT" {
		println!( "Received a PUT request!" );
		// TODO Implement the PUT method
		// For now, return a 405
		stream.write_all( b"HTTP/1.0 405 Method Not Allowed\r\n" ).await?;
		stream.write_all( ( format!( "Server: {}\r\n", server_id ) ).as_bytes() ).await?;
		stream.write_all( b"Connection: Close\r\n" ).await?;
		stream.write_all( b"Allow: GET, SOURCE\r\n" ).await?;
		stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
		stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
		stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
		stream.write_all( b"Pragma: no-cache\r\n\r\n" ).await?;
		stream.write_all( b"Access-Control-Allow-Origin: *\r\n" ).await?;
	} else if method == "GET" {
		println!( "Received a GET request!" );
		
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
		
		let mut serv = server.lock().await;
		let source_option = serv.sources.get( &source_id );
		// Check if the source is valid
		if let Some( source_lock ) = source_option {
			println!( "Received a client on {}", source_id );
			let mut source = source_lock.write().await;
			let reader = source.bus.write().await.add_rx();
			
			// Reply with a 200 OK
			send_listener_ok( &mut stream, server_id, &source.properties, serv.properties.metaint, source.content_type.as_str() ).await?;
			
			// Create a client
			// Check if metadata is enabled
			let meta_enabled = get_header( "Icy-MetaData", req.headers ).unwrap_or( b"0" ) == b"1";
			// Get a valid UUID
			let client_id = {
				let mut unique = Uuid::new_v4();
				// Hopefully this doesn't take until the end of time
				while serv.clients.contains_key( &unique ) {
					unique = Uuid::new_v4();
				}
				unique
			};

			// Add the client id to the list of clients attached to the source
			source.clients.insert( client_id );

			// No more need for source
			drop( source );

			let client = Client{
				reader: RwLock::new( reader ),
				id: client_id,
				metadata: meta_enabled,
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
				}	
			};
			
			println!( "Client has metadata: {}", meta_enabled );
			
			// Get the metaint
			let metalen = serv.properties.metaint;
			// Add our client
			// The map with an id may be unnecessary, but oh well
			// That can be removed later
			let arc_client = Arc::new( RwLock::new( client ) );
			serv.clients.insert( client_id, arc_client.clone() );
			drop( serv );
			
			println!( "Serving client{} ", client_id );
			
			// TODO Add bursting
			// TODO Kick slow listeners
			
			let mut sent_count = 0;
			loop {
				// Receive whatever bytes, then send to the client
				let client = arc_client.read().await;
				let res = client.reader.write().await.recv();
				// Check if the bus is still alive
				if let Ok( read ) = res {
					// Consume it here or something
					let send: Option< Vec< u8 > >;
					if meta_enabled {
						let new_sent = sent_count + read.len();
						// Check if we need to send the metadata
						if new_sent > metalen {
							// Get n and the metadata in a vec
							let meta_vec = get_metadata_vec( {
								let serv = server.lock().await;
								if let Some( source_lock ) = serv.sources.get( &source_id ) {
									let source = source_lock.read().await;
									
									source.metadata.as_ref().cloned()
								} else {
									None
								}
							} );
							
							// Create a new vector to hold our data
							let mut inserted: Vec< u8 > = Vec::new();
							// Insert the current range
							let mut index = metalen - sent_count;
							if index > 0 {
								inserted.extend_from_slice( &read[ .. index ] );
							}
							while index < read.len() {
								inserted.extend_from_slice( &meta_vec );
								
								// Add the data
								let end = std::cmp::min( read.len(), index + metalen );
								if index != end {
									inserted.extend_from_slice( &read[ index .. end ] );
									index = end;
								} else {
									// index should equal read.len() at this point, unless metalen is 0
								}
							}
							
							// Update the total sent amount and send
							sent_count = new_sent % metalen;
							send = Some( inserted );
						} else {
							// Copy over the new amount
							sent_count = new_sent;
							send = Some( read );
						}
					} else {
						send = Some( read );
					}
					
					if let Some( data ) = send {
						match stream.write_all( &data ).await {
							Ok( _ ) => (),
							Err( e ) => {
								println!( "An error occured while streaming: {}", e );
								break
							}
						}
					}
				} else {
					break;
				}
			}
			
			println!( "Client {} has disconnected!", client_id );
			let mut serv = server.lock().await;
			serv.clients.remove( &client_id );
			// Remove the client from the list of listeners
			if let Some( source ) = serv.sources.get( &source_id ) {
				source.write().await.clients.remove( &client_id );
			}
			drop( serv );
		} else {
			// Figure out what the request wants
			// /admin/metadata for updating the metadata
			// Anything else is not vanilla
			// Return a 404 otherwise
			
			if path == "/admin/metadata" {
				// Check for authorization
				if let Some( ( name, pass ) ) = get_basic_auth( req.headers ) {
					// For testing purposes right now
					// TODO Add proper configuration
					if name != "admin" || pass != "hackme" {
						send_unauthorized( &mut stream, server_id, Some( "Invalid credentials" ) ).await?;
						return Ok( () )
					}
				} else {
					// No auth, return and close
					send_unauthorized( &mut stream, server_id, Some( "You need to authenticate" ) ).await?;
					return Ok( () )
				}
				
				// Authentication passed
				// Now check the query fields
				// Takes in mode, mount, song and url
				if let Some( queries ) = queries {
					let mut mode = None;
					let mut mount = None;
					let mut song = None;
					let mut url = None;
					for query in queries {
						println!( "Received query {}={}", query.field, query.value );
						match query.field.as_str() {
							"mode" => mode = Some( query.value ),
							"mount" => mount = Some( query.value ),
							"song" => song = Some( query.value ),
							"url" => url = Some( query.value ),
							_ => (),
						}
					}
					
					if let Some( mode ) = mode {
						if mode == "updinfo" {
							if let Some( mount ) = mount {
								let source_option = serv.sources.get( &mount );
								if let Some( source ) = source_option {
									if song.is_some() || url.is_some() {
										let new_metadata = IcyMetadata {
											title: song,
											url
										};
										source.write().await.metadata = Some( new_metadata );
									} else {
										source.write().await.metadata = None;
									}
									// Ok
									send_ok( &mut stream, server_id, Some( "Success" ) ).await?;
								} else {
									// Unknown source
									send_forbidden( &mut stream, server_id, Some( "Invalid mount" ) ).await?;
								}
							} else {
								send_bad_request( &mut stream, server_id, Some( "No mount specified" ) ).await?;
							}
						} else {
							// Don't know what sort of mode
							// Bad request
							send_bad_request( &mut stream, server_id, Some( "Invalid mode" ) ).await?;
						}
					} else {
						// No mode specified
						send_bad_request( &mut stream, server_id, Some( "No mode specified" ) ).await?;
					}
				} else {
					// Bad request
					send_bad_request( &mut stream, server_id, Some( "Invalid query" ) ).await?;
				}
			} else {
				// Return 404 for now
				send_not_found( &mut stream, server_id, Some( "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>" ) ).await?;
			}
		}
	} else {
		// Unknown
		stream.write_all( b"HTTP/1.0 405 Method Not Allowed\r\n" ).await?;
		stream.write_all( ( format!( "Server: {}\r\n", server_id ) ).as_bytes() ).await?;
		stream.write_all( b"Connection: Close\r\n" ).await?;
		stream.write_all( b"Allow: GET, SOURCE\r\n" ).await?;
		stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
		stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
		stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
		stream.write_all( b"Pragma: no-cache\r\n\r\n" ).await?;
		stream.write_all( b"Access-Control-Allow-Origin: *\r\n" ).await?;
	}
	
	Ok( () )
}

async fn send_listener_ok( stream: &mut TcpStream, id: String, properties: &IcyProperties, metaint: usize, content_type: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n" ).await?;
	
	// Properties or default
	stream.write_all( ( format!( "icy-description:{}\r\n", properties.description.as_ref().unwrap_or( &"Unknown".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-genre:{}\r\n", properties.genre.as_ref().unwrap_or( &"Undefined".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-name:{}\r\n", properties.name.as_ref().unwrap_or( &"Unnamed Station".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-pub:{}\r\n", properties.public as usize ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-url:{}\r\n", properties.url.as_ref().unwrap_or( &"Unknown".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-metaint:{}\r\n\r\n", metaint ) ).as_bytes() ).await?;
	
	Ok( () )
}

async fn send_not_found( stream: &mut TcpStream, id: String, message: Option< &str > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 404 File Not Found\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if message.is_some() {
		stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( text ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
//	stream.write_all( b"<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>\r\n" ).await?;
	
	Ok( () )
}

async fn send_ok( stream: &mut TcpStream, id: String, message: Option< &str > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if message.is_some() {
		stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( text ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_bad_request( stream: &mut TcpStream, id: String, message: Option< &str > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 400 Bad Request\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if message.is_some() {
		stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( text ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_forbidden( stream: &mut TcpStream, id: String, message: Option< &str > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 403 Forbidden\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if message.is_some() {
		stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	}
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( text ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

async fn send_unauthorized( stream: &mut TcpStream, id: String, message: Option< &str > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 401 Authorization Required\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	if message.is_some() {
		stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	}
	stream.write_all( b"WWW-Authenticate: Basic realm=\"Icy Server\"\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	if let Some( text ) = message {
		stream.write_all( text.as_bytes() ).await?;
	}
	
	Ok( () )
}

/**
 * Get a vector containing n and the padded data
 */
fn get_metadata_vec( metadata: Option< IcyMetadata > ) -> Vec< u8 > {
	// Could just return a vec that contains n and any optional data
	let mut subvec = vec![ 0 ];
	if let Some( icy_metadata ) = metadata {
		subvec.extend_from_slice( b"StreamTitle='" );
		if let Some( title ) = icy_metadata.title {
			subvec.extend_from_slice( title.as_bytes() );
		}
		subvec.extend_from_slice( b"';StreamUrl='" );
		if let Some( url ) = icy_metadata.url {
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
			// Apparently decode doesn't treat + as a space
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

fn get_properties( headers: &[ httparse::Header< '_ > ] ) -> IcyProperties {
	let mut properties = IcyProperties::new();
	for header in headers {
		let name = header.name;
		let val = std::str::from_utf8( header.value ).unwrap_or( "" );
		
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
	properties
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

fn default_property_port() -> u16 {
	PORT
}

fn default_property_metaint() -> usize {
	METAINT
}

fn default_property_server_id() -> String {
	SERVER_ID.to_string()
}

fn default_property_admin() -> String {
	ADMIN.to_string()
}

fn default_property_host() -> String {
	HOST.to_string()
}

fn default_property_location() -> String {
	LOCATION.to_string()
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
			println!( "Attempting to save the config to file" );
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
		}
		Err( e ) => println!( "An error occured while trying to read the config: {}", e ),
	};
	
	println!( "Attempting to bind to port {}", properties.port );
	match TcpListener::bind( SocketAddr::new( IpAddr::V4( Ipv4Addr::new( 127, 0, 0, 1 ) ), properties.port ) ).await {
		Ok( listener ) => {
			let server = Arc::new( Mutex::new( Server::new( properties ) ) );
			
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
