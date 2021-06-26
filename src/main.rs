extern crate base64;
extern crate bus;
extern crate httparse;
extern crate regex;
extern crate tokio;
extern crate urlencoding;

use base64::decode;
use bus::{ Bus, BusReader };
use httpdate::fmt_http_date;
use path_clean::clean;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::net::{ SocketAddr, IpAddr, Ipv4Addr };
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::time::SystemTime;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use tokio::net::{ TcpListener, TcpStream };
use tokio::sync::{ Mutex, RwLock };

const PORT: u16 = 1900;
const METAINT: usize = 16_000;

#[ derive( Copy, Clone ) ]
struct Query< 'b > {
	field: &'b str,
	value: &'b str
}

struct Client {
	reader: RwLock< BusReader< Box< Vec< u8 > > > >,
	id: usize,
	metadata: bool
}

struct IcyProperties {
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
	bus: RwLock< Bus< Box< Vec< u8 > > > >
}

struct Server {
	sources: HashMap< String, Arc< RwLock< Source > > >,
	clients: HashMap< usize, Arc< RwLock< Client > > >,
	client_counter: AtomicUsize,
	icy_metaint: usize
}

impl Server {
	fn new( metaint: usize ) -> Server {
		Server{
			sources: HashMap::new(),
			clients: HashMap::new(),
			client_counter: AtomicUsize::new( 0 ),
			icy_metaint: metaint
		}
	}
}

async fn handle_connection( server: Arc< Mutex< Server > >, mut stream: TcpStream ) -> Result< (), Box< dyn Error > > {
	let mut buf = Vec::new();
	let mut buffer = [ 0; 512 ];
	
	while {
		let mut headers = [ httparse::EMPTY_HEADER; 16 ];
		let mut req = httparse::Request::new( &mut headers );
		let read = stream.read( &mut buffer ).await?;
		buf.extend_from_slice( &buffer[ .. read ] );
		
		req.parse( &buf )?.is_partial()
	} {}

	let mut headers = [ httparse::EMPTY_HEADER; 16 ];
	let mut req = httparse::Request::new( &mut headers );
	let _offset = req.parse( &buf ).unwrap();

	let method = req.method.unwrap();
	// Apparently decode doesn't treat + as a space
	let decoded = urlencoding::decode( req.path.unwrap().replace( "+", " " ).as_str() );
	if decoded.is_err() {
		send_bad_request( &mut stream, "Invalid location requested" ).await?;
		return Ok( () );
	}
	
	// Extract the queries
	let decoded_string = decoded.unwrap();
	let ( base_path, queries ) = extract_queries( decoded_string.as_str() );
	let path = clean( base_path );
	
	println!( "Received headers with method {} and path {}", method, decoded_string );

	if method == "SOURCE" {
		println!( "Received a SOURCE request!" );
		// Check for authorization
		if let Some( ( name, pass ) ) = get_basic_auth( req.headers ) {
			// For testing purposes right now
			if name != "source" || pass != "hackme" {
				send_unauthorized( &mut stream ).await?;
				return Ok( () )
			}
		} else {
			// No auth, return and close
			send_unauthorized( &mut stream ).await?;
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
			send_forbidden( &mut stream, "Invalid mountpoint" ).await?;
			return Ok( () )
		}
		
		println!( "Source is attempting to use mountpoint {}", path );
		// Check if it is valid
		let dir = Path::new( &path );
		if let Some( parent ) = dir.parent() {
			if let Some( parent_str ) = parent.to_str() {
				if parent_str != "/" {
					send_forbidden( &mut stream, "Invalid mountpoint" ).await?;
					return Ok( () )
				}
			} else {
				send_forbidden( &mut stream, "Invalid mountpoint!" ).await?;
				return Ok( () )
			}
		} else {
			send_forbidden( &mut stream, "Invalid mountpoint!" ).await?;
			return Ok( () )
		}
		
		let mut serv = server.lock().await;
		// Check if the mountpoint is already in use
		if serv.sources.contains_key( &path ) {
			send_forbidden( &mut stream, "Mountpoint in use" ).await?;
			return Ok( () )
		}
		
		let content_type_option = get_header( "Content-Type", req.headers );
		if content_type_option.is_none() {
			send_forbidden( &mut stream, "No Content-type given" ).await?;
			return Ok( () )
		}
		
		// Parse the headers for the source properties
		let source = Source {
			mountpoint: path.clone(),
			properties: get_properties( req.headers ),
			content_type: std::str::from_utf8( content_type_option.unwrap() ).unwrap().to_string(),
			metadata: Some( IcyMetadata {
				title: Some( "NieR:Automata Piano Collections - 1.1 – Weight of the World // 榊原　大".to_string() ),
				url: Some( "https://shop.r10s.jp/book/cabinet/6196/4988601466196.jpg".to_string() )
			} ),
			bus: RwLock::new( Bus::new( 512 ) )
		};
		
		// Add to the server
		let arc = Arc::new( RwLock::new( source ) );
		serv.sources.insert( path.clone(), arc.clone() );
		drop( serv );

		// Give an 200 OK response
		send_source_ok( &mut stream ).await?;
		
		println!( "Successfully mounted source {}", path );
		
		// Listen for bytes
		while {
			// Read the incoming stream data until it closes
			let mut buf = [ 0; 1024 ];
			let read = stream.read( &mut buf ).await?;
			
			if read != 0 {
				// Get the slice
				let mut slice: Vec< u8 > = Vec::new();
				slice.extend_from_slice( &buf[ .. read  ] );
				
				// Broadcast to all listeners
				arc.read().await.bus.write().await.broadcast( Box::new( slice ) );
			} else {
				println!( "Received a length of 0!" );
			}
			
			read != 0
		}  {}
		
		println!( "Cleaning up source {}", path );
		// Clean up and remove the source
		let mut serv = server.lock().await;
		serv.sources.remove( &path );
		println!( "Removed source {}", path );
	} else if method == "PUTs" {
		println!( "Received a PUT request!" );
		// TODO Implement the PUT method
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
		
		println!( "Attempting to add client to {}", source_id );
		
		let mut serv = server.lock().await;
		let source_option = serv.sources.get( &source_id );
		// Check if the source is valid
		if let Some( source_lock ) = source_option {
			println!( "Received a client on {}", source_id );
			let source = source_lock.read().await;
			let reader = source.bus.write().await.add_rx();
			
			// Reply with a 200 OK
			send_listener_ok( &mut stream, &source.properties, serv.icy_metaint, source.content_type.as_str() ).await?;
			
			// No more need for source
			drop( source );
			
			// Create a client
			// Check if metadata is enabled
			let meta_enabled = get_header( "Icy-MetaData", req.headers ).unwrap_or( b"0" ) == b"1";
			let client_id = serv.client_counter.fetch_add( 1, Ordering::Relaxed );
			let client = Client{
				reader: RwLock::new( reader ),
				id: client_id,
				metadata: meta_enabled
			};
			
			println!( "Client has metadata: {}", meta_enabled );
			
			// Get the metaint
			let metalen = serv.icy_metaint;
			// Add our client
			// The map with an id may be unnecessary, but oh well
			// That can be removed later
			let arc_client = Arc::new( RwLock::new( client ) );
			serv.clients.insert( client_id, arc_client.clone() );
			drop( serv );
			
			println!( "Serving client{} ", client_id );
			
			let mut sent_count = 0;
			loop {
				// Receive whatever bytes, then send to the client
				let client = arc_client.read().await;
				let res = client.reader.write().await.recv();
				// Check if the bus is still alive
				if let Ok( read ) = res {
					// Consume it here or something
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
							match stream.write_all( &inserted ).await {
								Ok( _ ) => (),
								Err( e ) => {
									println!( "Got a client error {}", e );
									break
								}
							}
						} else {
							// Copy over the new amount
							sent_count = new_sent;
							// Write it all
							match stream.write_all( &*read ).await {
								Ok( _ ) => (),
								Err( e ) => {
									println!( "Got a client error {}", e );
									break
								}
							}
						}
					} else {
						match stream.write_all( &*read ).await {
							Ok( _ ) => (),
							Err( e ) => {
								println!( "Got a client error {}", e );
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
			drop( serv );
			println!( "Successfully closed out client {}", client_id );
		} else {
			stream.write_all( b"HTTP/1.0 200 OK\r\n\r\n\r\n" ).await?;
			
			// Return 404 for now
//			send_not_found( &mut stream, "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>" ).await?;
			return Ok( () )
			
			// Stats or admin request
			// Or metadata update
			
			// Check for authorization
		}
	} else {
		// Unknown
		println!( "Received headers with method {} and path {}", req.method.unwrap(), req.path.unwrap() );
	
		let mut contains = false;
		for header in req.headers {
			let name = header.name;
			let val = header.value;
			println!( "Got header {:<18}: {}", name, std::str::from_utf8( &val ).unwrap() );
			
			if name == "Authorization" {
				contains = true;
				let base64 = std::str::from_utf8( &val ).unwrap();
				
				let reg = Regex::new( r"^Basic ((?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)$" ).unwrap();
				if let Some( capture ) = reg.captures( base64 ) {
					println!( "name:pass is {}", String::from_utf8( decode( &capture[ 1 ] ).unwrap() ).unwrap() );
				} else {
					println!( "Invalid authorization used!" );
				}
				
			}
		}
	
		if contains {
			println!( "Sending 200 OK" );
			stream.write_all( b"HTTP/1.1 200 OK\r\n" ).await?;
			stream.write_all( b"Server: Icecast 2.5.0\r\n" ).await?;
			stream.write_all( b"Connection: Close\r\n" ).await?;
			stream.write_all( b"Allow: GET, SOURCE\r\n" ).await?;
			stream.write_all( b"Date: Tue, 31 Jan 2017 21:26:13 GMT\r\n" ).await?;
			stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
			stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
			stream.write_all( b"Pragma: no-cache\r\n" ).await?;
			stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
			
			if req.method.unwrap() == "SOURCE" {
				let mut total = 0;
				println!( "Reading body..." );
				while {
					// Read the incoming stream data until
					let mut buf = [ 0; 512 ];
					let read = stream.read( &mut buf ).await?;
					total += read;
					if total > 2_000_000 {
						println!( "Read {}", total );
						total = 0;
					}
					read != 0
				}  {}
				println!( "Source has been closed!" );
			}
		} else {
			println!( "No authorization?" );
			stream.write_all( b"HTTP/1.1 401 Authorization required\r\n\r\n " ).await?;
		}
	}
	
	Ok( () )
}

async fn send_listener_ok( stream: &mut TcpStream, properties: &IcyProperties, metaint: usize, content_type: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
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
	stream.write_all( ( format!( "icy-pub:{}\r\n", {
		if properties.public {
			1
		} else {
			0
		}
	} ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-url:{}\r\n", properties.url.as_ref().unwrap_or( &"Unknown".to_string() ) ) ).as_bytes() ).await?;
	stream.write_all( ( format!( "icy-metaint:{}\r\n\r\n", metaint ) ).as_bytes() ).await?;
	
	Ok( () )
}

async fn send_not_found( stream: &mut TcpStream, message: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 404 File Not Found\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	stream.write_all( message.as_bytes() ).await?;
//	stream.write_all( b"<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>\r\n" ).await?;
	
	Ok( () )
}

async fn send_source_ok( stream: &mut TcpStream ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	
	println!( "Wrote ok to the client!" );
	
	Ok( () )
}

async fn send_bad_request( stream: &mut TcpStream, message: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 400 Bad Request\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	stream.write_all( message.as_bytes() ).await?;
	
	Ok( () )
}

async fn send_forbidden( stream: &mut TcpStream, message: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 403 Forbidden\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	stream.write_all( message.as_bytes() ).await?;
	
	println!( "Sending forbidden! {}", message );
	
	Ok( () )
}

async fn send_unauthorized( stream: &mut TcpStream ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 401 Authorization Required\r\n" ).await?;
	stream.write_all( b"Server: Rusty Zenith 0.0.1\r\n" ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
//	stream.write_all( b"Content-Type: text/plain; charset=utf-8\r\n" ).await?;
	stream.write_all( b"WWW-Authenticate: Basic realm=\"Icy Server\"\r\n" ).await?;
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;
	
	Ok( () )
}

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
			if let Some( ( name, value ) ) = field.split_once( '=' ) {
				queries.push( Query{ field: name, value } );
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
			if let Some( ( name, pass ) ) = std::str::from_utf8( &decode( &capture[ 1 ] ).unwrap() ).unwrap().split_once( ":" ) {
				return Some( ( String::from( name ), String::from( pass ) ) )
			}
		}
	}
	None
}

#[ tokio::main ]
async fn main() -> Result< (), Box< dyn std::error::Error > > {
	let listener = TcpListener::bind( SocketAddr::new( IpAddr::V4( Ipv4Addr::new( 127, 0, 0, 1 ) ), PORT ) ).await?;
	let server = Arc::new( Mutex::new( Server::new( METAINT ) ) );
	
	loop {
		let ( socket, _ ) = listener.accept().await?;
		let server_clone = server.clone();
		
		tokio::spawn( async move {
			handle_connection( server_clone, socket ).await.unwrap();
		} );
	}
}
