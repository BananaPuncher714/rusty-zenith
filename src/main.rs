use httpdate::fmt_http_date;
use httparse::Status;
use regex::Regex;
use serde::{ Deserialize, Serialize };
use serde_json::{ json, Value };
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{ BufWriter, ErrorKind, Write };
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{ Duration, SystemTime, UNIX_EPOCH };
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use tokio::net::{ TcpListener, TcpStream };
use tokio::sync::RwLock;
use tokio::sync::mpsc::{ UnboundedSender, UnboundedReceiver, unbounded_channel };
use tokio::time::timeout;
use tokio_native_tls::TlsStream;
use tokio_native_tls::native_tls::TlsConnector;
use url::Url;
use uuid::Uuid;

// Default constants
const ADDRESS: &str = "0.0.0.0";
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

// How many regular sources, not including relays
const SOURCES: usize = 4;
// How many sources can be connected, in total
const MAX_SOURCES: usize = 4;
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
// The maximum size in bytes of an acceptable http message not including the body
const HTTP_MAX_LENGTH: usize = 8192;
// The maximum number of redirects allowed, when fetching relays from another server/stream
const HTTP_MAX_REDIRECTS: usize = 5;

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

// TODO Add something determining if a source is a relay, or any other kind of source, for that matter
// TODO Implement hidden sources
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

impl Source {
    fn new( mountpoint: String, properties: IcyProperties ) -> Source {
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
struct SourceStats {
    start_time: u64,
    bytes_read: usize,
    peak_listeners: usize
}

// TODO Add a list of "relay" structs
// Relay structs should have an auth of their own, if provided
// Having a master server is not very good
// It would be much better to add relays through an api or something
#[ derive( Serialize, Deserialize, Clone ) ]
struct MasterServer {
    #[ serde( default = "default_property_master_server_enabled" ) ]
    enabled: bool,
    #[ serde( default = "default_property_master_server_url" ) ]
    url: String,
    #[ serde( default = "default_property_master_server_update_interval" ) ]
    update_interval: u64,
    #[ serde( default = "default_property_master_server_relay_limit" ) ]
    relay_limit: usize,
}

// TODO Add permissions, specifically source, admin, bypass client count, etc
#[ derive( Serialize, Deserialize, Clone ) ]
struct Credential {
    username: String,
    password: String
}

// Add a total source limit
#[ derive( Serialize, Deserialize, Clone ) ]
struct ServerLimits {
    #[ serde( default = "default_property_limits_clients" ) ]
    clients: usize,
    #[ serde( default = "default_property_limits_sources" ) ]
    sources: usize,
    #[ serde( default = "default_property_limits_total_sources" ) ]
    total_sources: usize,
    #[ serde( default = "default_property_limits_queue_size" ) ]
    queue_size: usize,
    #[ serde( default = "default_property_limits_burst_size" ) ]
    burst_size: usize,
    #[ serde( default = "default_property_limits_header_timeout" ) ]
    header_timeout: u64,
    #[ serde( default = "default_property_limits_source_timeout" ) ]
    source_timeout: u64,
    #[ serde( default = "default_property_limits_http_max_length" ) ]
    http_max_length: usize,
    #[ serde( default = "default_property_limits_http_max_redirects" ) ]
    http_max_redirects: usize,
    #[ serde( default = "default_property_limits_source_limits" ) ]
    source_limits: HashMap< String, SourceLimits >
}

#[ derive( Serialize, Deserialize, Clone ) ]
struct ServerProperties {
    // Ideally, there would be multiple addresses and ports and TLS support
    #[ serde( default = "default_property_address" ) ]
    address: String,
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
    users: Vec< Credential >,
    #[ serde( default = "default_property_master_server" ) ]
    master_server: MasterServer
}

impl ServerProperties {
    fn new() -> ServerProperties {
        ServerProperties {
            address: default_property_address(),
            port: default_property_port(),
            metaint: default_property_metaint(),
            server_id: default_property_server_id(),
            admin: default_property_admin(),
            host: default_property_host(),
            location: default_property_location(),
            description: default_property_description(),
            limits: default_property_limits(),
            users: default_property_users(),
            master_server: default_property_master_server()
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

struct Server {
    sources: HashMap< String, Arc< RwLock< Source > > >,
    clients: HashMap< Uuid, ClientProperties >,
    // TODO Find a better place to put these, for constant time fetching
    source_count: usize,
    relay_count: usize,
    properties: ServerProperties,
    stats: ServerStats
}

impl Server {
    fn new( properties: ServerProperties ) -> Server {
        Server{
            sources: HashMap::new(),
            clients: HashMap::new(),
            source_count: 0,
            relay_count: 0,
            properties,
            stats: ServerStats::new()
        }
    }
}

enum Stream {
    Plain( TcpStream ),
    Tls( Box< TlsStream< TcpStream > > )
}

impl Stream {
    async fn read( &mut self, buf: &mut [ u8 ] ) -> std::io::Result< usize > {
        match self {
            Stream::Plain( stream ) => stream.read( buf ).await,
            Stream::Tls( stream ) => stream.read( buf ).await
        }
    }

    async fn write_all( &mut self, buf: &[ u8 ] ) -> std::io::Result< () > {
        match self {
            Stream::Plain( stream ) => stream.write_all( buf ).await,
            Stream::Tls( stream ) => stream.write_all( buf ).await
        }
    }
}

#[ derive( PartialEq ) ]
enum TransferEncoding {
    Identity,
    Chunked,
    Length( usize )
}

struct StreamDecoder {
    encoding: TransferEncoding,
    remainder: usize,
    chunk: Vec< u8 >
}

impl StreamDecoder {
    fn new( encoding: TransferEncoding ) -> StreamDecoder {
        let remainder = match &encoding {
            TransferEncoding::Length( v ) => *v,
            _ => 1
        };
        StreamDecoder {
            encoding,
            remainder,
            chunk: Vec::new()
        }
    }

    fn decode( &mut self, out: &mut Vec< u8 >, buf: &[ u8 ], length: usize ) -> Result< usize, Box< dyn Error + Send > > {
        if length == 0 || self.is_finished() {
            Ok( 0 )
        } else {
            match &self.encoding {
                TransferEncoding::Identity => {
                    out.extend_from_slice( &buf[ .. length ] );
                    Ok( length )
                }
                TransferEncoding::Chunked => {
                    let mut read = 0;
                    let mut index = 0;
                    while index < length && self.remainder != 0 {
                        match self.remainder {
                            1 => {
                                // Get the chunk size
                                self.chunk.push( buf[ index ] );
                                index += 1;
                                if self.chunk.windows( 2 ).nth_back( 0 ) == Some( b"\r\n" ) {
                                    // Ignore chunk extensions
                                    if let Some( cutoff ) = self.chunk.iter().position( | &x | x == b';' || x == b'\r' ) {
                                        self.remainder = match std::str::from_utf8( &self.chunk[ .. cutoff ] ) {
                                            Ok( res ) =>  match usize::from_str_radix( res, 16 ) {
                                                Ok( hex ) => hex,
                                                Err( e ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, format!( "Invalid value provided for chunk size: {}", e ) ) ) )
                                            }
                                            Err( e ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, format!( "Could not parse chunk size: {}", e ) ) ) )
                                        };
                                        // Check if it's the last chunk
                                        // Ignore trailers
                                        if self.remainder != 0 {
                                            // +2 for remainder
                                            // +2 for extra CRLF
                                            self.remainder += 4;
                                            self.chunk.clear();
                                        }
                                    } else {
                                        return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Missing CRLF" ) ) )
                                    }
                                }
                            }
                            2 => {
                                // No more chunk data should be read
                                if self.chunk.windows( 2 ).nth_back( 0 ) == Some( b"\r\n" ) {
                                    // Append current data
                                    read += self.chunk.len() - 2;
                                    out.extend_from_slice( &self.chunk[ .. self.chunk.len() - 2 ] );
                                    // Prepare for reading the next chunk size
                                    self.remainder = 1;
                                    self.chunk.clear();
                                } else {
                                    return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Missing CRLF from chunk" ) ) )
                                }
                            }
                            v => {
                                // Get the chunk data
                                let max_read = std::cmp::min( length - index, v - 2 );
                                self.chunk.extend_from_slice( &buf[ index .. index + max_read ] );
                                index += max_read;
                                self.remainder -= max_read;
                            }
                        }
                    }

                    Ok( read )
                }
                TransferEncoding::Length( _ ) => {
                    let allowed = std::cmp::min( length, self.remainder );
                    if allowed != 0 {
                        out.extend_from_slice( &buf[ .. allowed ] );
                        self.remainder -= allowed;
                    }
                    Ok( allowed )
                }
            }
        }
    }

    fn is_finished( &self ) -> bool {
        self.encoding != TransferEncoding::Identity && self.remainder == 0
    }
}

async fn handle_connection( server: Arc< RwLock< Server > >, mut stream: TcpStream ) -> Result< (), Box< dyn Error > > {
    let ( server_id, header_timeout, http_max_len ) = {
        let properties = &server.read().await.properties;
        ( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length )
    };

    let mut message = Vec::new();
    let mut buf = [ 0; 1024 ];

    // Add a timeout
    timeout( Duration::from_millis( header_timeout ), async {
        loop {
            let mut headers = [ httparse::EMPTY_HEADER; 32 ];
            let mut req = httparse::Request::new( &mut headers );
            let read = stream.read( &mut buf ).await?;
            message.extend_from_slice( &buf[ .. read ] );
            match req.parse( &message ) {
                Ok( Status::Complete( offset ) ) => return Ok( offset ),
                Ok( Status::Partial ) if message.len() > http_max_len => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Request exceeded the maximum allowed length" ) ) ),
                Ok( Status::Partial ) => (),
                Err( e ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, format!( "Received an invalid request: {}", e ) ) ) )
            }
        }
    } ).await??;

    let mut _headers = [ httparse::EMPTY_HEADER; 32 ];
    let mut req = httparse::Request::new( &mut _headers );
    let body_offset = req.parse( &message )?.unwrap();
    let method = req.method.unwrap();

    let ( base_path, queries ) = extract_queries( req.path.unwrap() );
    let path = path_clean::clean( base_path );
    let headers = req.headers;

    match method {
        // Some info about the protocol is provided here: https://gist.github.com/ePirat/adc3b8ba00d85b7e3870
        "SOURCE" | "PUT" => {
            // Check for authorization
            if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                if !validate_user( &server.read().await.properties, name, pass ) {
                    // Invalid user/pass provided
                    return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=urf-8", "Invalid credentials" ) ) ).await;
                }
            } else {
                // No auth, return and close
                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=urf-8", "You need to authenticate" ) ) ).await;
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
                return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await;
            }

            // Check if it is valid
            // For now this assumes the stream directory is /
            let dir = Path::new( &path );
            if let Some( parent ) = dir.parent() {
                if let Some( parent_str ) = parent.to_str() {
                    if parent_str != "/" {
                        return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await;
                    }
                } else {
                    return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await;
                }
            } else {
                return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await;
            }

            // Sources must have a content type
            // Maybe the type that is served should be checked?
            let mut properties = match get_header( "Content-Type", headers ) {
                Some( content_type ) => IcyProperties::new( std::str::from_utf8( content_type )?.to_string() ),
                None => {
                    return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "No Content-type provided" ) ) ).await;
                }
            };

            let mut serv = server.write().await;
            // Check if the mountpoint is already in use
            if serv.sources.contains_key( &path ) {
                return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await;
            }

            // Check if the max number of sources has been reached
            if serv.source_count >= serv.properties.limits.sources || serv.sources.len() >= serv.properties.limits.total_sources {
                return send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Too many sources connected" ) ) ).await;
            }

            let mut decoder: StreamDecoder;

            if method == "SOURCE" {
                // Give an 200 OK response
                send_ok( &mut stream, &server_id, None ).await?;

                decoder = StreamDecoder::new( TransferEncoding::Identity );
            } else {
                // Verify that the transfer encoding is identity or not included
                // No support for chunked or encoding ATM
                // TODO Add support for transfer encoding options as specified here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
                match ( get_header( "Transfer-Encoding", headers ), get_header( "Content-Length", headers ) ) {
                    ( Some( b"identity"), Some( value ) ) | ( None, Some( value ) ) => {
                        // Use content length decoder
                        match std::str::from_utf8( value ) {
                            Ok( string ) => {
                                match string.parse::< usize >() {
                                    Ok( length ) => decoder = StreamDecoder::new( TransferEncoding::Length( length ) ),
                                    Err( _ ) => return send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid Content-Length" ) ) ).await
                                }
                            }
                            Err( _ ) => return send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Unknown unicode found in Content-Length" ) ) ).await
                        }
                    }
                    ( Some( b"chunked" ), None ) => {
                        // Use chunked decoder
                        decoder = StreamDecoder::new( TransferEncoding::Chunked );
                    }
                    ( Some( b"identity" ), None ) | ( None, None ) => {
                        // Use identity
                        decoder = StreamDecoder::new( TransferEncoding::Identity );
                    }
                    _ => return send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Unsupported transfer encoding" ) ) ).await
                }

                // Check if client sent Expect: 100-continue in header, if that's the case we will need to return 100 in status code
                // Without it, it means that client has no body to send, we will stop if that's the case
                match get_header( "Expect", headers ) {
                    Some( b"100-continue" ) => send_continue( &mut stream, &server_id ).await?,
                    Some( _ ) => return send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Expected 100-continue in Expect header" ) ) ).await,
                    None => return send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "PUT request must come with Expect header" ) ) ).await
                }
            }

            // Parse the headers for the source properties
            populate_properties( &mut properties, headers );

            let source = Source::new( path.clone(), properties );

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
            serv.source_count += 1;
            drop( serv );

            println!( "Mounted source on {} via {}", arc.read().await.mountpoint, method );

            if message.len() > body_offset {
                let slice = &message[ body_offset .. ];
                let mut data = Vec::new();
                match decoder.decode( &mut data, slice, message.len() - body_offset ) {
                    Ok( read ) => {
                        if read != 0 {
                            broadcast_to_clients( &arc, data, queue_size, burst_size ).await;
                            arc.read().await.stats.write().await.bytes_read += read;
                        }
                    }
                    Err( e ) => {
                        println!( "An error occurred while decoding stream data from source {}: {}", arc.read().await.mountpoint, e );
                        arc.write().await.disconnect_flag = true;
                    }
                }
            }

            // Listen for bytes
            if !decoder.is_finished() && !arc.read().await.disconnect_flag {
                while {
                    // Read the incoming stream data until it closes
                    let mut buf = [ 0; 1024 ];
                    let read = match timeout( Duration::from_millis( source_timeout ), stream.read( &mut buf ) ).await {
                        Ok( Ok( n ) ) => n,
                        Ok( Err( e ) ) => {
                            println!( "An error occurred while reading stream data from source {}: {}", arc.read().await.mountpoint, e );
                            0
                        }
                        Err( _ ) => {
                            println!( "A source timed out: {}", arc.read().await.mountpoint );
                            0
                        }
                    };

                    let mut data = Vec::new();
                    match decoder.decode( &mut data, &buf, read ) {
                        Ok( decode_read ) => {
                            if decode_read != 0 {
                                broadcast_to_clients( &arc, data, queue_size, burst_size ).await;
                                arc.read().await.stats.write().await.bytes_read += decode_read;
                            }

                            // Check if the source needs to be disconnected
                            read != 0 && !decoder.is_finished() && !arc.read().await.disconnect_flag
                        }
                        Err( e ) => {
                            println!( "An error occurred while decoding stream data from source {}: {}", arc.read().await.mountpoint, e );
                            false
                        }
                    }
                }  {}
            }

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
            serv.source_count -= 1;
            serv.stats.session_bytes_read += source.stats.read().await.bytes_read;

            if method == "PUT" {
                // request must end with server 200 OK response
                send_ok( &mut stream, &server_id, None ).await.ok();
            }

            println!( "Unmounted source {}", source.mountpoint );
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
                    send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Too many listeners connected" ) ) ).await?;
                    return Ok( () )
                }

                // Check if metadata is enabled
                let meta_enabled = get_header( "Icy-MetaData", headers ).unwrap_or( b"0" ) == b"1";

                // Reply with a 200 OK
                send_listener_ok( &mut stream, &server_id, &source.properties, meta_enabled, serv.properties.metaint ).await?;

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
                        if let Some( arr ) = get_header( "User-Agent", headers ) {
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
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
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
                                            send_ok( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Success" ) ) ).await?;
                                        }
                                        None => send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/listclients" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
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
                                            send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
                                        } else {
                                            send_internal_error( &mut stream, &server_id, None ).await?;
                                        }
                                    } else {
                                        send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/fallbacks" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
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
                                        send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
                                    } else {
                                        send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/moveclients" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
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
                                            send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
                                        }
                                        _ => send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/killclient" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
                        }

                        if let Some( queries ) = queries {
                            match get_queries_for( vec![ "mount", "id" ], &queries )[ .. ].as_ref() {
                                [ Some( mount ), Some( uuid_str ) ] => {
                                    match ( serv.sources.get( mount ), Uuid::parse_str( uuid_str ) ) {
                                        ( Some( source ), Ok( uuid ) ) => {
                                            if let Some( client ) = source.read().await.clients.get( &uuid ) {
                                                drop( client.read().await.sender.write().await.send( Arc::new( Vec::new() ) ) );
                                                println!( "Killing client {}", uuid );
                                                send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
                                            } else {
                                                send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid id" ) ) ).await?;
                                            }
                                        }
                                        ( None, _ ) => send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?,
                                        ( Some( _ ), Err( _ ) ) => send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid id" ) ) ).await?,
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/killsource" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
                        }

                        if let Some( queries ) = queries {
                            match get_queries_for( vec![ "mount" ], &queries )[ .. ].as_ref() {
                                [ Some( mount ) ] => {
                                    if let Some( source ) = serv.sources.get( mount ) {
                                        source.write().await.disconnect_flag = true;

                                        println!( "Killing source {}", mount );
                                        send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", "Success" ) ) ).await?;
                                    } else {
                                        send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
                        }
                    }
                    "/admin/listmounts" => {
                        let serv = server.read().await;
                        // Check for authorization
                        if let Some( ( name, pass ) ) = get_basic_auth( headers ) {
                            // For testing purposes right now
                            // TODO Add proper configuration
                            if !validate_user( &serv.properties, name, pass ) {
                                return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await;
                            }
                        } else {
                            // No auth, return and close
                            return send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await;
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
                            send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
                        } else {
                            send_internal_error( &mut stream, &server_id, None ).await?;
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
                            send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
                        } else {
                            send_internal_error( &mut stream, &server_id, None ).await?;
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
                                            send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", &serialized ) ) ).await?;
                                        } else {
                                            send_internal_error( &mut stream, &server_id, None ).await?;
                                        }
                                    } else {
                                        send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mount" ) ) ).await?;
                                    }
                                }
                                _ => send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?,
                            }
                        } else {
                            // Bad request
                            send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid query" ) ) ).await?;
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

                        send_ok( &mut stream, &server_id, Some( ( "application/json; charset=utf-8", &response.to_string() ) ) ).await?;

                    }
                    // Return 404
                    _ => send_not_found( &mut stream, &server_id, Some( ( "text/html; charset=utf-8", "<html><head><title>Error 404</title></head><body><b>404 - The file you requested could not be found</b></body></html>" ) ) ).await?,
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

async fn read_http_response( stream: &mut Stream, buffer: &mut Vec< u8 >, max_len: usize ) -> Result< usize, Box< dyn Error > > {
    let mut buf = [ 0; 1024 ];
    loop {
        let mut headers = [ httparse::EMPTY_HEADER; 32 ];
        let mut res = httparse::Response::new( &mut headers );
        let read = stream.read( &mut buf ).await?;
        buffer.extend_from_slice( &buf[ .. read ] );
        match res.parse( &buffer ) {
            Ok( Status::Complete( offset ) ) => return Ok( offset ),
            Ok( Status::Partial ) if buffer.len() > max_len => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Request exceeded the maximum allowed length" ) ) ),
            Ok( Status::Partial ) => (),
            Err( e ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, format!( "Received an invalid request: {}", e ) ) ) )
        }
    }
}

async fn connect_and_redirect( url: String, headers: Vec< String >, max_len: usize, max_redirects: usize ) -> Result< ( Stream, Vec< u8 > ), Box< dyn Error > > {
    let mut str_url = url;
    let mut remaining_redirects = max_redirects;
    loop {
        let mut url = Url::parse( str_url.as_str() )?;
        if let Some( host ) = url.host_str() {
            let addr = {
                if let Some( port ) = url.port_or_known_default() {
                    format!( "{}:{}", host, port )
                } else {
                    host.to_string()
                }
            };

            let mut stream = match url.scheme() {
                "https" => {
                    // Use tls
                    let stream = TcpStream::connect( addr.clone() ).await?;
                    let cx = tokio_native_tls::TlsConnector::from( TlsConnector::builder().build()? );
                    Stream::Tls( Box::new( cx.connect( host, stream ).await? ) )
                }
                _ => {
                    Stream::Plain( TcpStream::connect( addr.clone() ).await? )
                }
            };

            // Build the path
            let mut path = url.path().to_string();
            if let Some( query ) = url.query() {
                path = format!( "{}?{}", path, query );
            }
            if let Some( fragment ) = url.fragment() {
                path = format!( "{}#{}", path, fragment );
            }

            // Write the message
            let mut req_buf = Vec::new();
            req_buf.extend_from_slice( format!( "GET {} HTTP/1.1\r\n", path ).as_bytes() );
            let mut auth_included = false;
            for header in &headers {
                req_buf.extend_from_slice( header.as_bytes() );
                req_buf.extend_from_slice( b"\r\n" );
                auth_included |= header.to_lowercase().starts_with( "authorization:" );
            }
            req_buf.extend_from_slice( format!( "Host: {}\r\n", addr ).as_bytes() );
            if !auth_included {
                if let Some( passwd ) = url.password() {
                    let encoded = base64::encode( format!( "{}:{}", url.username(), passwd ) );
                    req_buf.extend_from_slice( format!( "Authorization: Basic {}\r\n", encoded ).as_bytes() );
                }
            }
            req_buf.extend_from_slice( b"\r\n" );
            stream.write_all( &req_buf ).await?;

            let mut buf = Vec::new();
            // First time parsing the response
            read_http_response( &mut stream, &mut buf, max_len ).await?;

            let mut _headers = [ httparse::EMPTY_HEADER; 32 ];
            let mut res = httparse::Response::new( &mut _headers );

            // Second time parsing the response
            if res.parse( &buf )? == Status::Partial {
                return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete response" ) ) );
            }

            match res.code {
                Some( code ) => {
                    if code / 100 == 3 || code == 201 {
                        if remaining_redirects == 0 {
                            // Reached maximum number of redirects!
                            return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Maximum redirects reached" ) ) );
                        } else if let Some( location ) = get_header( "Location", res.headers ) {
                            // Try parsing it into a URL first
                            let loc_str = std::str::from_utf8( location )?;
                            if let Ok( mut redirect ) = Url::parse( loc_str ) {
                                redirect.set_query( url.query() );
                                str_url = redirect.as_str().to_string();
                            } else {
                                if location[ 0 ] == b'/' {
                                    url.set_path( loc_str );
                                } else {
                                    url.join( loc_str )?;
                                }
                                str_url = url.as_str().to_string();
                            }

                            remaining_redirects -= 1;
                        } else {
                            return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Invalid Location" ) ) );
                        }
                    } else {
                        return Ok( ( stream, buf ) );
                    }
                }
                None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Missing response code" ) ) )
            }
        } else {
            return Err( Box::new( std::io::Error::new( ErrorKind::AddrNotAvailable, format!( "Invalid URL provided: {}", str_url ) ) ) );
        }
    }
}

async fn master_server_mountpoints( server: &Arc< RwLock< Server > >, master_info: &MasterServer ) -> Result< Vec<String>, Box< dyn Error > > {
    // Get all master mountpoints
    let ( server_id, header_timeout, http_max_len, http_max_redirects ) = {
        let properties = &server.read().await.properties;
        ( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length, properties.limits.http_max_redirects )
    };

    // read headers from client
    let headers = vec![ format!( "User-Agent: {}", server_id ), "Connection: Closed".to_string() ];
    let ( mut sock, message ) = timeout( Duration::from_millis( header_timeout ), connect_and_redirect( format!( "{}/api/serverinfo", master_info.url ), headers, http_max_len, http_max_redirects ) ).await??;

    let mut headers = [ httparse::EMPTY_HEADER; 32 ];
    let mut res = httparse::Response::new( &mut headers );

    let body_offset = match res.parse( &message )? {
        Status::Complete( offset ) => offset,
        Status::Partial => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete response" ) ) )
    };

    let mut len = match get_header( "Content-Length", res.headers ) {
        Some( val ) => {
            let parsed = std::str::from_utf8( val )?;
            parsed.parse::< usize >()?
        },
        None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "No Content-Length specified" ) ) )
    };

    match res.code {
        Some( 200 ) => (),
        Some( code ) => return Err( Box::new( std::io::Error::new( ErrorKind::Other, format!( "Invalid response: {} {}", code, res.reason.unwrap() ) ) ) ),
        None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Missing response code" ) ) )
    }

    let source_timeout = server.read().await.properties.limits.source_timeout;

    let mut json_slice = Vec::new();
    if message.len() > body_offset {
        len -= message.len() - body_offset;
        json_slice.extend_from_slice( &message[ body_offset .. ] );
    }
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
#[ allow( clippy::blocks_in_if_conditions ) ]
async fn relay_mountpoint( server: Arc< RwLock< Server > >, master_server: MasterServer, mount: String ) -> Result< (), Box< dyn Error > > {
    let ( server_id, header_timeout, http_max_len, http_max_redirects ) = {
        let properties = &server.read().await.properties;
        ( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length, properties.limits.http_max_redirects )
    };

    // read headers from server
    let headers = vec![ format!( "User-Agent: {}", server_id ), "Connection: Closed".to_string(), "Icy-Metadata:1".to_string() ];
    let ( mut sock, buf ) = timeout( Duration::from_millis( header_timeout ), connect_and_redirect( format!( "{}{}", master_server.url, mount ), headers, http_max_len, http_max_redirects ) ).await??;

    let mut headers = [ httparse::EMPTY_HEADER; 32 ];
    let mut res = httparse::Response::new( &mut headers );

    let body_offset = match res.parse( &buf )? {
        Status::Complete( offset )=> offset,
        Status::Partial => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete response" ) ) )
    };

    match res.code {
        Some( 200 ) => (),
        Some( code ) => return Err( Box::new( std::io::Error::new( ErrorKind::Other, format!( "Invalid response: {}", code ) ) ) ),
        None => return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Missing response code" ) ) )
    }

    // checking if our peer is really an icecast server
    if get_header( "icy-name", res.headers ).is_none() {
        return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Is this a valid icecast stream?" ) ) );
    }

    let mut decoder = match ( get_header( "Transfer-Encoding", res.headers ), get_header( "Content-Length", res.headers ) ) {
        ( Some( b"identity"), Some( value ) ) | ( None, Some( value ) ) => {
            // Use content length decoder
            match std::str::from_utf8( value ) {
                Ok( string ) => {
                    match string.parse::< usize >() {
                        Ok( length ) => StreamDecoder::new( TransferEncoding::Length( length ) ),
                        Err( _ ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Invalid Content-Length" ) ) )
                    }
                }
                Err( _ ) => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Unknown unicode found in Content-Length" ) ) )
            }
        }
        ( Some( b"chunked" ), None ) => StreamDecoder::new( TransferEncoding::Chunked ),
        ( Some( b"identity" ), None ) | ( None, None ) => StreamDecoder::new( TransferEncoding::Identity ),
        _ => return Err( Box::new( std::io::Error::new( ErrorKind::InvalidData, "Unsupported Transfer-Encoding" ) ) )
    };

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
        if serv.relay_count >= master_server.relay_limit {
            return Err( Box::new( std::io::Error::new( ErrorKind::Other, "The server relay limit has been reached" ) ) );
        } else if serv.sources.len() >= serv.properties.limits.total_sources {
            return Err( Box::new( std::io::Error::new( ErrorKind::Other, "The server total source limit has been reached" ) ) );
        }

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
        serv.relay_count += 1;
        drop( serv );

        struct MetaParser {
            metaint: usize,
            vec: Vec< u8 >,
            remaining: usize
        }

        let metaint = match get_header( "Icy-Metaint", res.headers ) {
            Some( val ) => std::str::from_utf8( val )?.parse::< usize >()?,
            None => 0
        };
        let mut meta_info = MetaParser {
            metaint,
            vec: Vec::new(),
            remaining: metaint
        };

        println!( "Mounted relay on {}", arc.read().await.mountpoint );

        if buf.len() > body_offset {
            let slice = &buf[ body_offset .. ];
            let mut data = Vec::new();
            // This bit of code is really ugly, but oh well
            match decoder.decode( &mut data, slice, buf.len() - body_offset ) {
                Ok( read ) => {
                    if read != 0 {
                        // Process any metadata, if it exists
                        let data = {
                            if meta_info.metaint != 0 {
                                let mut trimmed = Vec::new();
                                let mut position = 0;
                                let mut last_full: Option< Vec< u8 > > = None;
                                while position < read {
                                    // Either reading in regular stream data
                                    // Reading in the length of the metadata
                                    // Or reading the metadata directly
                                    if meta_info.remaining != 0 {
                                        let frame_length = std::cmp::min( read - position, meta_info.remaining );
                                        if frame_length != 0 {
                                            trimmed.extend_from_slice( &data[ position .. position + frame_length ] );
                                            meta_info.remaining -= frame_length;
                                            position += frame_length;
                                        }
                                    } else if meta_info.vec.is_empty() {
                                        // Reading the length of the metadata segment
                                        meta_info.vec.push( data[ position ] );
                                        position += 1;
                                    } else {
                                        // Reading in metadata
                                        let size = 1 + ( ( meta_info.vec[ 0 ] as usize ) << 4 );
                                        let remaining_metadata = std::cmp::min( read - position, size - meta_info.vec.len() );
                                        meta_info.vec.extend_from_slice( &data[ position .. position + remaining_metadata ] );
                                        position += remaining_metadata;

                                        // If it's reached the max size, then copy it over to last_full
                                        if meta_info.vec.len() == size {
                                            meta_info.remaining = meta_info.metaint;
                                            last_full = Some( meta_info.vec.clone() );
                                            meta_info.vec.clear();
                                        }
                                    }
                                }

                                // Update the source's metadata
                                if let Some( metadata_vec ) = last_full {
                                    if !{
                                        let serv_vec = &arc.read().await.metadata_vec;
                                        serv_vec.len() == metadata_vec.len() && serv_vec.iter().eq( metadata_vec.iter() )
                                    } {
                                        if metadata_vec[ .. ] == [ 1; 0 ] {
                                            let mut serv = arc.write().await;
                                            println!( "Updated relay {} metadata with no title and url", serv.mountpoint );
                                            serv.metadata_vec = vec![ 0 ];
                                            serv.metadata = None;
                                        } else {
                                            let cut = {
                                                let mut last = metadata_vec.len();
                                                while metadata_vec[ last - 1 ] == 0 {
                                                    last -= 1;
                                                }
                                                last
                                            };
                                            if let Ok( meta_str ) = std::str::from_utf8( &metadata_vec[ 1 .. cut ] ) {
                                                let reg = Regex::new( r"^StreamTitle='(.+?)';StreamUrl='(.+?)';$" ).unwrap();
                                                if let Some( captures ) = reg.captures( meta_str ) {
                                                    let metadata = IcyMetadata {
                                                        title: {
                                                            let m_str = captures.get( 1 ).unwrap().as_str();
                                                            if m_str.is_empty() {
                                                                None
                                                            } else {
                                                                Some( m_str.to_string() )
                                                            }
                                                        },
                                                        url: {
                                                            let m_str = captures.get( 2 ).unwrap().as_str();
                                                            if m_str.is_empty() {
                                                                None
                                                            } else {
                                                                Some( m_str.to_string() )
                                                            }
                                                        }
                                                    };

                                                    let mut serv = arc.write().await;
                                                    println!( "Updated relay {} metadata with title '{}' and url '{}'", serv.mountpoint, metadata.title.as_ref().unwrap_or( &"".to_string() ), metadata.url.as_ref().unwrap_or( &"".to_string() ) );
                                                    serv.metadata_vec = metadata_vec;
                                                    serv.metadata = Some( metadata );
                                                } else {
                                                    println!( "Unknown metadata format received from relay {}: `{}`", arc.read().await.mountpoint, meta_str );
                                                    arc.write().await.disconnect_flag = true;
                                                }
                                            } else {
                                                println!( "Invalid metadata parsed from relay {}", arc.read().await.mountpoint );
                                                arc.write().await.disconnect_flag = true;
                                            }
                                        }
                                    }
                                }

                                trimmed
                            } else {
                                data
                            }
                        };

                        if !data.is_empty() {
                            arc.read().await.stats.write().await.bytes_read += data.len();
                            broadcast_to_clients( &arc, data, queue_size, burst_size ).await;
                        }
                    }
                }
                Err( e ) => {
                    println!( "An error occurred while decoding stream data from relay {}: {}", arc.read().await.mountpoint, e );
                    arc.write().await.disconnect_flag = true;
                }
            }
        }

        // Listen for bytes
        if !decoder.is_finished() && !arc.read().await.disconnect_flag {
            while {
                // Read the incoming stream data until it closes
                let mut buf = [ 0; 1024 ];
                let read = match timeout( Duration::from_millis( source_timeout ), sock.read( &mut buf ) ).await {
                    Ok( Ok( n ) ) => n,
                    Ok( Err( e ) ) => {
                        println!( "An error occurred while reading stream data from relay {}: {}", arc.read().await.mountpoint, e );
                        0
                    }
                    Err( _ ) => {
                        println!( "A relay timed out: {}", arc.read().await.mountpoint );
                        0
                    }
                };

                let mut data = Vec::new();
                match decoder.decode( &mut data, &buf, read ) {
                    Ok( decode_read ) => {
                        if decode_read != 0 {
                            // Process any metadata, if it exists
                            let data = {
                                if meta_info.metaint != 0 {
                                    let mut trimmed = Vec::new();
                                    let mut position = 0;
                                    let mut last_full: Option< Vec< u8 > > = None;
                                    while position < decode_read {
                                        // Either reading in regular stream data
                                        // Reading in the length of the metadata
                                        // Or reading the metadata directly
                                        if meta_info.remaining != 0 {
                                            let frame_length = std::cmp::min( decode_read - position, meta_info.remaining );
                                            trimmed.extend_from_slice( &data[ position .. position + frame_length ] );
                                            meta_info.remaining -= frame_length;
                                            position += frame_length;
                                        } else if meta_info.vec.is_empty() {
                                            // Reading the length of the metadata segment
                                            meta_info.vec.push( data[ position ] );
                                            position += 1;
                                        } else {
                                            // Reading in metadata
                                            let size = 1 + ( ( meta_info.vec[ 0 ] as usize ) << 4 );
                                            let remaining_metadata = std::cmp::min( decode_read - position, size - meta_info.vec.len() );
                                            meta_info.vec.extend_from_slice( &data[ position .. position + remaining_metadata ] );
                                            position += remaining_metadata;

                                            // If it's reached the max size, then copy it over to last_full
                                            if meta_info.vec.len() == size {
                                                meta_info.remaining = meta_info.metaint;
                                                last_full = Some( meta_info.vec.clone() );
                                                meta_info.vec.clear();
                                            }
                                        }
                                    }

                                    // Update the source's metadata
                                    if let Some( metadata_vec ) = last_full {
                                        if !{
                                            let serv_vec = &arc.read().await.metadata_vec;
                                            serv_vec.len() == metadata_vec.len() && serv_vec.iter().eq( metadata_vec.iter() )
                                        } {
                                            if metadata_vec[ .. ] == [ 1; 0 ] {
                                                let mut serv = arc.write().await;
                                                println!( "Updated relay {} metadata with no title and url", serv.mountpoint );
                                                serv.metadata_vec = vec![ 0 ];
                                                serv.metadata = None;
                                            } else {
                                                let cut = {
                                                    let mut last = metadata_vec.len();
                                                    while metadata_vec[ last - 1 ] == 0 {
                                                        last -= 1;
                                                    }
                                                    last
                                                };
                                                if let Ok( meta_str ) = std::str::from_utf8( &metadata_vec[ 1 .. cut ] ) {
                                                    let reg = Regex::new( r"^StreamTitle='(.*?)';StreamUrl='(.*?)';$" ).unwrap();
                                                    if let Some( captures ) = reg.captures( meta_str ) {
                                                        let metadata = IcyMetadata {
                                                            title: {
                                                                let m_str = captures.get( 1 ).unwrap().as_str();
                                                                if m_str.is_empty() {
                                                                    None
                                                                } else {
                                                                    Some( m_str.to_string() )
                                                                }
                                                            },
                                                            url: {
                                                                let m_str = captures.get( 2 ).unwrap().as_str();
                                                                if m_str.is_empty() {
                                                                    None
                                                                } else {
                                                                    Some( m_str.to_string() )
                                                                }
                                                            }
                                                        };

                                                        let mut serv = arc.write().await;
                                                        println!( "Updated relay {} metadata with title '{}' and url '{}'", serv.mountpoint, metadata.title.as_ref().unwrap_or( &"".to_string() ), metadata.url.as_ref().unwrap_or( &"".to_string() ) );
                                                        serv.metadata_vec = metadata_vec;
                                                        serv.metadata = Some( metadata );
                                                    } else {
                                                        println!( "Unknown metadata format received from relay {}: `{}`", arc.read().await.mountpoint, meta_str );
                                                        arc.write().await.disconnect_flag = true;
                                                    }
                                                } else {
                                                    println!( "Invalid metadata parsed from relay {}", arc.read().await.mountpoint );
                                                    arc.write().await.disconnect_flag = true;
                                                }
                                            }
                                        }
                                    }

                                    trimmed
                                } else {
                                    data
                                }
                            };

                            if !data.is_empty() {
                                arc.read().await.stats.write().await.bytes_read += data.len();
                                broadcast_to_clients( &arc, data, queue_size, burst_size ).await;
                            }
                        }

                        // Check if the source needs to be disconnected
                        read != 0 && !decoder.is_finished() && !arc.read().await.disconnect_flag
                    }
                    Err( e ) => {
                        println!( "An error occurred while decoding stream data from relay {}: {}", arc.read().await.mountpoint, e );
                        false
                    }
                }
            }  {}
        }

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
        serv.relay_count -= 1;
        serv.stats.session_bytes_read += source.stats.read().await.bytes_read;

        println!( "Unmounted relay {}", source.mountpoint );

        Ok( () )
    }
}

async fn slave_node( server: Arc< RwLock< Server > >, master_server: MasterServer ) {
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
            Err( e ) => println!( "Error while fetching mountpoints from {}: {}", master_server.url, e )
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

            {
                let serv = server.read().await;
                // Check relay limit and if the source already exists
                if serv.relay_count >= master_server.relay_limit ||
                    serv.sources.len() >= serv.properties.limits.total_sources ||
                    server.read().await.sources.contains_key( &path ) {
                    continue;
                }
            }

            // trying to mount all mounts from master
            let server_clone = server.clone();
            let master_clone = master_server.clone();
            tokio::spawn( async move {
                let url = master_clone.url.clone();
                if let Err( e ) = relay_mountpoint( server_clone, master_clone, path.clone() ).await {
                    println!( "An error occurred while relaying {} from {}: {}", path, url, e );
                }
            } );
        }

        // update interval
        tokio::time::sleep( tokio::time::Duration::from_secs( master_server.update_interval ) ).await;
    }
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
    // Check if we need to send the metadata
    if new_sent > metalen {
        // Create a new vector to hold our data + metadata
        let mut inserted: Vec< u8 > = Vec::new();
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
        stream.write_all( &inserted ).await
    } else {
        // Copy over the new amount
        *sent_count = new_sent;
        stream.write_all( data ).await
    }

}

async fn send_listener_ok( stream: &mut TcpStream, id: &str, properties: &IcyProperties, meta_enabled: bool, metaint: usize ) -> Result< (), Box< dyn Error > > {
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

async fn send_not_found( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

async fn send_ok( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    if let Some( ( content_type, text ) ) = message {
        stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
        stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
    }
    server_info(stream).await?;
    if let Some( ( _, text ) ) = message {
        stream.write_all( text.as_bytes() ).await?;
    }

    Ok( () )
}

async fn send_continue( stream: &mut TcpStream, id: &str ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    server_info(stream).await?;
    Ok( () )
}

async fn send_internal_error( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 500 Internal Server Error\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    if let Some( ( content_type, text ) ) = message {
        stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
        stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
    }
    server_info(stream).await?;
    if let Some( ( _, text ) ) = message {
        stream.write_all( text.as_bytes() ).await?;
    }

    Ok( () )
}

async fn send_bad_request( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 400 Bad Request\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    if let Some( ( content_type, text ) ) = message {
        stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
        stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
    }
    server_info(stream).await?;
    if let Some( ( _, text ) ) = message {
        stream.write_all( text.as_bytes() ).await?;
    }

    Ok( () )
}

async fn send_forbidden( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 403 Forbidden\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    if let Some( ( content_type, text ) ) = message {
        stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
        stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
    }
    server_info(stream).await?;
    if let Some( ( _, text ) ) = message {
        stream.write_all( text.as_bytes() ).await?;
    }

    Ok( () )
}

async fn send_unauthorized( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
    stream.write_all( b"HTTP/1.0 401 Authorization Required\r\n" ).await?;
    stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
    stream.write_all( b"Connection: Close\r\n" ).await?;
    if let Some( ( content_type, text ) ) = message {
        stream.write_all( ( format!( "Content-Type: {}\r\n", content_type ) ).as_bytes() ).await?;
        stream.write_all( ( format!( "Content-Length: {}\r\n", text.len() ) ).as_bytes() ).await?;
    }
    stream.write_all( b"WWW-Authenticate: Basic realm=\"Icy Server\"\r\n" ).await?;
    server_info(stream).await?;
    if let Some( ( _, text ) ) = message {
        stream.write_all( text.as_bytes() ).await?;
    }

    Ok( () )
}

async fn server_info( stream: &mut TcpStream )  -> Result< (), Box< dyn Error > > {
    stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
    stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
    stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
    stream.write_all( b"Pragma: no-cache\r\n" ).await?;
    stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;

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

fn get_header< 'a >( key: &str, headers: &[ httparse::Header< 'a > ] ) -> Option< &'a [ u8 ] > {
    let key = key.to_lowercase();
    for header in headers {
        if header.name.to_lowercase() == key {
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
fn default_property_address() -> String { ADDRESS.to_string() }
fn default_property_port() -> u16 { PORT }
fn default_property_metaint() -> usize { METAINT }
fn default_property_server_id() -> String { SERVER_ID.to_string() }
fn default_property_admin() -> String { ADMIN.to_string() }
fn default_property_host() -> String { HOST.to_string() }
fn default_property_location() -> String { LOCATION.to_string() }
fn default_property_description() -> String { DESCRIPTION.to_string() }
fn default_property_users() -> Vec< Credential > { vec![ Credential{ username: "admin".to_string(), password: "hackme".to_string() }, Credential { username: "source".to_string(), password: "hackme".to_string() } ] }
fn default_property_limits() -> ServerLimits { ServerLimits {
    clients: default_property_limits_clients(),
    sources: default_property_limits_sources(),
    total_sources: default_property_limits_total_sources(),
    queue_size: default_property_limits_queue_size(),
    burst_size: default_property_limits_burst_size(),
    header_timeout: default_property_limits_header_timeout(),
    source_timeout: default_property_limits_source_timeout(),
    source_limits: default_property_limits_source_limits(),
    http_max_length: default_property_limits_http_max_length(),
    http_max_redirects: default_property_limits_http_max_redirects()
} }
fn default_property_limits_clients() -> usize { CLIENTS }
fn default_property_limits_sources() -> usize { SOURCES }
fn default_property_limits_total_sources() -> usize { MAX_SOURCES }
fn default_property_limits_queue_size() -> usize { QUEUE_SIZE }
fn default_property_limits_burst_size() -> usize { BURST_SIZE }
fn default_property_limits_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_property_limits_source_timeout() -> u64 { SOURCE_TIMEOUT }
fn default_property_limits_http_max_length() -> usize { HTTP_MAX_LENGTH }
fn default_property_limits_http_max_redirects() -> usize { HTTP_MAX_REDIRECTS }
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
fn default_property_master_server() -> MasterServer { MasterServer {
    enabled: default_property_master_server_enabled(),
    url: default_property_master_server_url(),
    update_interval: default_property_master_server_update_interval(),
    relay_limit: default_property_master_server_relay_limit()
} }
fn default_property_master_server_enabled() -> bool { false }
fn default_property_master_server_url() -> String { format!( "http://localhost:{}", default_property_port() + 1 ) }
fn default_property_master_server_update_interval() -> u64 { 120 }
fn default_property_master_server_relay_limit() -> usize { SOURCES }

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
                Err( e ) => println!( "An error occurred while parsing the config: {}", e ),
            }
        }
        Err( e ) if e.kind() == std::io::ErrorKind::NotFound => {
            println!( "The config file was not found! Attempting to save to file" );
        }
        Err( e ) => println!( "An error occurred while trying to read the config: {}", e ),
    }

    // Create or update the current config
    match File::create( &config_location ) {
        Ok( file ) => {
            match serde_json::to_string_pretty( &properties ) {
                Ok( config ) => {
                    let mut writer = BufWriter::new( file );
                    if let Err( e ) = writer.write_all( config.as_bytes() ) {
                        println!( "An error occurred while writing to the config file: {}", e );
                    }
                }
                Err( e ) => println!( "An error occurred while trying to serialize the server properties: {}", e ),
            }
        }
        Err( e ) => println!( "An error occurred while to create the config file: {}", e ),
    }

    println!( "Using ADDRESS            : {}", properties.address );
    println!( "Using PORT               : {}", properties.port );
    println!( "Using METAINT            : {}", properties.metaint );
    println!( "Using SERVER ID          : {}", properties.server_id );
    println!( "Using ADMIN              : {}", properties.admin );
    println!( "Using HOST               : {}", properties.host );
    println!( "Using LOCATION           : {}", properties.location );
    println!( "Using DESCRIPTION        : {}", properties.description );
    println!( "Using CLIENT LIMIT       : {}", properties.limits.clients );
    println!( "Using SOURCE LIMIT       : {}", properties.limits.sources );
    println!( "Using QUEUE SIZE         : {}", properties.limits.queue_size );
    println!( "Using BURST SIZE         : {}", properties.limits.burst_size );
    println!( "Using HEADER TIMEOUT     : {}", properties.limits.header_timeout );
    println!( "Using SOURCE TIMEOUT     : {}", properties.limits.source_timeout );
    println!( "Using HTTP MAX LENGTH    : {}", properties.limits.http_max_length );
    println!( "Using HTTP MAX REDIRECTS : {}", properties.limits.http_max_length );
    if properties.master_server.enabled {
        println!( "Using a master server:" );
        println!( "      URL                : {}", properties.master_server.url );
        println!( "      UPDATE INTERVAL    : {} seconds", properties.master_server.update_interval );
        println!( "      RELAY LIMIT        : {}", properties.master_server.relay_limit );
    }
    for ( mount, limit ) in &properties.limits.source_limits {
        println!( "Using limits for {}:", mount );
        println!( "      CLIENT LIMIT       : {}", limit.clients );
        println!( "      SOURCE TIMEOUT     : {}", limit.source_timeout );
        println!( "      BURST SIZE         : {}", limit.burst_size );
    }

    if properties.users.is_empty() {
        println!( "At least one user must be configured in the config!" );
    } else {
        println!( "{} users registered", properties.users.len() );
        match format!( "{}:{}", properties.address, properties.port ).parse::< SocketAddr >() {
            Ok( address ) => {
                println!( "Attempting to bind to {}:{}", properties.address, properties.port );
                match TcpListener::bind( address ).await {
                    Ok( listener ) => {
                        let server = Arc::new( RwLock::new( Server::new( properties ) ) );

                        if let Ok( time ) = SystemTime::now().duration_since( UNIX_EPOCH ) {
                            println!( "The server has started on {}", fmt_http_date( SystemTime::now() ) );
                            server.write().await.stats.start_time = time.as_secs();
                        } else {
                            println!( "Unable to capture when the server started!" );
                        }

                        let master_server = server.read().await.properties.master_server.clone();
                        if master_server.enabled {
                            // Start our slave node
                            let server_clone = server.clone();
                            tokio::spawn( async move {
                                slave_node( server_clone, master_server ).await;
                            } );
                        }

                        println!( "Listening..." );
                        loop {
                            match listener.accept().await {
                                Ok( ( socket, addr ) ) => {
                                    let server_clone = server.clone();

                                    tokio::spawn( async move {
                                        if let Err( e ) = handle_connection( server_clone, socket ).await {
                                            println!( "An error occurred while handling a connection from {}: {}", addr, e );
                                        }
                                    } );
                                }
                                Err( e ) => println!( "An error occurred while accepting a connection: {}", e )
                            }
                        }
                    }
                    Err( e ) => println!( "Unable to bind to port: {}", e )
                }
            }
            Err( e ) => println!( "Could not parse the address: {}", e )
        }
    }
}
