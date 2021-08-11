use std::fs::File;
use std::io::{ BufWriter, Write };
use serde::{ Deserialize, Serialize };
use serde_json;
use std::collections::HashMap;

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

#[ derive( Serialize, Deserialize, Clone ) ]
pub struct ServerProperties {
	#[ serde( default = "default_property_port" ) ]
	pub port: u16,
	#[ serde( default = "default_property_metaint" ) ]
	pub metaint: usize,
	#[ serde( default = "default_property_server_id" ) ]
	pub server_id: String,
	#[ serde( default = "default_property_admin" ) ]
	pub admin: String,
	#[ serde( default = "default_property_host" ) ]
	pub host: String,
	#[ serde( default = "default_property_location" ) ]
	pub location: String,
	#[ serde( default = "default_property_description" ) ]
	pub description: String,
	#[ serde( default = "default_property_limits" ) ]
	pub limits: ServerLimits,
	#[ serde( default = "default_property_users" ) ]
	pub users: Vec< Credential >,
	#[ serde( default = "default_property_master_server" ) ]
	pub master_server: MasterServer
}

impl ServerProperties {
	pub fn new() -> ServerProperties {
		ServerProperties {
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

// TODO Add permissions, specifically source, admin, bypass client count, etc
#[ derive( Serialize, Deserialize, Clone ) ]
pub struct Credential {
	pub username: String,
	pub password: String
}

// Add a total source limit
#[ derive( Serialize, Deserialize, Clone ) ]
pub struct ServerLimits {
	#[ serde( default = "default_property_limits_clients" ) ]
	pub clients: usize,
	#[ serde( default = "default_property_limits_sources" ) ]
	pub sources: usize,
	#[ serde( default = "default_property_limits_total_sources" ) ]
	pub total_sources: usize,
	#[ serde( default = "default_property_limits_queue_size" ) ]
	pub queue_size: usize,
	#[ serde( default = "default_property_limits_burst_size" ) ]
	pub burst_size: usize,
	#[ serde( default = "default_property_limits_header_timeout" ) ]
	pub header_timeout: u64,
	#[ serde( default = "default_property_limits_source_timeout" ) ]
	pub source_timeout: u64,
    #[ serde( default = "default_property_limits_http_max_length" ) ]
	pub http_max_length: usize,
	#[ serde( default = "default_property_limits_source_limits" ) ]
	pub source_limits: HashMap< String, SourceLimits >
}

#[ derive( Serialize, Deserialize, Clone ) ]
pub struct SourceLimits {
	#[ serde( default = "default_property_limits_clients" ) ]
	pub clients: usize,
	#[ serde( default = "default_property_limits_burst_size" ) ]
	pub burst_size: usize,
	#[ serde( default = "default_property_limits_source_timeout" ) ]
	pub source_timeout: u64
}

// TODO Add a list of "relay" structs
// Relay structs should have an auth of their own, if provided
// TODO Add basic authorization
#[ derive( Serialize, Deserialize, Clone ) ]
pub struct MasterServer {
	#[ serde( default = "default_property_master_server_enabled" ) ]
	pub enabled: bool,
	#[ serde( default = "default_property_master_server_host" ) ]
	pub host: String,
	#[ serde( default = "default_property_master_server_port" ) ]
	pub port: u16,
	#[ serde( default = "default_property_master_server_update_interval" ) ]
	pub update_interval: u64,
	#[ serde( default = "default_property_master_server_relay_limit" ) ]
	pub relay_limit: usize,
}

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
	http_max_length: default_property_limits_http_max_length()
} }
fn default_property_limits_clients() -> usize { CLIENTS }
fn default_property_limits_sources() -> usize { SOURCES }
fn default_property_limits_total_sources() -> usize { MAX_SOURCES }
fn default_property_limits_queue_size() -> usize { QUEUE_SIZE }
fn default_property_limits_burst_size() -> usize { BURST_SIZE }
fn default_property_limits_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_property_limits_source_timeout() -> u64 { SOURCE_TIMEOUT }
fn default_property_limits_http_max_length() -> usize { HTTP_MAX_LENGTH }
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
	host: default_property_master_server_host(),
	port: default_property_master_server_port(),
	update_interval: default_property_master_server_update_interval(),
	relay_limit: default_property_master_server_relay_limit()
} }
fn default_property_master_server_enabled() -> bool { false }
fn default_property_master_server_host() -> String { "localhost".to_string() }
fn default_property_master_server_port() -> u16 { default_property_port() + 1 }
fn default_property_master_server_update_interval() -> u64 { 120 }
fn default_property_master_server_relay_limit() -> usize { SOURCES }

pub fn load(properties: &mut ServerProperties) {
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
				Ok( prop ) => *properties = prop,
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
}
