use std::net::{ SocketAddr, IpAddr, Ipv4Addr };
use tokio::net::{ TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use std::sync::Arc;
use tokio::time::timeout;
use std::collections::HashMap;
use uuid::Uuid;
use serde::{ Serialize, Deserialize };
use std::time::{ Duration, SystemTime, UNIX_EPOCH };
use httpdate::fmt_http_date;
use regex::Regex;
use std::error::Error;
use httparse::Status;
use std::io::ErrorKind;
use std::path::Path;
use tokio::sync::mpsc::unbounded_channel;
use serde_json::{ json, Value };

use crate::icecast::{ Source, IcyProperties, IcyMetadata, Client, populate_properties, mount_source, get_metadata_vec };
use crate::config::ServerProperties;
use crate::relay::slave_node;
use crate::response::{
    send_ok,
    send_continue,
    send_forbidden,
    send_not_found,
    send_listener_ok,
    send_bad_request,
    send_unauthorized,
    send_internal_error
};
use crate::stream::{
    write_to_client,
    read_http_message
};

pub struct Server {
	pub sources: HashMap< String, Arc< RwLock< Source > > >,
	pub clients: HashMap< Uuid, ClientProperties >,
	// TODO Find a better place to put these, for constant time fetching
	pub source_count: usize,
	pub relay_count: usize,
	pub properties: ServerProperties,
	pub stats: ServerStats
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

#[ derive( Serialize, Deserialize, Clone ) ]
pub struct ServerStats {
	pub start_time: u64,
	pub peak_listeners: usize,
	pub session_bytes_sent: usize,
	pub session_bytes_read: usize,
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

#[ derive( Serialize, Clone ) ]
pub struct ClientProperties {
	id: Uuid,
	uagent: Option< String >,
	metadata: bool
}

#[ derive( Serialize, Clone ) ]
pub struct ClientStats {
	start_time: u64,
	bytes_sent: usize
}

#[ derive( Clone ) ]
struct Query {
	field: String,
	value: String
}

async fn handle_connection( server: Arc< RwLock< Server > >, mut stream: TcpStream ) -> Result< (), Box< dyn Error > > {
	let mut message = Vec::new();
    let ( server_id, header_timeout, http_max_len ) = {
        let properties = &server.read().await.properties;
        ( properties.server_id.clone(), properties.limits.header_timeout, properties.limits.http_max_length )
    };

	// Add a timeout
	timeout( Duration::from_millis( header_timeout ), read_http_message( &mut stream, &mut message, http_max_len ) ).await??;

	let mut _headers = [ httparse::EMPTY_HEADER; 32 ];
	let mut req = httparse::Request::new( &mut _headers );
    if req.parse( &message )? == Status::Partial {
        return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Received an incomplete request" ) ) );
	}

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
					send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=urf-8", "Invalid credentials" ) ) ).await?;
					return Ok( () )
				}
			} else {
				// No auth, return and close
				send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=urf-8", "You need to authenticate" ) ) ).await?;
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
				send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}

			// Check if it is valid
			// For now this assumes the stream directory is /
			let dir = Path::new( &path );
			if let Some( parent ) = dir.parent() {
				if let Some( parent_str ) = parent.to_str() {
					if parent_str != "/" {
						send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
						return Ok( () )
					}
				} else {
					send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
					return Ok( () )
				}
			} else {
				send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}

			// Sources must have a content type
			// Maybe the type that is served should be checked?
			let mut properties = match get_header( "Content-Type", headers ) {
				Some( content_type ) => IcyProperties::new( std::str::from_utf8( content_type )?.to_string() ),
				None => {
					send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "No Content-type provided" ) ) ).await?;
					return Ok( () )
				}
			};

			let mut serv = server.write().await;
			// Check if the mountpoint is already in use
			if serv.sources.contains_key( &path ) {
				send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid mountpoint" ) ) ).await?;
				return Ok( () )
			}

			// Check if the max number of sources has been reached
			if serv.source_count >= serv.properties.limits.sources || serv.sources.len() >= serv.properties.limits.total_sources {
				send_forbidden( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Too many sources connected" ) ) ).await?;
				return Ok( () )
			}

			if method == "SOURCE" {
				// Give an 200 OK response
				send_ok( &mut stream, &server_id, None ).await?;
			} else {
				// Verify that the transfer encoding is identity or not included
				// No support for chunked or encoding ATM
				// TODO Add support for transfer encoding options as specified here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
				// TODO Also potentially add support for content length? Not sure if that's a particularly large priority
				match get_header( "Transfer-Encoding", headers ) {
					Some( v ) if v != b"identity" => {
						send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Unsupported transfer encoding" ) ) ).await?;
						return Ok( () )
					}
					_ => ()
				}

				// Check if client sent Expect: 100-continue in header, if that's the case we will need to return 100 in status code
				// Without it, it means that client has no body to send, we will stop if that's the case
				match get_header( "Expect", headers ) {
					Some( b"100-continue" ) => {
						send_continue( &mut stream, &server_id ).await?;
					}
					Some( _ ) => {
						send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Expected 100-continue in Expect header" ) ) ).await?;
						return Ok( () )
					}
					None => {
						send_bad_request( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "PUT request must come with Expect header" ) ) ).await?;
						return Ok( () )
					}
				}
			}

			// Parse the headers for the source properties
			populate_properties( &mut properties, headers );

			let source = Source::new( path.clone(), properties );

			// Add to the server
			let arc = Arc::new( RwLock::new( source ) );
			serv.sources.insert( path, arc.clone() );
			serv.source_count += 1;
			drop( serv );

			println!( "Mounted source on {} via {}", arc.read().await.mountpoint, method );

            mount_source(&mut stream, &arc, &server).await;

            let mut serv = server.write().await;
            serv.source_count -= 1;

			if method == "PUT" {
				// request must end with server 200 OK response
				send_ok( &mut stream, &server_id, None ).await.ok();
			}

			println!( "Unmounted source {}", arc.read().await.mountpoint );
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
							return Ok( () )
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
								send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "Invalid credentials" ) ) ).await?;
								return Ok( () )
							}
						} else {
							// No auth, return and close
							send_unauthorized( &mut stream, &server_id, Some( ( "text/plain; charset=utf-8", "You need to authenticate" ) ) ).await?;
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

pub fn get_header< 'a >( key: &str, headers: &[ httparse::Header< 'a > ] ) -> Option< &'a [ u8 ] > {
	let key = key.to_lowercase();
	for header in headers {
		if header.name.to_lowercase() == key {
			return Some( header.value )
		}
	}
	None
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

pub async fn listener(properties: ServerProperties) {
    match TcpListener::bind( SocketAddr::new( IpAddr::V4( Ipv4Addr::new( 127, 0, 0, 1 ) ), properties.port ) ).await {
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
