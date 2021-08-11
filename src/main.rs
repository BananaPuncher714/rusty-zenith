mod config;
mod server;
mod response;
mod relay;
mod stream;
mod icecast;

#[ tokio::main ]
async fn main() {
	// TODO Log everything somehow or something
	let mut properties = config::ServerProperties::new();

	config::load(&mut properties);

	println!( "Using PORT            : {}", properties.port );
	println!( "Using METAINT         : {}", properties.metaint );
	println!( "Using SERVER ID       : {}", properties.server_id );
	println!( "Using ADMIN           : {}", properties.admin );
	println!( "Using HOST            : {}", properties.host );
	println!( "Using LOCATION        : {}", properties.location );
	println!( "Using DESCRIPTION     : {}", properties.description );
	println!( "Using CLIENT LIMIT    : {}", properties.limits.clients );
	println!( "Using SOURCE LIMIT    : {}", properties.limits.sources );
	println!( "Using QUEUE SIZE      : {}", properties.limits.queue_size );
	println!( "Using BURST SIZE      : {}", properties.limits.burst_size );
	println!( "Using HEADER TIMEOUT  : {}", properties.limits.header_timeout );
	println!( "Using SOURCE TIMEOUT  : {}", properties.limits.source_timeout );
	println!( "Using HTTP MAX LENGTH : {}", properties.limits.http_max_length );
	if properties.master_server.enabled {
		println!( "Using a master server:" );
		println!( "      HOST            : {}", &properties.master_server.host );
		println!( "      PORT            : {}", &properties.master_server.port );
		println!( "      UPDATE INTERVAL : {} seconds", &properties.master_server.update_interval );
		println!( "      RELAY LIMIT     : {}", &properties.master_server.relay_limit );
	}
	for ( mount, limit ) in &properties.limits.source_limits {
		println!( "Using limits for {}:", mount );
		println!( "      CLIENT LIMIT    : {}", limit.clients );
		println!( "      SOURCE TIMEOUT  : {}", limit.source_timeout );
		println!( "      BURST SIZE      : {}", limit.burst_size );
	}

	if properties.users.is_empty() {
		println!( "At least one user must be configured in the config!" );
	} else {
		println!( "{} users registered", properties.users.len() );
		println!( "Attempting to bind to port {}", properties.port );

		server::listener(properties).await;
	}
}
