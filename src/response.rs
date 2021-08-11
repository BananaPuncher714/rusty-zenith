use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use httpdate::fmt_http_date;
use std::time::SystemTime;
use std::error::Error;

use crate::icecast::IcyProperties;

pub async fn send_listener_ok( stream: &mut TcpStream, id: &str, properties: &IcyProperties, meta_enabled: bool, metaint: usize ) -> Result< (), Box< dyn Error > > {
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

pub async fn send_not_found( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 404 File Not Found\r\n" ).await?;
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

pub async fn send_ok( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

pub async fn send_continue( stream: &mut TcpStream, id: &str ) -> Result< (), Box< dyn Error > > {
	stream.write_all( b"HTTP/1.0 200 OK\r\n" ).await?;
	stream.write_all( ( format!( "Server: {}\r\n", id ) ).as_bytes() ).await?;
	stream.write_all( b"Connection: Close\r\n" ).await?;
	server_info(stream).await?;
	Ok( () )
}

pub async fn send_internal_error( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

pub async fn send_bad_request( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

pub async fn send_forbidden( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

pub async fn send_unauthorized( stream: &mut TcpStream, id: &str, message: Option< ( &str, &str ) > ) -> Result< (), Box< dyn Error > > {
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

async fn server_info(stream: &mut TcpStream) -> Result< (), Box< dyn Error > > {
	stream.write_all( ( format!( "Date: {}\r\n", fmt_http_date( SystemTime::now() ) ) ).as_bytes() ).await?;
	stream.write_all( b"Cache-Control: no-cache, no-store\r\n" ).await?;
	stream.write_all( b"Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n" ).await?;
	stream.write_all( b"Pragma: no-cache\r\n" ).await?;
	stream.write_all( b"Access-Control-Allow-Origin: *\r\n\r\n" ).await?;

	Ok( () )
}
