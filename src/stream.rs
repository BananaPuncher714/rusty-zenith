use tokio::net::TcpStream;
use std::error::Error;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use std::io::ErrorKind;

pub async fn write_to_client( stream: &mut TcpStream, sent_count: &mut usize, metalen: usize, data: &[ u8 ], metadata: &[ u8 ] )
             -> Result< (), std::io::Error > {
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

pub async fn read_http_message( stream: &mut TcpStream, buf: &mut Vec< u8 >, max_len: usize ) -> Result< (), Box< dyn Error > > {
	let mut byte = [ 0; 1 ];
	while buf.windows( 4 ).last() != Some( b"\r\n\r\n" ) {
		match stream.read( &mut byte ).await {
			Ok( read ) if read > 0 => {
				buf.push( byte[ 0 ] );
				if buf.len() > max_len {
					// Stop any potential attack
					return Err( Box::new( std::io::Error::new( ErrorKind::Other, "Master node sent a long header" ) ) )
				}
			}
			// Cannot be anything besides 0 at this point, but the compiler can't tell the difference
			Ok( _ ) => return Ok( () ),
			Err( e ) => return Err( Box::new( e ) )
		}
	}

	Ok( () )
}
