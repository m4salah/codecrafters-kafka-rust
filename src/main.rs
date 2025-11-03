use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        let mut buffer = [0u8; 1024];
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let bytes_read = stream.read(&mut buffer)?;
                // ensure that the size of the request at least 10 bytes long
                assert!(bytes_read >= 12);
                let message = &buffer[0..bytes_read];
                let _message_size = &message[0..4];
                let _request_api_key = &message[4..6];
                let _request_api_version = &message[6..8];
                let correlation_id = &message[8..12];

                let mut response: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00];
                response.extend_from_slice(correlation_id);

                println!("{:#?}", response);
                stream.write_all(&response)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
