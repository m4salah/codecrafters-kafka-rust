use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn handle_conn(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buffer = [0u8; 1024];
    while let Ok(bytes_read) = stream.read(&mut buffer) {
        // ensure that the size of the request at least 10 bytes long
        assert!(bytes_read >= 12);
        let message = &buffer[0..bytes_read];
        let _message_size = &message[0..4];
        let request_api_key = &message[4..6];
        let request_api_version = u16::from_be_bytes(message[6..8].try_into()?);

        let correlation_id = &message[8..12];

        let mut response: Vec<u8> = vec![];

        // message size
        response.extend_from_slice(&19u32.to_be_bytes());

        // correlation id
        response.extend_from_slice(correlation_id);

        if request_api_version > 4 {
            response.extend_from_slice(&[0x00, 0x23]);
        }

        // wirte the error 0
        response.extend_from_slice(&[0, 0]);

        // api keys
        response.extend_from_slice(&[0x02]);

        // api key
        response.extend_from_slice(request_api_key);

        // min version
        response.extend_from_slice(&[0, 0]);

        // max version
        response.extend_from_slice(&[0, 4]);

        // TAG_BUFFER
        response.extend_from_slice(&[0]);

        // throttle_time_ms
        response.extend_from_slice(&[0, 0, 0, 0]);

        // TAG_BUFFER
        response.extend_from_slice(&[0]);

        stream.write_all(&response)?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| -> anyhow::Result<()> { handle_conn(stream) });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
