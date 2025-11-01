#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

const CORRELATION_ID: u8 = 7u8;
fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // TODO: Uncomment the code below to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                stream.write(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07])?;
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
