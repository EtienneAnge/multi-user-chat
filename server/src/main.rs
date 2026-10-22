use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use std::error::Error;
use std::io::stderr;
use std::os::unix::raw::off_t;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use std::{io, println};
use common::Message;

use serde::{Serialize, Deserialize};

async fn process_socket(socket: TcpStream, tx: broadcast::Sender<String>) -> Result<(), Box<dyn Error>>  {
    // do work with socket here
    println!("connection done");
    
    let (reader, mut writer) = socket.into_split();

    let mut rx = tx.subscribe();
    
    let read_task = tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            
            let msg: Message = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Invalid message : {e}");
                    continue;
                }
            };
            println!("Received : {msg}");






            let msg = match serde_json::to_string(&msg) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Serialize error : {e}");
                    continue;
                }
            };

            match tx.send(msg) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("Channel error : {e}");
                    continue;
                }

            };
        }
        println!("server closed");
    });
    
    let write_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if writer.write_all(format!("{msg}\n").as_bytes()).await.is_err() {
                break;
            }
        }
    });

    
   
    

    let _ = read_task.await;
    write_task.abort();

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9090").await?;
    let (tx, _ ) = broadcast::channel::<String>(16);
    let historic: Vec<Message>= Vec::new();

    loop {
        let (socket, _) = listener.accept().await?;
        let tx = tx.clone();

        println!("connection done");
    
        let (reader, mut writer) = socket.into_split();

        let mut rx = tx.subscribe();

        tokio::spawn(async move {
            read_task(reader)
        });
        tokio::spawn(async move {
            // Process each socket concurrently.

            process_socket(socket, tx).await;
        });
    }
}

pub async fn read_task(reader: OwnedReadHalf){

        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            
            let msg: Message = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Invalid message : {e}");
                    continue;
                }
            };
            println!("Received : {msg}");






            let msg = match serde_json::to_string(&msg) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Serialize error : {e}");
                    continue;
                }
            };

            match tx.send(msg) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("Channel error : {e}");
                    continue;
                }

            };
        }
        println!("server closed");
}

pub async fn writ_task(){

}

pub async fn validate_message(){

}





