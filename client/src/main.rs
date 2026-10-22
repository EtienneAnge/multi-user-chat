use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::error::Error;
use std::os::unix::net::SocketAddr;
use tokio::sync::mpsc;
use std::io;
use serde::{Serialize, Deserialize};
use std::hash::{DefaultHasher, Hash, Hasher};
use uuid::Uuid;
use jiff::Zoned;

use common::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let historic: Vec<Message>= Vec::new();

    // Connect to a peer
    let mut stream = TcpStream::connect("127.0.0.1:9090").await?;



    let (reader, mut writer) = stream.into_split();

    let (tx, mut rx) = mpsc::channel::<String>(32);
    
    let read_task = tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            println!("Received : {line}");
        }
        println!("server closed");
    });
    
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if writer.write_all(format!("{msg}\n").as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let mut pseudo = String::new();
    io::stdin().read_line(&mut pseudo).expect("Reading error");
    let pseudo: String = String::from(pseudo.trim()); 
    
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Reading error");
        let input: String = String::from(input.trim()); 

        let msg: Message = Message::new(
            input, 
            calculate_hash(&historic.last()),
            String::from(pseudo.clone())
        );
        let msg = serde_json::to_string(&msg)?;
        tx.send(msg).await?;
    }


    let _ = tokio::join!(read_task, write_task);
    Ok(())
}



fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

