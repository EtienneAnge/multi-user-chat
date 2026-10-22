


use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use std::error::Error;
use std::f64::consts::E;
use std::io::stderr;
use std::os::unix::raw::off_t;
use std::sync::mpsc::Receiver;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use std::hash::{DefaultHasher, Hash, Hasher};

use std::{io, println};
use common::message::Message;
use common::history::{self, HistoryHandle};

use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientRequest {
    SendMessage ( Message),
    GetHistory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ServerRequest {
    NewMessage(Message),
    History(Vec<Message>),
}


#[tokio::main]
async fn main() -> io::Result<()> {
    
    // Connect to a peer
    let mut socket = TcpStream::connect("127.0.0.1:9090").await?;
    println!("connection done");
    let mut pseudo = String::new();
    io::stdin().read_line(&mut pseudo).expect("Reading error");
    let pseudo: String = String::from(pseudo.trim());
    
    let (tx_send_to_everyone, _ ) = broadcast::channel::<Message>(16);
    
    let history = HistoryHandle::new(tx_send_to_everyone);
  
    
    let history_clone = history.clone();
    

    let (reader, mut writer) = socket.into_split();
    let (tx_send, mut rx_send) = mpsc::channel::<ClientRequest>(32);

    tokio::spawn(async move {
        
        let write_handle = tokio::spawn({
            
            write_task( writer, rx_send)
        });
        let _ = read_task(reader, history_clone).await;
        write_handle.abort();
        println!("client disconnected");
    });

    

    tx_send.send(ClientRequest::GetHistory).await;
    
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Reading error");
        let input: String = String::from(input.trim()); 

        let msg: Message = Message::new(
            input, 
            history.prev_hash().await,
            String::from(pseudo.clone())
        );
        tx_send.send(ClientRequest::SendMessage(msg)).await;
    }
        

}





pub async fn read_task(reader: OwnedReadHalf, history: HistoryHandle)
    -> Result<(), Box<dyn Error + Send + Sync>>
 {

    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let request: ServerRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error request");
                continue;
            },
        };

        match request {
            ServerRequest::NewMessage(msg) => {
                println!("Received : {msg}");
                
                
                match history.validate(msg).await {
                    Ok(o) => {println!("Message integrity verified");},
                    Err(e) => {
                        println!("Message invalid");
                        continue;
                    },
                }
            },
            ServerRequest::History(new_history) => {
                println!("New history received");
                history.new_history(new_history).await;
                clear_screen();
                history.print().await;
            }
            

        }
    }
    println!("server closed");

    Ok(())
}

pub async fn write_task(mut writer: OwnedWriteHalf,mut rx_send: mpsc::Receiver<ClientRequest>)
    -> Result<(), Box<dyn Error + Send + Sync>>
{
    while let Some(req) = rx_send.recv().await{

        let json = match serde_json::to_string(&req) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("serialization error : {e}");
                continue;
            }
        };

        if writer.write_all(format!("{json}\n").as_bytes()).await.is_err() {
            break;
        }
    }
    
        


    Ok(())
}




use std::io::{Write};

fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    io::stdout().flush().unwrap();
}