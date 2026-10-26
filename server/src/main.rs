use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use std::error::Error;

use tokio::sync::broadcast;
use tokio::sync::mpsc;


use std::{io, println};
use common::message::Message;
use common::history::HistoryHandle;

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
    let listener = TcpListener::bind("127.0.0.1:9090").await?;
    let (tx_send_to_everyone, _ ) = broadcast::channel::<Message>(16);
    
    let history = HistoryHandle::new(tx_send_to_everyone.clone());
    
   

    loop {
        let (socket, _) = listener.accept().await?;
        let history_clone = history.clone();

        println!("connection done");
    
        let (reader,  writer) = socket.into_split();

        let  rx_send_to_everyone = tx_send_to_everyone.subscribe();

        tokio::spawn(async move {
            let (tx_reply,  rx_reply) = mpsc::channel::<ServerRequest>(32);
            let write_handle = tokio::spawn({
                
                write_task( writer, rx_send_to_everyone, rx_reply)
            });
            let _ = read_task(reader, history_clone, tx_reply).await;
            write_handle.abort();
            println!("client disconnected");
        });
        
    }
}





pub async fn read_task(reader: OwnedReadHalf, history: HistoryHandle, tx_reply: mpsc::Sender<ServerRequest>)
    -> Result<(), Box<dyn Error + Send + Sync>>
 {

    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let request: ClientRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error request : {e}");
                continue;
            },
        };

        match request {
            ClientRequest::SendMessage ( msg ) => {
        
                

                match history.validate(msg.clone()).await {
                    Ok(_) => {println!("Received: {msg} \x1B[32m[Message integrity verified]\x1B[0m");},
                    Err(common::history::HistoryError::InvalidPrevHash) => {
                        println!("Received: {msg} \x1B[31m[Message invalid]\x1B[0m");
                        continue;
                    },
                    Err(e) => return Err(e.into()),
                }
            },
            ClientRequest::GetHistory => {
                
                let _ = tx_reply.send(ServerRequest::History(history.get_history().await)).await;
                println!("History has been sent")
            }

        }
    }
    println!("server closed");

    Ok(())
}

pub async fn write_task(mut writer: OwnedWriteHalf, mut rx_send_to_everyone: broadcast::Receiver<Message>, mut rx_reply: mpsc::Receiver<ServerRequest>)
    -> Result<(), Box<dyn Error + Send + Sync>>
{
    loop {
        let req = tokio::select! {
            Ok(msg) = rx_send_to_everyone.recv() => ServerRequest::NewMessage(msg),
            Some(req) = rx_reply.recv() => req,
        };

        let json = match serde_json::to_string(&req) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Erreur de sérialisation : {e}");
                continue;
            }
        };

        if writer.write_all(format!("{json}\n").as_bytes()).await.is_err() {
            break;
        }
    }
    
        


    Ok(())
}




