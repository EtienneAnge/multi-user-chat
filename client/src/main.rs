



use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use std::{eprint, eprintln, io, println};
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
async fn main() -> anyhow::Result<()> {
    
    // Connect to a peer
    let socket = TcpStream::connect("127.0.0.1:9090").await?;
    println!("connection done");

    // user input for the pseudo
    println!("Type your psuedo : ");
    let mut pseudo = String::new();
    io::stdin().read_line(&mut pseudo).expect("Reading error");
    let pseudo: String = String::from(pseudo.trim());
    
    let (tx_send_to_everyone, _ ) = broadcast::channel::<Message>(16);
    
    let history = HistoryHandle::new(tx_send_to_everyone);
  
    
    let history_clone = history.clone();
    

    let (reader,  writer) = socket.into_split();
    let (tx_send,  rx_send) = mpsc::channel::<ClientRequest>(32);

    tokio::spawn(async move {
        
        let write_handle = tokio::spawn({
            
            write_task( writer, rx_send)
        });
        let _ = read_task(reader, history_clone).await;
        write_handle.abort();
        println!("client disconnected");
    });

    

    tx_send.send(ClientRequest::GetHistory).await?;
    
    loop {
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Reading error");
        clear_screen();
        history.print().await;
        let input: String = String::from(input.trim()); 

        let msg: Message = Message::new(
            input, 
            history.prev_hash().await,
            String::from(pseudo.clone())
        );
        tx_send.send(ClientRequest::SendMessage(msg)).await?;
    }
        

}





pub async fn read_task(reader: OwnedReadHalf, history: HistoryHandle)
    ->  anyhow::Result<()> 
 {

    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let request: ServerRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error request: {e}");
                continue;
            },
        };

        match request {
            ServerRequest::NewMessage(msg) => {
                
                
                
                match history.validate(msg.clone()).await {
                    Ok(_) => {println!("{msg} \x1B[32m[Message integrity verified]\x1B[0m");},
                    Err(history::HistoryError::InvalidPrevHash) => {
                        println!("{msg} \x1B[31m[Message invalid]\x1B[0m");
                        continue;
                    },
                    Err(e) => return Err(e.into()),
                };
                println!("Type your message : ");
            },
            ServerRequest::History(new_history) => {
                
                match history.new_history(new_history).await {
                    Ok(_) => {
                        clear_screen();
                        println!("New history received");
                        history.print().await;
                    },
                    Err(history::HistoryError::BrokenChain) => {
                        eprintln!("hsitory hash link is broken : history rejected");

                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                };
                println!("Type your message : ");
                
            }
            

        }
    }
    println!("server closed");

    Ok(())
}

pub async fn write_task(mut writer: OwnedWriteHalf,mut rx_send: mpsc::Receiver<ClientRequest>)
    ->  anyhow::Result<()> 
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
    match io::stdout().flush(){
        Err (e) => eprint!("{e}"),
        Ok(_) => {}
    };
}