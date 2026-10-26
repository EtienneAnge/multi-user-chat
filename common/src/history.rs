use std::{eprintln, hash::{DefaultHasher, Hash, Hasher}};

use tokio::sync::{broadcast, mpsc, oneshot};
use crate::{message::Message};


# [derive(Clone)]
pub struct HistoryHandle {
    tx: mpsc::Sender<HistoryCommand>,
}

enum HistoryCommand {
    NewHistory(oneshot::Sender<Result<(),()>>, Vec<Message>),
    Validate(oneshot::Sender<Result<Message,()>>, Message),
    GetHistory(oneshot::Sender<Vec<Message>>),
}

impl HistoryHandle {
    /// Crée l'acteur et lance sa tâche
    pub fn new(tx_send: broadcast::Sender<Message>) -> Self {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(history_actor(rx, tx_send));
        Self { tx }
    }

    pub async fn validate(&self, msg: Message) -> Result<Message, ()> {
        let (reply_tx, reply_rx) = oneshot::channel::<Result<Message, ()>>();
        self.tx
        .send(HistoryCommand::Validate(reply_tx, msg))
        .await
        .map_err(|_| ())?;
        reply_rx.await.unwrap_or(Err(()))
    }

    pub async fn get_history(&self) -> Vec<Message> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.tx.send(HistoryCommand::GetHistory(reply_tx)).await;
        reply_rx.await.unwrap_or_default()
    }

    pub async fn new_history(&self, new_history: Vec<Message>) -> Result<(),()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
        .send(HistoryCommand::NewHistory(reply_tx, new_history))
        .await
        .map_err(|_| ())?;
        reply_rx.await.unwrap_or(Err(()))   
    }

    pub async fn prev_hash(&self) -> u64{
        match self.get_history().await.last(){
            None => calculate_hash(&0),
            Some(s) => calculate_hash(s),
        }
    }

    pub async fn print(&self){
        self.get_history().await.iter().for_each(|msg| println!("{msg}"));

        
    }


}





async fn history_actor(
    mut rx: mpsc::Receiver<HistoryCommand>,
    tx_send: broadcast::Sender<Message>,
) {
    let mut historic = Vec::new();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            HistoryCommand::Validate(reply, msg) => {
                let prev_hash = match historic.last() {
                    None => calculate_hash(&0),
                    Some(s) => calculate_hash(s),
                };
                
                if msg.prev_hash == prev_hash {
                    historic.push(msg.clone());
                    _ =  tx_send.send(msg.clone());
                    
                    let _ = reply.send (Ok(msg));
                }else{
                    eprintln!("prev_hash is not valid");
                    let _ = reply.send (Err(()));
                }
             },
             HistoryCommand::NewHistory(reply, new_history) => {
                
                historic = new_history;
                let _ = reply.send (Ok(()));
             },
            HistoryCommand::GetHistory(reply) => { let _ = reply.send(historic.clone()); },
        }
    }
}


fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}