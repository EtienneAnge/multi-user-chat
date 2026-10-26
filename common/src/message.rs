
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use jiff::Zoned;
use std::fmt;
use sha2::{Digest, Sha256};


pub const GENESIS_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

    
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub pseudo: String,
    pub time: Zoned,
    pub content: String,
    
    pub prev_hash: String,


}


impl Message {
    pub fn new(content: String, prev_hash: String, pseudo: String)-> Self{
       Message {
        id: Uuid::new_v4(),
        time: Zoned::now(),
        content: content,
        prev_hash: prev_hash,
        pseudo: pseudo,

       }
    }

    pub fn compute_hash(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("Message is always serializable");
        hex::encode(Sha256::digest(&bytes))
    
    }
}



impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.pseudo, self.content)
    }
}