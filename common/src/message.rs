use serde::{Serialize, Deserialize};
use std::{format, hash::{DefaultHasher, Hash, Hasher}};
use uuid::Uuid;
use jiff::Zoned;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Message {
    id: Uuid,
    pseudo: String,
    time: Zoned,
    content: String,
    
    prev_hash: u64,


}


impl Message {
    pub fn new(content: String, prev_hash: u64, pseudo: String)-> Self{
       Message {
        id: Uuid::new_v4(),
        time: Zoned::now(),
        content: content,
        prev_hash: prev_hash,
        pseudo: pseudo,

       }
    }

    
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.pseudo, self.content)
    }
}