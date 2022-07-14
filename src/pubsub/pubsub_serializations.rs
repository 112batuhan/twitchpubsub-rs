#![allow(dead_code)]
use serde::{Serialize, Deserialize, Deserializer, de};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum Request {
    PING,
    PONG,
    RECONNECT,
    RESPONSE{nonce:String, error:String},
    LISTEN{nonce:String, data:Listen},
    UNLISTEN{nonce:String, data:Listen},
    MESSAGE{data:MessageData},
    //CLOSE added to implement a higher level gracefull shutdown close message for the client
    //client sends messages between threads as request,
    //to send a low level close message, either the whole structure must be changed
    //or a high level enum variant.
    //this is kind of weird but who cares
    CLOSE, 
}
impl Request {
    pub fn unwrap_response(self) -> Option<(String,String)>{
        match self{
            Request::RESPONSE { nonce, error } => {Some((nonce, error))},
            _=>{None},
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Listen{
    pub topics: Vec<String>,
    pub auth_token: String,
}

/// You can add required fields here in the future
/// I'm keeping it minimal and using only required ones.
/// Maybe could be combined with API Serializations in the future

fn serialize_json_string<'de, D>(deserializer: D) -> Result<MessageBody, D::Error>
    where D: Deserializer<'de>
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(de::Error::custom)
    
}

#[derive(Clone, Debug)]
pub struct FlattenedMessageBody{
    topic: String,
    type_of: String,
    user_id: String,
    redemption_id: String,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageData{
    pub topic: String,
    #[serde(deserialize_with = "serialize_json_string")]
    pub message: MessageBody,
}
impl MessageData{
    pub fn flatten(self) -> FlattenedMessageBody{
        FlattenedMessageBody { 
            topic: self.topic,
            type_of: self.message.type_of, 
            user_id: self.message.data.redemption.user.id,
            redemption_id: self.message.data.redemption.reward.id,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageBody{
    #[serde(rename = "type")]
    pub type_of: String,
    pub data: MessageContentData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MessageContentData{
    pub redemption: Redemption,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Redemption{
    pub user: User,
    pub reward: Reward,
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Reward{
    pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct User{
    pub     id: String,
}
