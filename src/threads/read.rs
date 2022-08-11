use futures::stream::SplitStream;
use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::task::{self, JoinHandle};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error};

use crate::pubsub_serializations::{MessageData, Request};
use crate::shutdown_enum::Shutdown;

pub async fn spawn_read_thread(
    mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    message_broadcast_sender: broadcast::Sender<Request>,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
    message_export_broadcast_sender: broadcast::Sender<MessageData>,
) -> JoinHandle<()> {
    task::spawn(async move {
        let mut keep_alive = true;
        while keep_alive {
            tokio::select! {
                biased;
                _ = async {
                    if let Ok(shutdown_message) = shutdown_broadcast_receiver.recv().await{
                        match shutdown_message {
                            Shutdown::CLOSE | Shutdown::RECONNECT => {
                                keep_alive = false;
                                debug!("Shutdown message received in read thread.");
                            },
                            _ => {}
                        }
                    }
                } => {},
                _ = async{
                    if let Some(message_result) = reader.next().await {
                        if let Ok(message) = message_result{
                            match message {
                                Message::Text(text) => {
                                    let serialized_message_response:Result<Request, serde_json::Error> = serde_json::from_str(&text);
                                    match serialized_message_response {
                                        Err(err) => error!("Unexpected message format from server. {:?}", err),
                                        Ok(serialized_message) => {
                                            if let Err(_) = message_broadcast_sender.send(serialized_message.clone()){
                                                error!("Error while sending messages to other threads from read thread. No active receivers.");
                                            }else{
                                                debug!("Request send to message broadcast channel in read thread. Sent data: {:?}",serialized_message.clone());
                                                match serialized_message {
                                                    Request::MESSAGE {data} => {
                                                        if let Ok(_) = message_export_broadcast_sender.send(data.clone()){
                                                            debug!("MESSAGE Request sent to export broadcast channel in read thread. Sent data: {:?}",data);
                                                        }
                                                        else{
                                                            debug!("MESSAGE could not be exported. No active receivers.");
                                                        }

                                                    },
                                                    Request::RECONNECT => {
                                                        debug!("RECONNECT message from server.");
                                                        if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
                                                            error!("An error occured while trying to send shutdown message from read thread to other threads.
                                                                    No active shutdown broadcast receivers.");
                                                        }

                                                    },
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                }
                                Message::Close(_)=> {
                                    debug!("Raw close message from server!");
                                    if let Err(_) = shutdown_broadcast_sender.send(Shutdown::CLOSE){
                                        error!("An error occured while trying to send shutdown message from read thread to other threads.
                                                No active shutdown broadcast receivers.");
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                } => {},
            }
        }
        debug!("Read thread has ended.");
    })
}
