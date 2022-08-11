use futures::stream::SplitSink;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error};

use crate::pubsub::pubsub_serializations::Request;
use crate::pubsub::shutdown_enum::Shutdown;

pub async fn spawn_write_thread(
    mut writer: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    mut message_mpsc_receiver: mpsc::Receiver<Request>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
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
                                debug!("Shutdown message received in write thread.");
                            },
                            _ => {}
                        }
                    }
                } => {},
                _ = async{
                    if let Some(request) = message_mpsc_receiver.recv().await{
                        match request {
                            Request::CLOSE => {
                                if let Err(err) = writer.send(Message::Close(None)).await{
                                    error!("An error occured while trying to send close message to server in read thread. Error: {}", err);
                                }else{
                                    debug!("Close message sent to server in write thread.");
                                }
                            },
                            _ => {
                                let request_str_result = serde_json::to_string(&request);
                                match request_str_result {
                                    Err(err) => error!("An error occured when trying to deserialize. Check serializations. Error: {}", err),
                                    Ok(request_str) => {
                                        debug!("Request message sent to server in write thread. Raw JSON: {:?}",request_str);
                                        if let Err(err) = writer.send(Message::text(request_str.clone())).await{
                                            error!("An error occured while trying to send message to server in read thread. Error: {}", err);
                                        }else{
                                            debug!("Request message sent to server in write thread. Raw JSON: {:?}", request_str);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } => {},
            }
        }
        debug!("Write thread has ended.");
    })
}
