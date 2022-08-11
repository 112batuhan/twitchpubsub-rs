use futures_util::StreamExt;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info};

use super::listen::spawn_listen_thread;
use super::ping::spawn_ping_thread;
use super::read::spawn_read_thread;
use super::shutdown::spawn_shutdown_thread;
use super::write::spawn_write_thread;

use crate::pubsub::pubsub_serializations::{MessageData, Request};
use crate::pubsub::shutdown_enum::Shutdown;

pub async fn spawn_setup_thread(
    active: Arc<AtomicBool>,
    url: &str,
    request: Request,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
    message_export_broadcast_sender: broadcast::Sender<MessageData>,
) -> JoinHandle<()> {
    //TODO: Put this url in new()
    let url = url::Url::parse(&url).unwrap();
    task::spawn(async move {
        let mut connect = true;
        'keep_alive: loop {
            if connect {
                let retry_limit = 20;
                let mut current_try = 0;
                let mut retry_ms = 10;

                let ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>;

                'keep_retry: loop {
                    let connection = connect_async(&url).await;
                    match connection {
                        Err(err) => {
                            error!("Connection attempt failed. ERROR: {:?}", err);
                            sleep(Duration::from_millis(retry_ms)).await;
                            retry_ms *= 2;
                            current_try += 1;

                            if current_try >= retry_limit {
                                error!(
                                    "Failed to connect within retry limit. Cancelling the attemps."
                                );
                                break 'keep_alive;
                            }
                        }
                        Ok((local_ws_sream, _response)) => {
                            ws_stream = Some(local_ws_sream);
                            info!("Connection successful!");
                            connect = false;
                            break 'keep_retry;
                        }
                    }
                }

                let (writer, reader) = ws_stream.unwrap().split();

                let (reader_broadcast_sender, mut _reader_broadcast_receiver) =
                    broadcast::channel::<Request>(16);
                let (writer_mpsc_sender, writer_mpsc_receiver) = mpsc::channel::<Request>(16);

                spawn_listen_thread(
                    request.clone(),
                    writer_mpsc_sender.clone(),
                    reader_broadcast_sender.subscribe(),
                    shutdown_broadcast_sender.clone(),
                    shutdown_broadcast_sender.subscribe(),
                )
                .await;

                spawn_ping_thread(
                    writer_mpsc_sender.clone(),
                    reader_broadcast_sender.subscribe(),
                    shutdown_broadcast_sender.clone(),
                    shutdown_broadcast_sender.subscribe(),
                )
                .await;

                spawn_write_thread(
                    writer,
                    writer_mpsc_receiver,
                    shutdown_broadcast_sender.subscribe(),
                )
                .await;

                spawn_read_thread(
                    reader,
                    reader_broadcast_sender,
                    shutdown_broadcast_sender.clone(),
                    shutdown_broadcast_sender.subscribe(),
                    message_export_broadcast_sender.clone(),
                )
                .await;

                spawn_shutdown_thread(
                    writer_mpsc_sender.clone(),
                    shutdown_broadcast_sender.clone(),
                    shutdown_broadcast_sender.subscribe(),
                )
                .await;
            }

            if let Ok(shutdown_message) = shutdown_broadcast_receiver.recv().await {
                match shutdown_message {
                    Shutdown::CLOSE => {
                        debug!(
                            "Close message received in setup thread. The thread won't continue."
                        );
                        break 'keep_alive;
                    }
                    Shutdown::RECONNECT => {
                        debug!("Reconnect message received in setup thread. The connection will be restarted.");
                        connect = true;
                    }
                    _ => {}
                }
            }
        }
        active.store(false, Ordering::SeqCst);
        debug!("Setup thread has ended.");
    })
}
