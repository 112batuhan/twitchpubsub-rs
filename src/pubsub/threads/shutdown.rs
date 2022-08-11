use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{sleep, Duration};

use tracing::debug;

use crate::pubsub::pubsub_serializations::Request;
use crate::pubsub::shutdown_enum::Shutdown;

/// Second future sends close message to write thread to send close message to server.
/// If the server responds with close message, The read thread sends the close message
/// to other threads and closes them.
/// If not, then the second future waits for 10 seconds, then sends the close message itself.
pub async fn spawn_shutdown_thread(
    message_mpsc_sender: mpsc::Sender<Request>,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
) -> JoinHandle<()> {
    let mut gracefull_shutdown_broadcast_receiver = shutdown_broadcast_sender.subscribe();
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
                                debug!("Shutdown message received in shutdown thread.");

                            },
                            _ => {},
                        }
                    }
                } => {},
                _ = async {
                    if let Ok(shutdown_message) = gracefull_shutdown_broadcast_receiver.recv().await{
                        match shutdown_message {
                            Shutdown::GRACEFULL => {
                                debug!("Gracefull shutdown message received in shutdown thread.");
                                message_mpsc_sender.send(Request::CLOSE).await.unwrap();
                                sleep(Duration::from_secs(5)).await;
                                debug!("No close message received in time. Forcing the shutdown.");
                                shutdown_broadcast_sender.send(Shutdown::CLOSE).unwrap();
                            }
                            _ => {},
                        }
                    }
                } => {},

            }
        }
        debug!("Shutdown thread has ended.");
    })
}
