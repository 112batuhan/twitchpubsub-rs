use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, error};

use crate::pubsub::pubsub_serializations::Request;
use crate::pubsub::shutdown_enum::Shutdown;

pub async fn spawn_ping_thread(
    message_mpsc_sender: mpsc::Sender<Request>,
    message_broadcast_receiver: broadcast::Receiver<Request>,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
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
                                debug!("Shutdown message received in ping thread.");

                            },
                            _ => {},
                        }
                    }
                } => {},
                _ = async {sleep(Duration::from_secs(5*60)).await;
                    message_mpsc_sender.send(Request::PING).await.unwrap();
                    let mut message_broadcast_receiver = message_broadcast_receiver.resubscribe();
                    let start_of_pong_wait = Instant::now();
                    loop{
                        if let Ok(request_result) = timeout(Duration::from_secs(10), message_broadcast_receiver.recv()).await{
                            if let Ok(request) = request_result{
                                let elapsed_time = Instant::now().duration_since(start_of_pong_wait);
                                if matches!(request, Request::PONG) && elapsed_time < Duration::from_secs(10){
                                    debug!("Received PONG in time!");
                                    break;
                                }else if elapsed_time > Duration::from_secs(10){
                                    error!("No PONG request has been received in 10 seconds after sending PING request! Trying reconnection.");
                                    shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                                    break;
                                }
                            }
                        }else{
                            error!("No response has been received in 10 seconds after sending PING request! Trying reconnection.");
                            shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                            break;
                        }
                    }} => {}
            }
        }
        debug!("Ping thread has ended.");
    })
}
