use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info};

use crate::pubsub_serializations::Request;
use crate::shutdown_enum::Shutdown;

pub async fn spawn_listen_thread(
    request: Request,
    message_mpsc_sender: mpsc::Sender<Request>,
    message_broadcast_receiver: broadcast::Receiver<Request>,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
) -> JoinHandle<()> {
    if let Err(_) = message_mpsc_sender.send(Request::PING).await{
        error!("Error while sending PING message to writer thread from ping thread. No active receivers.");
        if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
            error!("An error occured while trying to send reconnect message from ping thread to other threads.
                    No active shutdown broadcast receivers.");
        }
    }
    debug!("LISTEN Request sent to message mpsc channel for to be send in write channel. Complete message: {:?}", request);
    let (local_nonce, _) = request.unwrap_listen().unwrap();

    task::spawn(async move {
        let mut message_broadcast_receiver = message_broadcast_receiver.resubscribe();
        let start_of_response_wait = Instant::now();
        
        tokio::select! {
            biased;
            _ = async {
                //Here, it's not necessary to check the message type as there is no loop in this thread.
                //If a shudown message comes, the thread will end anyway.
                if let Ok(_) = shutdown_broadcast_receiver.recv().await{
                    debug!("Shutdown message received in listen thread.");
                }
            } => {},
            _ = async {
                if let Ok(request_result) = timeout(Duration::from_secs(10), message_broadcast_receiver.recv()).await{
                    if let Ok(request) = request_result{
                        let elapsed_time = Instant::now().duration_since(start_of_response_wait);
                        if matches!(&request, Request::RESPONSE { nonce:_, error:_ }) && elapsed_time < Duration::from_secs(10){
                            let (nonce, error) = request.unwrap_response().unwrap();
                            if nonce != local_nonce{
                                error!("Nonce check failed! Trying reconnection.");
                                if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
                                    error!("An error occured while trying to send RECONNECT message from listen thread to other threads.
                                            No active shutdown broadcast receivers.");
                                }
                            }
                            else if error != "".to_string(){
                                error!("Server sent an error in response to LISTEN request: {}", error);
                                if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
                                    error!("An error occured while trying to send CLOSE message from listen thread to other threads.
                                            No active shutdown broadcast receivers.");
                                }
                            }
                            else{
                                info!("Response received without errors.");
                            }
                        }else if elapsed_time > Duration::from_secs(10){
                            error!("No response has been received in 10 seconds after sending LISTEN request! Trying reconnection.");
                            if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
                                error!("An error occured while trying to send RECONNECT message from listen thread to other threads.
                                        No active shutdown broadcast receivers.");
                            }
                        }
                    }
                }else{
                    error!("No messages has been received in 10 seconds after sending LISTEN request! Trying reconnection.");
                    if let Err(_) = shutdown_broadcast_sender.send(Shutdown::RECONNECT){
                        error!("An error occured while trying to send RECONNECT message from listen thread to other threads.
                                No active shutdown broadcast receivers.");
                    }
                }
            } => {},
        }

        debug!("Listen send thread has ended.");
    })
}
