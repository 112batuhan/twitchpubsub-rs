use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info};
use std::fmt::Debug;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{sleep, timeout, Duration, Instant};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

use super::pubsub_serializations::{Listen, MessageData, Request};

///BIG TODO: HANDLE ALL ERRORS!

/// GRACEFULL is tracked in ping thread.
/// GRACEFULL shutdown sends close message to the server first.
/// CLOSE signals the threads to close
/// RECONNECTS signals all threads to close except setup and

#[derive(Clone, Debug)]
pub enum Shutdown {
    GRACEFULL,
    CLOSE,
    RECONNECT,
}

#[derive(Debug, Default)]
pub struct PubsubHandler {
    url: String,
    broadcaster_id: String,
    oauth_token: String,
    nonce: String,
    message_export_sender: Option<broadcast::Sender<MessageData>>,
    pub active: Arc<AtomicBool>,
    pub shutdown_broadcast_sender: Option<broadcast::Sender<Shutdown>>,
    pub thread_handler: Option<JoinHandle<()>>,
}

impl PubsubHandler {
    pub fn new(broadcaster_id: &str, oauth_token: &str) -> Self {
        let charset = "abcdefghijklmnopqrstuvwxyz1234567890";
        let nonce = random_string::generate(8, charset);

        PubsubHandler {
            url: "wss://pubsub-edge.twitch.tv".to_string(),
            broadcaster_id: broadcaster_id.to_string(),
            oauth_token: oauth_token.to_string(),
            nonce: nonce,
            active: Arc::new(AtomicBool::new(false)),
            ..Default::default()
        }
    }

    pub async fn start(&mut self, reconnect: bool) {
        if self.active.load(Ordering::SeqCst) == true {
            if reconnect {
                debug!("Setup called when an active connection is present. Reconnecting.");
                if let Some(shutdown_broadcast_sender) = &self.shutdown_broadcast_sender {
                    //instead of this we could count the threads but this works too!
                    if shutdown_broadcast_sender.receiver_count() < 6 {
                        debug!("Still during the setup phase, Aborting reconnect.");
                        return;
                    }
                    shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                }
            } else {
                debug!("Setup called when an active connection is present. Opted out of reconnect.");
            }
            return;
        }
        self.active.store(true, Ordering::SeqCst);

        let request = Request::LISTEN {
            nonce: self.nonce.clone(),
            data: Listen {
                auth_token: self.oauth_token.clone(),
                topics: vec!["channel-points-channel-v1.".to_string() + &self.broadcaster_id],
            },
        };

        let (shutdown_broadcast_sender, shutdown_broadcast_receiver) =
            broadcast::channel::<Shutdown>(3);
        let (message_export_broadcast_sender, _message_export_broadcast_receiver) =
            broadcast::channel::<MessageData>(16);
        self.shutdown_broadcast_sender = Some(shutdown_broadcast_sender.clone());
        self.message_export_sender = Some(message_export_broadcast_sender.clone());

        let thread_handler = spawn_setup_thread(
            self.active.clone(),
            &self.url,
            request,
            shutdown_broadcast_sender,
            shutdown_broadcast_receiver,
            message_export_broadcast_sender,
        )
        .await;
        self.thread_handler = Some(thread_handler);
    }

    pub async fn stop(&mut self) {
        if let Some(shutdown_broadcast_sender) = &self.shutdown_broadcast_sender {
            shutdown_broadcast_sender.send(Shutdown::GRACEFULL).unwrap();
        }
    }
}

async fn spawn_setup_thread(
    active: Arc<AtomicBool>,
    url: &str,
    request: Request,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
    message_export_broadcast_sender: broadcast::Sender<MessageData>,
) -> JoinHandle<()> {
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

                send_listen(
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

async fn spawn_read_thread(
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
                                    let serialized_message: Request = serde_json::from_str(&text).unwrap();
                                    message_broadcast_sender.send(serialized_message.clone()).unwrap();
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
                                        _ => {}

                                    }
                                }
                                Message::Close(_)=> {
                                    debug!("Raw close message from server!");
                                    shutdown_broadcast_sender.send(Shutdown::CLOSE).unwrap();
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

async fn spawn_write_thread(
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
                                writer.send(Message::Close(None)).await.unwrap();
                                debug!("Close message sent to server in write thread.")
                            },
                            _ => {
                                let request_str = serde_json::to_string(&request).unwrap();
                                debug!("Request message sent to server in write thread. Raw JSON: {:?}",request_str);
                                writer.send(Message::text(request_str)).await.unwrap();
                            }
                        }
                    }
                } => {},
            }
        }
        debug!("Write thread has ended.");
    })
}

async fn spawn_ping_thread(
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

async fn send_listen(
    request: Request,
    message_mpsc_sender: mpsc::Sender<Request>,
    message_broadcast_receiver: broadcast::Receiver<Request>,
    shutdown_broadcast_sender: broadcast::Sender<Shutdown>,
    mut shutdown_broadcast_receiver: broadcast::Receiver<Shutdown>,
) -> JoinHandle<()> {
    message_mpsc_sender.send(request.clone()).await.unwrap();
    debug!("LISTEN Request sent to message mpsc channel for to be send in write channel. Complete message: {:?}", request);
    let (local_nonce, _) = request.unwrap_listen().unwrap();

    task::spawn(async move {
        let mut message_broadcast_receiver = message_broadcast_receiver.resubscribe();
        let start_of_response_wait = Instant::now();
        tokio::select! {
            biased;
            _ = async {
                shutdown_broadcast_receiver.recv().await.unwrap();
                debug!("Shutdown message received in listen send thread.");
            } => {},
            _ = async {
                if let Ok(request_result) = timeout(Duration::from_secs(10), message_broadcast_receiver.recv()).await{
                    if let Ok(request) = request_result{
                        let elapsed_time = Instant::now().duration_since(start_of_response_wait);
                        if matches!(&request, Request::RESPONSE { nonce:_, error:_ }) && elapsed_time < Duration::from_secs(10){
                            let (nonce, error) = request.unwrap_response().unwrap();
                            if nonce != local_nonce{
                                error!("Nonce check failed! Trying reconnection.");
                                shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                            }
                            if error != "".to_string(){
                                error!("Server sent an error: {}", error);
                                shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                            }
                            info!("Response received without errors.");
                        }else if elapsed_time > Duration::from_secs(10){
                            error!("No response has been received in 10 seconds after sending LISTEN request! Trying reconnection.");
                            shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                        }
                    }
                }else{
                    error!("No messages has been received in 10 seconds after sending LISTEN request! Trying reconnection.");
                    shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                }
            } => {},
        }

        debug!("Listen send thread has ended.");
    })
}

/// Second future sends close message to write thread to send close message to server.
/// If the server responds with close message, The read thread sends the close message
/// to other threads and closes them.
/// If not, then the second future waits for 10 seconds, then sends the close message itself.
async fn spawn_shutdown_thread(
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
