use std::fmt::Debug;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use tracing::{debug, error};

use super::pubsub_serializations::{Listen, MessageData, Request};
use super::shutdown_enum::Shutdown;
use super::threads::setup::spawn_setup_thread;

/// BIG TODO: HANDLE ALL ERRORS!
/// TODO: implement drop ??

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
                    //this unwrap is protected by the if statement so i'm keeping it.
                    shutdown_broadcast_sender.send(Shutdown::RECONNECT).unwrap();
                }
            } else {
                debug!(
                    "Setup called when an active connection is present. Opted out of reconnect."
                );
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
            if let Err(err) = shutdown_broadcast_sender.send(Shutdown::GRACEFULL) {
                error!(
                    "An error occured: {} It's probably because there is no active connection.",
                    err
                )
            }
        }
    }

    pub fn get_message_receiver(&mut self) -> Option<broadcast::Receiver<MessageData>> {
        if let Some(message_export_sender) = &self.message_export_sender {
            Some(message_export_sender.subscribe())
        } else {
            None
        }
    }
}
