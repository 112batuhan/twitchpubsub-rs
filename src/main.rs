mod pubsub;

use pubsub::pubsub_handler::PubsubHandler;
use std::fs;
use tokio::io::Result;
use tokio::time::{sleep, Duration};
use tracing::{self, Level};
use tracing_subscriber::FmtSubscriber;

///main for test because i don't know how lib.rs and tests work

#[tokio::main]
pub async fn main() -> Result<()> {
    let oauth_key =
        fs::read_to_string("src/secret.txt").expect("Something went wrong reading the file");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut handler = PubsubHandler::new("32473614", &oauth_key);

    sleep(Duration::from_secs(10)).await;
    handler.start(false).await;
    //sleep(Duration::from_secs(10)).await;
    handler.start(true).await;
    //sleep(Duration::from_secs(10)).await;
    handler.start(true).await;

    sleep(Duration::from_secs(10)).await;
    handler.stop().await;

    handler.get_message_receiver();

    sleep(Duration::from_secs(10)).await;
    handler.start(true).await;
    sleep(Duration::from_secs(10)).await;
    handler.stop().await;
    handler.thread_handler.unwrap().await?; //to keep the threads living

    Ok(())
}
