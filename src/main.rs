mod pubsub;

use pubsub::pubsub_handler::PubsubHandler;
use std::fs;
use tokio::io::Result;
use tokio::time::{sleep, Duration};

///main for test because i don't know how lib.rs and tests work

#[tokio::main]
pub async fn main() -> Result<()> {
    let oauth_key =
        fs::read_to_string("src/secret.txt").expect("Something went wrong reading the file");

    simple_logger::init_with_level(log::Level::Debug).unwrap();

    let mut handler = PubsubHandler::new("32473614", &oauth_key);

    handler.setup(false).await;
    sleep(Duration::from_secs(5)).await;
    handler.setup(false).await;
    sleep(Duration::from_secs(5)).await;
    handler.setup(true).await;

    
    sleep(Duration::from_secs(5)).await;
    handler.send_gracefull_shutdown_signal().await;

    handler.thread_handler.unwrap().await?; //to keep the threads living

    Ok(())

}
