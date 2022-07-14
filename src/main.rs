mod pubsub;

use tokio::io::Result;
use pubsub::pubsub_handler::{PubsubHandler};
use tokio::time::{sleep, Duration};
use std::fs;

///main for test because i don't know how lib.rs and tests work

#[tokio::main]
pub async fn main() -> Result<()> {
    
    let oauth_key = fs::read_to_string("src/secret.txt")
        .expect("Something went wrong reading the file");

    simple_logger::init_with_level(log::Level::Debug).unwrap();

    let mut handler = PubsubHandler::new("32473614", &oauth_key);

    handler.setup().await;

    sleep(Duration::from_secs(25)).await;
    handler.send_gracefull_shutdown_signal().await;

    handler.read_thread_handler.unwrap().await?; //to keep the threads living
    handler.write_thread_handler.unwrap().await?;


    Ok(())
}



