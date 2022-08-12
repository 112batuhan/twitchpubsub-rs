# twitchpubsub-rs
twitch.tv pubsub client implementation. Only implemented channel rewards so far because that's what i needed in my main project.

I made this for my hobby project. I'm no means a good rust programmer, let alone a good programmer. But i believe that this client is usable now. Because of rust compiler, I feel more comfortable sharing this project with others than when I write code with other languages.

If you want something more profesional, Check out [this repository](https://github.com/Nerixyz/twitch-pubsub-rs)


## To use: 

Add this repository to Cargo.toml:

```toml
[dependencies]
twitchpubsub = { git = "https://github.com/112batuhan/twitchpubsub-rs" }
```

You can also add the other crates that is used in the following example:
```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = "0.3"
tokio = {version = "1.19", features = ["full"]}
```

main.rs:
```rust
extern crate twitchpubsub;

use twitchpubsub::pubsub_handler::PubsubHandler;
use tokio::io::Result;
use tokio::time::{sleep, Duration};
use tracing::{self, Level};
use tracing_subscriber::FmtSubscriber;

///main for test because i don't know how lib.rs and tests work

#[tokio::main]
pub async fn main() -> Result<()> {
    
    //A subscriber for tracing crate to see logs.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    //Creating a pubsub Handler with broadcaster id and oauth_key
    let broadcaster_id:&str = "asd";
    let oauth_key:&str = "123";
    let mut handler = PubsubHandler::new(broadcaster_id, oauth_key);

    //Setting reconnect to false will not reset the connection
    //and setting it true will reconnect the handler to the client.
    //If it's a brand new connection, this setting doesn't matter.
    handler.start(false).await;
    handler.start(true).await;

    sleep(Duration::from_secs(5)).await;

    //Disconnect the websocket and stop threads.
    handler.stop().await;

    //This method returns a copy of a tokio broadcast receiver.
    //Use this to handle messages from the server.
    handler.get_message_receiver();

    //to keep the threads living
    handler.thread_handler.unwrap().await?;
    Ok(())
}
```