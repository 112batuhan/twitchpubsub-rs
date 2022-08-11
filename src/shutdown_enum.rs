/// GRACEFULL shutdown sends close message to the server first.
/// CLOSE signals the threads to close
/// RECONNECTS signals all threads to close except setup and

#[derive(Clone, Debug)]
pub enum Shutdown {
    GRACEFULL,
    CLOSE,
    RECONNECT,
}
