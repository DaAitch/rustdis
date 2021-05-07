use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use connection::handle_connection;
use tokio::net::TcpListener;

mod connection;
mod processor;
mod resp;

type Error = Box<dyn std::error::Error>;
type SharedState = Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::builder().format_module_path(false).init();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let shared_state = Arc::new(RwLock::new(HashMap::<Vec<u8>, Vec<u8>>::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let shared_state = shared_state.clone();
        tokio::spawn(async move {
            handle_connection(socket, shared_state).await.unwrap();
        });
    }
}
