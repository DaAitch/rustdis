use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use connection::handle_connection;
use tokio::net::TcpListener;

mod connection;
mod processor;
mod resp;

// again measured: seems to be no improvement
const STATE_SPREAD: usize = 1;

type Error = Box<dyn std::error::Error>;
type SpreadState = RwLock<HashMap<Vec<u8>, Vec<u8>>>;
type SharedState = Arc<[SpreadState; STATE_SPREAD]>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::builder()
        .format_module_path(false)
        // .filter_module("profiling", log::LevelFilter::Trace)
        .format_timestamp_nanos()
        .init();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let shared_state: SharedState = Arc::new([Default::default()]);

    loop {
        let (socket, _) = listener.accept().await?;
        let shared_state = shared_state.clone();
        tokio::spawn(async move {
            handle_connection(socket, shared_state).await.unwrap();
        });
    }
}
