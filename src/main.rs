use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use connection::handle_connection;
use tokio::{net::TcpListener, runtime::Builder};

mod connection;
mod processor;
mod resp;

// again measured: seems to be no improvement
const STATE_SPREAD: usize = 1;

type SpreadState = Mutex<HashMap<Vec<u8>, Vec<u8>>>;
type SharedState = Arc<[SpreadState; STATE_SPREAD]>;

fn main() {
    env_logger::builder()
        .format_module_path(false)
        // .filter_module("profiling", log::LevelFilter::Trace)
        // .filter_module("p", log::LevelFilter::Trace)
        .format_timestamp_nanos()
        .init();

    let runtime = Builder::new_multi_thread()
        .enable_io()
        .max_blocking_threads(8)
        .thread_name("rustdis-worker")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap();

    runtime.block_on(run());
}

async fn run() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let shared_state: SharedState = Arc::new([Default::default()]);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let shared_state = shared_state.clone();
        tokio::spawn(async move {
            handle_connection(socket, shared_state).await.unwrap();
        });
    }
}
