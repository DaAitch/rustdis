use connection::handle_connection;
use tokio::net::TcpListener;

mod connection;
mod processor;
mod resp;

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_connection(socket).await.unwrap();
        });
    }
}
