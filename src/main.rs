use connection::Connection;
use tokio::net::TcpListener;

mod connection;
mod resp_read;
mod resp_write;

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut conn = Connection::new(socket);
            conn.run().await.unwrap();
        });
    }
}
