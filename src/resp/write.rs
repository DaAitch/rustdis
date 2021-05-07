use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use super::{B_ASTERISK, B_CR, B_DOLLAR, B_LF};

type Result<T> = std::io::Result<T>;

pub struct SocketWriter<'a> {
    socket: &'a mut TcpStream,
}

impl<'a> SocketWriter<'a> {
    pub fn new(socket: &'a mut TcpStream) -> Self {
        Self { socket }
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        println!("<< OUT {:?}", String::from_utf8(bytes.into()).unwrap());
        self.socket.write_all(bytes).await
    }

    pub async fn write_ok(&mut self) -> Result<()> {
        self.write_bulk_string(b"OK").await?;
        self.socket.flush().await
    }

    pub async fn write_bulk_string(&mut self, bytes: &[u8]) -> Result<()> {
        self.write(&[B_DOLLAR]).await?;
        let len = bytes.len().to_string();
        self.write(len.as_bytes()).await?;
        self.write_rn().await?;
        self.write(bytes).await?;
        self.write_rn().await?;

        Ok(())
    }

    pub async fn write_bulk_string_nil(&mut self) -> Result<()> {
        self.write(b"$-1\r\n").await?;
        Ok(())
    }

    async fn write_rn(&mut self) -> Result<()> {
        self.write(b"\r\n").await
    }

    pub async fn write_array_start(&mut self, len: &[u8]) -> Result<()> {
        self.write(&[B_ASTERISK]).await?;
        self.write(len).await?;
        self.write_rn().await?;
        Ok(())
    }

    pub async fn write_array_end(&mut self) -> Result<()> {
        self.write_rn().await?;
        Ok(())
    }
}