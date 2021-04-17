use bytes::BytesMut;
use resp::{RespEventsTransformer, RespEventsVisitor};
use tokio::{io::AsyncReadExt, net::TcpListener};

mod resp;

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            process_socket(socket).await.unwrap();
        });
    }
}

async fn process_socket(mut socket: tokio::net::TcpStream) -> Result<(), Error> {
    let mut transformer = RespEventsTransformer::new(RespEventPrinter {});

    loop {
        let mut buf = BytesMut::with_capacity(65536);
        if socket.read_buf(&mut buf).await? == 0 {
            break;
        }

        println!("{:?}", buf);
        transformer
            .process(buf)
            .map_err(|err| Into::<Error>::into(err))?;
    }

    Ok(())
}

struct RespEventPrinter {}

impl RespEventsVisitor for RespEventPrinter {
    fn on_string(&mut self, string: Vec<u8>) {
        let string = String::from_utf8(string).unwrap();
        println!("on_string: {}", &string);
    }

    fn on_error(&mut self, error: Vec<u8>) {
        let error = String::from_utf8(error).unwrap();
        println!("on_error: {}", &error);
    }

    fn on_integer(&mut self, integer: isize) {
        println!("on_integer: {}", &integer);
    }

    fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) {
        match bulk_string {
            Some(string) => {
                let string = String::from_utf8(string).unwrap();
                println!("on_bulk_string: {}", &string);
            }
            None => {
                println!("on_bulk_string: null");
            }
        }
    }

    fn on_array(&mut self, length: Option<usize>) {
        match length {
            Some(length) => {
                println!("on_array: {}", length);
            }
            None => {
                println!("on_array: null");
            }
        }
    }
}
