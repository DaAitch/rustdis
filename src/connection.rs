use crate::resp_read::{RespError, RespEventsTransformer, RespEventsVisitor};
use bytes::{Bytes, BytesMut};
use std::{fmt::Write, sync::mpsc};
use tokio::{io::AsyncReadExt, task::JoinError};
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
pub enum ConnectionError {
    Resp(RespError),
    Io(std::io::Error),
}

type Result<T> = std::result::Result<T, ConnectionError>;

pub struct Connection {
    socket: TcpStream,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self { socket }
    }

    pub async fn run(&mut self) -> Result<()> {
        let transformer = RespEventsTransformer::new();
        let buf = BytesMut::with_capacity(65536);

        let (sender, receiver) = mpsc::channel::<SendEvents>();

        let mut context = Some((transformer, buf, sender));

        loop {
            let (mut transformer, mut buf, sender) = context.take().unwrap();

            if self
                .socket
                .read_buf(&mut buf)
                .await
                .map_err(|err| ConnectionError::Io(err))?
                == 0
            {
                break;
            }

            println!("<< {:?}", &buf);

            let (transformer, mut buf, sender) = tokio::task::spawn_blocking(
                move || -> Result<(RespEventsTransformer, BytesMut, mpsc::Sender<SendEvents>)> {
                    let mut processor = Processor {
                        sender: &sender,
                        state: (),
                    };
                    transformer
                        .process(&mut processor, &mut buf)
                        .map_err(|err| ConnectionError::Resp(err))?;

                    Ok((transformer, buf, sender))
                },
            )
            .await
            .unwrap()?;

            buf.clear();

            loop {
                let event = receiver.try_recv();
                match event {
                    Ok(SendEvents::Data(mut data)) => {
                        println!(">> {:?}", &data);
                        self.socket
                            .write_buf(&mut data)
                            .await
                            .map_err(ConnectionError::Io)?;
                    }
                    Ok(SendEvents::Flush) => {
                        println!(">> flush");
                        self.socket.flush().await.map_err(ConnectionError::Io)?;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => unreachable!(),
                }
            }

            context = Some((transformer, buf, sender));
        }

        Ok(())
    }
}

struct Processor<'a> {
    sender: &'a mpsc::Sender<SendEvents>,
    state: (),
}

impl<'a> RespEventsVisitor for Processor<'a> {
    fn on_string(&mut self, string: Vec<u8>) {}

    fn on_error(&mut self, error: Vec<u8>) {}

    fn on_integer(&mut self, integer: isize) {}

    fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) {
        let mut buf = BytesMut::with_capacity(65536);
        write!(buf, "$2\r\nhi\r\n").unwrap();
        self.sender.send(SendEvents::Data(buf.into())).unwrap();
    }

    fn on_array(&mut self, length: Option<usize>) {}
}

enum SendEvents {
    Data(Bytes),
    Flush,
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
