use crate::resp_read::{RespError, RespEventsTransformer, RespEventsVisitor, VisitorResult};
use bytes::{Bytes, BytesMut};
use std::fmt::{self, Write};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
pub enum ConnectionError {
    Resp(RespError<ProcessorError>),
    Io(std::io::Error),
}

type Result<T> = std::result::Result<T, ConnectionError>;

pub async fn handle_connection(mut socket: TcpStream) -> Result<()> {
    let mut transformer = RespEventsTransformer::new();
    let mut buf = BytesMut::with_capacity(65536);
    let mut state = ();

    loop {
        if socket
            .read_buf(&mut buf)
            .await
            .map_err(|err| ConnectionError::Io(err))?
            == 0
        {
            break;
        }

        println!("<< {:?}", &buf);

        let (sender, mut receiver) = mpsc::channel::<SendEvents>(100);
        let process_task = tokio::task::spawn_blocking(
            move || -> Result<(RespEventsTransformer, BytesMut, ())> {
                let mut processor = Processor { sender, state };
                transformer
                    .process(&mut processor, &mut buf)
                    .map_err(|err| ConnectionError::Resp(err))?;

                Ok((transformer, buf, state))
            },
        );

        let sending_task: tokio::task::JoinHandle<Result<TcpStream>> = tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                match event {
                    SendEvents::Data(mut data) => {
                        println!(">> {:?}", &data);
                        socket
                            .write_buf(&mut data)
                            .await
                            .map_err(ConnectionError::Io)?;
                    }
                    SendEvents::Flush => {
                        println!(">> flush");
                        socket.flush().await.map_err(ConnectionError::Io)?;
                    }
                }
            }

            Ok(socket)
        });

        let (process_result, sending_result) = tokio::join!(process_task, sending_task);

        // write back transformer, buf, state
        let process_result = process_result.unwrap()?;
        transformer = process_result.0;
        buf = process_result.1;
        state = process_result.2;

        // write back socket
        let sending_result = sending_result.unwrap()?;
        socket = sending_result;

        buf.clear();
    }

    Ok(())
}

pub enum ProcessorError {}

impl fmt::Debug for ProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "processor error")
    }
}

struct Processor {
    sender: mpsc::Sender<SendEvents>,
    state: (),
}

impl RespEventsVisitor<ProcessorError> for Processor {
    fn on_string(&mut self, _string: Vec<u8>) -> VisitorResult<ProcessorError> {
        Ok(())
    }

    fn on_error(&mut self, _error: Vec<u8>) -> VisitorResult<ProcessorError> {
        Ok(())
    }

    fn on_integer(&mut self, _integer: isize) -> VisitorResult<ProcessorError> {
        Ok(())
    }

    fn on_bulk_string(&mut self, _bulk_string: Option<Vec<u8>>) -> VisitorResult<ProcessorError> {
        let mut buf = BytesMut::with_capacity(65536);
        write!(buf, "$2\r\nhi\r\n").unwrap();

        let sender = self.sender.clone();
        tokio::spawn(async move {
            let _ = sender.send(SendEvents::Data(buf.into())).await;
        });

        Ok(())
    }

    fn on_array(&mut self, _length: Option<usize>) -> VisitorResult<ProcessorError> {
        Ok(())
    }
}

enum SendEvents {
    Data(Bytes),
    Flush,
}
