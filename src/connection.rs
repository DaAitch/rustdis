use std::collections::HashMap;

use crate::{
    processor::{Processor, ProcessorError, ProcessorState},
    resp::write::SocketWriter,
};
use crate::{
    resp::read::{RespError, RespEventsTransformer},
    SharedState,
};
use bytes::BytesMut;
use log::{debug, trace};
use tokio::net::TcpStream;
use tokio::{
    io::AsyncReadExt,
    spawn,
    sync::mpsc::{Receiver, Sender},
};
use tokio::{sync::mpsc, task::spawn_blocking};

#[derive(Debug)]
pub enum ConnectionError {
    Resp(RespError<ProcessorError>),
    Io(std::io::Error),
    JoinError(tokio::task::JoinError),
    SendError(tokio::sync::mpsc::error::SendError<SendEvents>),
}

type Result<T> = std::result::Result<T, ConnectionError>;

pub async fn handle_connection(socket: TcpStream, shared_state: SharedState) -> Result<()> {
    let (sender, receiver) = mpsc::channel::<SendEvents>(10000);

    let mut process_ctx = ProcessContext {
        transformer: RespEventsTransformer::new(),
        buf: BytesMut::with_capacity(65536),
        state: ProcessorState::NoState,
        sender,
    };
    let mut io_ctx = UpstreamContext {
        socket,
        receiver,
        shared_state,
    };

    while io_ctx
        .socket
        .read_buf(&mut process_ctx.buf)
        .await
        .map_err(|err| ConnectionError::Io(err))?
        > 0
    {
        debug!(">> IN  {:?}", &process_ctx.buf);

        let process_task = spawn_blocking(move || {
            let ProcessContext {
                mut transformer,
                mut buf,
                state,
                sender,
            } = process_ctx;
            let mut processor = Processor::new(&sender, state);
            transformer
                .process(&mut processor, &mut buf)
                .map_err(|err| ConnectionError::Resp(err))?;

            let state = processor.into_state();

            Ok(spawn(async move {
                sender.send(SendEvents::End).await?;

                Ok(ProcessContext {
                    transformer,
                    buf,
                    state,
                    sender,
                })
            }))
        });

        let upstream_task = spawn(async move {
            let mut socket_writer = SocketWriter::new(&mut io_ctx.socket);

            while let Some(event) = io_ctx.receiver.recv().await {
                trace!("upstream event: {}", &event);

                match event {
                    SendEvents::InfoCommand => {
                        socket_writer
                            .write_ok()
                            .await
                            .map_err(ConnectionError::Io)?;
                    }
                    SendEvents::SetCommand(key, value) => {
                        let _ = io_ctx.shared_state.write().unwrap().insert(key, value);
                        socket_writer
                            .write_ok()
                            .await
                            .map_err(ConnectionError::Io)?;
                    }
                    SendEvents::GetCommand(key) => {
                        let state = io_ctx
                            .shared_state
                            .read()
                            .unwrap()
                            .get(&key)
                            .map(|value| value.clone());

                        match state {
                            Some(value) => {
                                socket_writer
                                    .write_bulk_string(value.as_slice())
                                    .await
                                    .map_err(ConnectionError::Io)?;
                            }
                            None => {
                                socket_writer
                                    .write_bulk_string_nil()
                                    .await
                                    .map_err(ConnectionError::Io)?;
                            }
                        }
                    }
                    SendEvents::End => {
                        break;
                    }
                }
            }

            Ok(io_ctx)
        });

        let (process_result, sending_result) = tokio::join!(process_task, upstream_task);

        {
            let processor_result = process_result.map_err(ConnectionError::JoinError)?;
            let end_sender_result = processor_result?;
            let send_result = end_sender_result
                .await
                .map_err(ConnectionError::JoinError)?;
            process_ctx = send_result.map_err(ConnectionError::SendError)?;
        }

        {
            let socket_result = sending_result.map_err(ConnectionError::JoinError)?;
            io_ctx = socket_result?;
        }

        process_ctx.buf.clear();
    }

    Ok(())
}

#[derive(Debug)]
pub enum SendEvents {
    SetCommand(Vec<u8>, Vec<u8>),
    GetCommand(Vec<u8>),
    InfoCommand,
    End,
}

impl std::fmt::Display for SendEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendEvents::InfoCommand => {
                write!(f, "info")
            }
            SendEvents::SetCommand(key, value) => {
                let key = String::from_utf8(key.clone()).map_err(|_| std::fmt::Error {})?;
                let value = String::from_utf8(value.clone()).map_err(|_| std::fmt::Error {})?;

                write!(f, "set {} {}", &key, &value)
            }
            SendEvents::GetCommand(key) => {
                let key = String::from_utf8(key.clone()).map_err(|_| std::fmt::Error {})?;

                write!(f, "get {}", &key)
            }
            SendEvents::End => {
                write!(f, "END")
            }
        }
    }
}

struct ProcessContext {
    transformer: RespEventsTransformer,
    buf: BytesMut,
    state: ProcessorState,
    sender: Sender<SendEvents>,
}

struct UpstreamContext {
    socket: TcpStream,
    receiver: Receiver<SendEvents>,
    shared_state: SharedState,
}
