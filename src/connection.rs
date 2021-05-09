use crate::processor::{Processor, ProcessorError, ProcessorState};
use crate::{
    resp::read::{RespError, RespEventsTransformer},
    SharedState,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, trace};
use tokio::{io::AsyncReadExt, spawn, task::block_in_place};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio::{
    select,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};

#[derive(Debug)]
pub enum ConnectionError {
    Resp(RespError<ProcessorError>),
    Io(std::io::Error),
    JoinError(tokio::task::JoinError),
    SendError(tokio::sync::mpsc::error::SendError<SendEvents>),
}

type Result<T> = std::result::Result<T, ConnectionError>;

pub async fn handle_connection(socket: TcpStream, shared_state: SharedState) -> Result<()> {
    let (sender, receiver) = mpsc::unbounded_channel::<SendEvents>();

    let mut process_ctx = ProcessContext {
        transformer: RespEventsTransformer::new(),
        buf: BytesMut::with_capacity(65536), // which number makes sense?
        state: ProcessorState::NoState,
        sender,
        shared_state,
    };
    let mut io_ctx = UpstreamContext {
        socket,
        receiver,
        other_buf: BytesMut::with_capacity(65536),
        write_buf: BytesMut::with_capacity(65536),
    };

    loop {
        if !process_ctx.buf.has_remaining() {
            let s = io_ctx
                .socket
                .read_buf(&mut process_ctx.buf)
                .await
                .map_err(|err| ConnectionError::Io(err))?;

            if s == 0 {
                break;
            }
        }
        debug!(">> IN  {:?}", &process_ctx.buf);

        let upstream_task = spawn(async move {
            let end_of_socket = loop {
                select! {
                    Some(event) = io_ctx.receiver.recv() => {
                        match event {
                            SendEvents::Data(bytes) => {
                                let i = std::time::Instant::now();
                                if io_ctx.write_buf.has_remaining() && io_ctx.write_buf.remaining_mut() < bytes.remaining() {
                                    // check if bytes larger than capacity, then directly write?
                                    io_ctx
                                        .socket
                                        .write_all(&io_ctx.write_buf)
                                        .await
                                        .map_err(ConnectionError::Io)?;

                                    io_ctx.write_buf.clear();
                                }

                                io_ctx.write_buf.extend(bytes);

                                trace!(target: "p", "write took {:?}", i.elapsed());
                            }
                            SendEvents::End => {
                                if io_ctx.write_buf.has_remaining() {
                                    io_ctx
                                        .socket
                                        .write_all(&io_ctx.write_buf)
                                        .await
                                        .map_err(ConnectionError::Io)?;

                                    io_ctx.write_buf.clear();
                                }

                                io_ctx.socket.flush().await.map_err(ConnectionError::Io)?;
                                break false;
                            }
                        }
                    },
                    read_buf_result = io_ctx.socket.read_buf(&mut io_ctx.other_buf) => {
                        let s = read_buf_result.map_err(ConnectionError::Io)?;
                        if s == 0 {
                            break true;
                        }
                    }
                }
            };

            if end_of_socket {
                while let Some(event) = io_ctx.receiver.recv().await {
                    match event {
                        SendEvents::Data(bytes) => {
                            io_ctx
                                .socket
                                .write_all(&bytes)
                                .await
                                .map_err(ConnectionError::Io)?;
                        }
                        SendEvents::End => {
                            io_ctx.socket.flush().await.map_err(ConnectionError::Io)?;
                            break;
                        }
                    }
                }
            }

            Ok((io_ctx, end_of_socket))
        });

        process_ctx = block_in_place(move || {
            debug!(target: "profiling", "start processing task");
            let ProcessContext {
                mut transformer,
                mut buf,
                state,
                sender,
                shared_state,
            } = process_ctx;

            let mut processor = Processor::new(&sender, &shared_state, state);
            // TODO: not send every few bytes a bulk at once !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            transformer
                .process(&mut processor, &mut buf)
                .map_err(ConnectionError::Resp)?;

            sender
                .send(SendEvents::End)
                .map_err(ConnectionError::SendError)?;

            debug!(target: "profiling", "end processing task");

            Ok(ProcessContext {
                transformer,
                buf,
                state: processor.into_state(),
                sender,
                shared_state,
            })
        })?;

        let upstream_result = upstream_task.await.map_err(ConnectionError::JoinError)??;
        let end_of_socket = upstream_result.1;
        if end_of_socket {
            break;
        }

        io_ctx = upstream_result.0;

        // process buf has been processed
        process_ctx.buf.clear();
        std::mem::swap(&mut process_ctx.buf, &mut io_ctx.other_buf);
    }

    Ok(())
}

#[derive(Debug)]
pub enum SendEvents {
    Data(Bytes),
    End,
}

impl std::fmt::Display for SendEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendEvents::Data(bytes) => {
                write!(f, "<< OUT {:?}", bytes)
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
    sender: UnboundedSender<SendEvents>,
    shared_state: SharedState,
}

struct UpstreamContext {
    socket: TcpStream,
    receiver: UnboundedReceiver<SendEvents>,
    other_buf: BytesMut,
    write_buf: BytesMut,
}
