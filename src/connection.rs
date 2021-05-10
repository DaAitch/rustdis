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
}

type Result<T> = std::result::Result<T, ConnectionError>;

pub async fn handle_connection(socket: TcpStream, shared_state: SharedState) -> Result<()> {
    let (sender, receiver) = mpsc::unbounded_channel::<SendEvents>();

    let mut process_ctx = ProcessContext {
        transformer: RespEventsTransformer::new(),
        socket_read_buf: BytesMut::with_capacity(65536), // which number makes sense?
        state: ProcessorState::NoState,
        sender,
        shared_state,
    };
    let mut io_ctx = UpstreamContext {
        socket,
        receiver,
        socket_read_backbuf: BytesMut::with_capacity(65536),
        write_buf: BytesMut::with_capacity(65536),
    };

    loop {
        if !process_ctx.socket_read_buf.has_remaining() {
            let read_len = io_ctx
                .socket
                .read_buf(&mut process_ctx.socket_read_buf)
                .await
                .map_err(|err| ConnectionError::Io(err))?;

            trace!("read {} bytes", read_len);

            if read_len == 0 {
                break;
            }

            debug!(target: "payload", ">> IN  {:?}", &process_ctx.socket_read_buf);
        } else {
            trace!("buf not empty, so process it first");
        }

        let upstream_task = spawn(async move {
            let end_of_socket = loop {
                select! {
                    Some(event) = io_ctx.receiver.recv() => {
                        match event {
                            SendEvents::Data(bytes) => {
                                trace!("data event {} bytes", bytes.remaining());
                                write_or_buffer(&mut io_ctx.socket, &mut io_ctx.write_buf, &bytes).await?;
                            }
                            SendEvents::End(bytes) => {
                                trace!("end event {} bytes", bytes.remaining());
                                write_buffer(&mut io_ctx.socket, &mut io_ctx.write_buf, &bytes).await?;
                                break false;
                            }
                        }
                    },
                    read_buf_result = io_ctx.socket.read_buf(&mut io_ctx.socket_read_backbuf) => {
                        let read_len = read_buf_result.map_err(ConnectionError::Io)?;

                        trace!("2nd phase read event {} bytes", read_len);

                        if read_len == 0 {
                            break true;
                        }
                    }
                }
            };

            if end_of_socket {
                trace!("draining events");
                while let Some(event) = io_ctx.receiver.recv().await {
                    match event {
                        SendEvents::Data(bytes) => {
                            trace!("data event {} bytes", bytes.remaining());
                            write_or_buffer(&mut io_ctx.socket, &mut io_ctx.write_buf, &bytes)
                                .await?;
                        }
                        SendEvents::End(bytes) => {
                            trace!("end event {} bytes", bytes.remaining());
                            write_buffer(&mut io_ctx.socket, &mut io_ctx.write_buf, &bytes).await?;
                            io_ctx.socket.flush().await.map_err(ConnectionError::Io)?;
                            break;
                        }
                    }
                }
            }

            Ok((io_ctx, end_of_socket))
        });

        process_ctx = block_in_place(move || {
            let ProcessContext {
                mut transformer,
                socket_read_buf: mut process_buf,
                state,
                sender,
                shared_state,
            } = process_ctx;

            let mut processor = Processor::new(&sender, &shared_state, state);
            transformer
                .process(&mut processor, &mut process_buf)
                .map_err(ConnectionError::Resp)?;

            processor.flush_and_end();

            debug!(target: "profiling", "end processing task");

            Ok(ProcessContext {
                transformer,
                socket_read_buf: process_buf,
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
        process_ctx.socket_read_buf.clear();
        std::mem::swap(
            &mut process_ctx.socket_read_buf,
            &mut io_ctx.socket_read_backbuf,
        );
    }

    Ok(())
}

#[derive(Debug)]
pub enum SendEvents {
    Data(Bytes),
    End(Bytes),
}

impl std::fmt::Display for SendEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendEvents::Data(bytes) => {
                write!(f, "<< OUT {:?}", bytes)
            }
            SendEvents::End(bytes) => {
                write!(f, "<< END {:?}", bytes)
            }
        }
    }
}

struct ProcessContext {
    transformer: RespEventsTransformer,
    socket_read_buf: BytesMut,
    state: ProcessorState,
    sender: UnboundedSender<SendEvents>,
    shared_state: SharedState,
}

struct UpstreamContext {
    socket: TcpStream,
    receiver: UnboundedReceiver<SendEvents>,
    socket_read_backbuf: BytesMut,
    write_buf: BytesMut,
}

async fn write_or_buffer(
    socket: &mut TcpStream,
    write_buf: &mut BytesMut,
    bytes: &Bytes,
) -> Result<()> {
    if write_buf.has_remaining() && write_buf.remaining_mut() < bytes.remaining() {
        // check if bytes larger than capacity, then directly write?

        trace!("will write {} bytes", write_buf.remaining());
        socket
            .write_all(&write_buf)
            .await
            .map_err(ConnectionError::Io)?;

        write_buf.clear();
    }

    write_buf.extend(bytes);
    Ok(())
}

async fn write_buffer(
    socket: &mut TcpStream,
    write_buf: &mut BytesMut,
    bytes: &Bytes,
) -> Result<()> {
    write_buf.extend(bytes);

    trace!("will write {} bytes", write_buf.remaining());
    socket
        .write_all(&write_buf)
        .await
        .map_err(ConnectionError::Io)?;

    write_buf.clear();

    // and flush?
    // io_ctx.socket.flush().await.map_err(ConnectionError::Io)?;
    Ok(())
}
