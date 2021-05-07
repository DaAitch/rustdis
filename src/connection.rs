use crate::processor::{Processor, ProcessorError, ProcessorState};
use crate::{
    resp::read::{RespError, RespEventsTransformer},
    SharedState,
};
use bytes::{Bytes, BytesMut};
use log::{debug, trace};
use tokio::{
    io::AsyncReadExt,
    spawn,
    sync::mpsc::{Receiver, Sender},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
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
    let (sender, receiver) = mpsc::channel::<SendEvents>(10000); // which number makes sense?

    let mut process_ctx = ProcessContext {
        transformer: RespEventsTransformer::new(),
        buf: BytesMut::with_capacity(65536), // which number makes sense?
        state: ProcessorState::NoState,
        sender,
        shared_state,
    };
    let mut io_ctx = UpstreamContext { socket, receiver };

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
                shared_state,
            } = process_ctx;
            let mut processor = Processor::new(&sender, &shared_state, state);
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
                    shared_state,
                })
            }))
        });

        let upstream_task = spawn(async move {
            while let Some(event) = io_ctx.receiver.recv().await {
                trace!("upstream event: {}", &event);

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
    sender: Sender<SendEvents>,
    shared_state: SharedState,
}

struct UpstreamContext {
    socket: TcpStream,
    receiver: Receiver<SendEvents>,
}
