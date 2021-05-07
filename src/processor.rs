use log::trace;
use tokio::{spawn, sync::mpsc};

use crate::{
    connection::SendEvents,
    resp::read::{RespEventsVisitor, VisitorResult},
};
use std::{
    fmt::{self, Debug},
    mem::replace,
};

pub enum ProcessorError {
    UnknownCommand,
}

impl fmt::Debug for ProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "processor error")
    }
}

#[derive(Debug)]
struct ArrayData {
    remaining: usize,
    values: Vec<ProcessorState>,
}

#[derive(Debug)]
pub enum ProcessorState {
    NoState,
    String(Vec<u8>),
    Integer(isize),
    BulkString(Option<Vec<u8>>),
    Array(Option<ArrayData>),
}

impl std::fmt::Display for ProcessorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorState::NoState => {
                write!(f, "no state")
            }
            ProcessorState::String(string) => {
                let string = String::from_utf8(string.clone()).map_err(|_| std::fmt::Error {})?;
                write!(f, "\"+{}\"", string)
            }
            ProcessorState::Integer(integer) => {
                write!(f, ":{}", integer)
            }
            ProcessorState::BulkString(maybe_string) => match maybe_string {
                Some(string) => {
                    let string =
                        String::from_utf8(string.clone()).map_err(|_| std::fmt::Error {})?;
                    write!(f, "\"${}\"", string)
                }
                None => {
                    write!(f, "nil string")
                }
            },
            ProcessorState::Array(maybe_array) => match maybe_array {
                Some(array) => {
                    write!(f, "* (remaining: {}) [", array.remaining)?;
                    for (i, value) in array.values.iter().enumerate() {
                        std::fmt::Display::fmt(value, f)?;
                        if i + 1 < array.values.len() {
                            write!(f, ", ")?;
                        }
                    }

                    write!(f, "]")
                }
                None => {
                    write!(f, "nil array")
                }
            },
        }
    }
}

enum Events {
    String(Vec<u8>),
    Integer(isize),
    BulkString(Option<Vec<u8>>),
    Array(Option<usize>),
}

pub struct Processor<'a> {
    sender: &'a mpsc::Sender<SendEvents>,
    state: ProcessorState,
}

impl<'a> Processor<'a> {
    pub fn new(sender: &'a mpsc::Sender<SendEvents>, state: ProcessorState) -> Self {
        Self { sender, state }
    }

    // DONT DELETE: is this how async closures may work? a closure return a future (async move)...
    // fn run<
    //     T: Future<Output = std::result::Result<(), SendError<SendEvents>>> + Send + 'static,
    //     F: FnOnce(mpsc::Sender<SendEvents>) -> T,
    // >(
    //     &mut self,
    //     f: F,
    // ) {
    //     let sender = self.sender.clone();

    //     let _ = tokio::spawn(f(sender));
    // }
    // self.run(|sender| async move {
    //     sender.send(event).await?;
    //     Ok(())
    // });

    fn send_cmd(&mut self, event: SendEvents) {
        // let sender = self.sender.clone();
        // spawn(async move { sender.send(event).await });

        self.sender.blocking_send(event).unwrap();
    }

    pub fn into_state(self) -> ProcessorState {
        self.state
    }

    fn process_event(&mut self, event: Events) {
        if Self::append_state(&mut self.state, event) {
            let state = replace(&mut self.state, ProcessorState::NoState);
            self.process_frame(state);
        }
    }

    fn process_frame(&mut self, state: ProcessorState) {
        trace!("processing frame: {}", &state);

        match state {
            ProcessorState::Array(Some(ArrayData { mut values, .. })) => {
                let mut drain = values.drain(..);

                match (drain.next(), drain.next(), drain.next(), drain.next()) {
                    (Some(ProcessorState::BulkString(Some(cmd))), None, None, None) => {
                        if cmd.eq_ignore_ascii_case(b"info") {
                            self.send_cmd(SendEvents::InfoCommand);
                        }
                    }
                    (
                        Some(ProcessorState::BulkString(Some(cmd))),
                        Some(ProcessorState::BulkString(Some(p1_str))),
                        None,
                        None,
                    ) => {
                        if cmd.eq_ignore_ascii_case(b"get") {
                            self.send_cmd(SendEvents::GetCommand(p1_str));
                        }
                    }
                    (
                        Some(ProcessorState::BulkString(Some(cmd))),
                        Some(ProcessorState::BulkString(Some(p1_str))),
                        Some(ProcessorState::BulkString(Some(p2_str))),
                        None,
                    ) => {
                        if cmd.eq_ignore_ascii_case(b"set") {
                            self.send_cmd(SendEvents::SetCommand(p1_str, p2_str));
                        }
                    }

                    _ => {}
                }
            }
            _ => todo!(),
        }
    }

    fn append_state(state: &mut ProcessorState, event: Events) -> bool {
        match state {
            ProcessorState::NoState => match event {
                Events::String(string) => {
                    *state = ProcessorState::String(string);
                    true
                }
                Events::BulkString(string) => {
                    *state = ProcessorState::BulkString(string);
                    true
                }
                Events::Array(array) => match array {
                    Some(len) => {
                        *state = ProcessorState::Array(Some(ArrayData {
                            remaining: len,
                            values: vec![],
                        }));
                        false
                    }
                    None => {
                        *state = ProcessorState::Array(None);
                        true
                    }
                },
                _ => false,
            },
            ProcessorState::Array(Some(ArrayData { remaining, values })) => {
                assert!(*remaining >= 1);

                match last_unexhausted_array(values) {
                    Some(array_data) => Self::append_state(array_data, event),
                    None => {
                        let elem = match event {
                            Events::String(string) => ProcessorState::String(string),
                            Events::BulkString(string) => ProcessorState::BulkString(string),
                            Events::Array(array) => match array {
                                Some(len) => ProcessorState::Array(Some(ArrayData {
                                    remaining: len,
                                    values: Vec::with_capacity(len),
                                })),
                                None => ProcessorState::Array(None),
                            },
                            _ => todo!(),
                        };

                        values.push(elem);
                        *remaining -= 1;

                        *remaining == 0
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}

impl<'a> RespEventsVisitor<ProcessorError> for Processor<'a> {
    fn on_string(&mut self, string: Vec<u8>) -> VisitorResult<ProcessorError> {
        self.process_event(Events::String(string));
        Ok(())
    }

    fn on_error(&mut self, _error: Vec<u8>) -> VisitorResult<ProcessorError> {
        Ok(())
    }

    fn on_integer(&mut self, integer: isize) -> VisitorResult<ProcessorError> {
        self.process_event(Events::Integer(integer));
        Ok(())
    }

    fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) -> VisitorResult<ProcessorError> {
        self.process_event(Events::BulkString(bulk_string));
        Ok(())
    }

    fn on_array(&mut self, length: Option<usize>) -> VisitorResult<ProcessorError> {
        self.process_event(Events::Array(length));

        Ok(())
    }
}

fn last_unexhausted_array<'a>(
    values: &'a mut Vec<ProcessorState>,
) -> Option<&'a mut ProcessorState> {
    let last = values.last_mut()?;

    match last {
        ProcessorState::Array(Some(ArrayData { remaining, .. })) if *remaining >= 1 => Some(last),
        _ => None,
    }
}
