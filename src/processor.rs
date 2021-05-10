use bytes::{BufMut, BytesMut};
use log::{debug, trace};
use tokio::sync::mpsc;

use crate::{
    connection::SendEvents,
    resp::read::{RespEventsVisitor, VisitorResult},
    SharedState,
};
use std::{
    fmt::{self, Debug, Write},
    mem::replace,
};

pub enum ProcessorError {}

impl fmt::Debug for ProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "processor error")
    }
}

#[derive(Debug)]
pub struct ArrayData {
    remaining: usize,
    values: Vec<ProcessorState>,
}

#[derive(Debug)]
pub enum ProcessorState {
    NoState,
    String(Vec<u8>),
    // Integer(isize),
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
            // ProcessorState::Integer(integer) => {
            //     write!(f, ":{}", integer)
            // }
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
    sender: &'a mpsc::UnboundedSender<SendEvents>,
    shared_state: &'a SharedState,
    state: ProcessorState,
    buffer: BytesMut,
}

impl<'a> Processor<'a> {
    pub fn new(
        sender: &'a mpsc::UnboundedSender<SendEvents>,
        shared_state: &'a SharedState,
        state: ProcessorState,
    ) -> Self {
        Self {
            sender,
            shared_state,
            state,
            buffer: BytesMut::new(),
        }
    }

    fn send_cmd(&mut self, event: SendEvents) {
        self.sender.send(event).expect("receiver is never closed");
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
                            self.write_buffer_fixed(b"$2\r\nOK\r\n")
                        }
                    }
                    (
                        Some(ProcessorState::BulkString(Some(cmd))),
                        Some(ProcessorState::BulkString(Some(p1_str))),
                        None,
                        None,
                    ) => {
                        if cmd.eq_ignore_ascii_case(b"get") {
                            match self.shared_state.lock().unwrap().get(&p1_str) {
                                Some(value) => {
                                    self.prepare_write_buffer(value.len() + 26); // 21 max size u64 in dec, + 5 ("$\r\n\r\n")

                                    self.buffer.put_u8(b'$');
                                    write!(self.buffer, "{}", value.len()).unwrap();
                                    self.buffer.put_u8(b'\r');
                                    self.buffer.put_u8(b'\n');
                                    self.buffer.extend(value);
                                    self.buffer.put_u8(b'\r');
                                    self.buffer.put_u8(b'\n');
                                }
                                None => {
                                    self.write_buffer_fixed(b"$-1\r\n");
                                }
                            }
                        }
                    }
                    (
                        Some(ProcessorState::BulkString(Some(cmd))),
                        Some(ProcessorState::BulkString(Some(p1_str))),
                        Some(ProcessorState::BulkString(Some(p2_str))),
                        None,
                    ) => {
                        if cmd.eq_ignore_ascii_case(b"set") {
                            self.write_buffer_fixed(b"$2\r\nOK\r\n");
                            self.shared_state.lock().unwrap().insert(p1_str, p2_str);
                        }
                    }
                    _ => {}
                }
            }
            _ => unimplemented!(),
        }
    }

    fn write_buffer_fixed(&mut self, bytes: &[u8]) {
        self.prepare_write_buffer(bytes.len());
        self.buffer.extend(bytes);
    }

    fn prepare_write_buffer(&mut self, approx_size: usize) {
        if self.buffer.remaining_mut() < approx_size {
            if !self.buffer.is_empty() {
                self.flush();
            }

            self.buffer.reserve(65536.max(approx_size));
        }
    }

    fn flush(&mut self) {
        let bytes = std::mem::replace(&mut self.buffer, BytesMut::new());
        self.send_cmd(SendEvents::Data(bytes.freeze()));
    }

    pub fn flush_and_end(&mut self) {
        let bytes = std::mem::replace(&mut self.buffer, BytesMut::new());
        self.send_cmd(SendEvents::End(bytes.freeze()));
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
                            values: Vec::with_capacity(len),
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
                            _ => unimplemented!(),
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
