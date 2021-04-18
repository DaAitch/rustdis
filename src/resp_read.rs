use std::{error, fmt};

use bytes::{Buf, BytesMut};
use futures::Stream;

const B_CR: u8 = b'\r';
const B_LF: u8 = b'\n';
const B_0: u8 = b'0';
const B_1: u8 = b'1';
const B_9: u8 = b'9';
const B_PLUS: u8 = b'+';
const B_MINUS: u8 = b'-';
const B_COLLON: u8 = b':';
const B_DOLLAR: u8 = b'$';
const B_ASTERISK: u8 = b'*';

const NO_STATE: NoState = NoState {};

macro_rules! impl_step {
    ($name:ident, $state_type:ty, $expected:expr, $state:expr, $next:expr) => {
        fn $name<T: RespEventsVisitor>(
            &mut self,
            state: $state_type,
            visitor: &mut T,
            buf: &mut BytesMut,
        ) -> Result<()> {
            if buf.remaining() > 0 {
                let ch = buf.get_u8();
                if ch == $expected {
                    $next(self, state, visitor, buf)
                } else {
                    Err(RespError::UnexpectedChar(ch))
                }
            } else {
                self.state = Some($state(state));
                Ok(())
            }
        }
    };
}

macro_rules! entry {
    ($state:expr, $this:expr, $visitor:expr, $buf:expr, $( $state_enum:ident => $func:ident, )*) => {
        match $state {
            $(
                Some(State::$state_enum(state)) => Self::$func($this, state, $visitor, $buf),
            )*
            None => Err(RespError::UnrecoverableErrorState),
        }
    };
}

pub struct RespEventsTransformer {
    state: Option<State>,
}

impl RespEventsTransformer {
    pub fn new() -> Self {
        Self {
            state: Some(State::Start(NO_STATE)),
        }
    }

    pub fn process<T: RespEventsVisitor>(
        &mut self,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        entry![self.state.take(), self, visitor, buf,
            Start => buf_process_start,
            String => buf_process_string,
            StringLf => buf_process_string_lf,
            Error => buf_process_error,
            ErrorLf => buf_process_error_lf,
            Integer => buf_process_integer,
            IntegerLf => buf_process_integer_lf,
            BulkStringLen => buf_process_bulk_string_len,
            BulkStringLenNbsOne => buf_process_bulk_string_len_nbs_one,
            BulkStringLenNbsCr => buf_process_bulk_string_len_nbs_cr,
            BulkStringLenNbsLf => buf_process_bulk_string_len_nbs_lf,
            BulkStringData => buf_process_bulk_string_data,
            BulkStringDataCr => buf_process_bulk_string_data_cr,
            BulkStringDataLf => buf_process_bulk_string_data_lf,
            ArrayLen => buf_process_array_len,
            ArrayLenNaOne => buf_process_array_len_na_one,
            ArrayLenNaCr => buf_process_array_len_na_cr,
            ArrayLenNaLf => buf_process_array_len_na_lf,
            ArrayLenLf => buf_process_array_len_lf,
        ]
    }

    fn buf_process_start<T: RespEventsVisitor>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        if buf.remaining() > 0 {
            match buf.get_u8() {
                B_PLUS => self.buf_process_string(StringState { string: vec![] }, visitor, buf),
                B_MINUS => self.buf_process_error(ErrorState { error: vec![] }, visitor, buf),
                B_COLLON => self.buf_process_integer(
                    IntegerState {
                        integer: None,
                        is_negative: false,
                    },
                    visitor,
                    buf,
                ),
                B_DOLLAR => self.buf_process_bulk_string_len(
                    BulkStringLenState { length: None },
                    visitor,
                    buf,
                ),
                B_ASTERISK => {
                    self.buf_process_array_len(ArrayLenState { length: None }, visitor, buf)
                }
                unexpected_ch => Err(RespError::ReadingTypeError(unexpected_ch)),
            }
        } else {
            self.state = Some(State::Start(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_string<T: RespEventsVisitor>(
        &mut self,
        mut st: StringState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.string.extend(&buf[..index]);
                buf.advance(index + 1);

                visitor.on_string(st.string);

                self.buf_process_string_lf(NO_STATE, visitor, buf)
            }
            None => {
                st.string.extend(buf.iter());
                self.state = Some(State::String(st));
                Ok(())
            }
        }
    }

    fn buf_process_string_lf<T: RespEventsVisitor>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::StringLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_error<T: RespEventsVisitor>(
        &mut self,
        mut st: ErrorState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.error.extend(&buf[..index]);
                buf.advance(index + 1);

                visitor.on_error(st.error);

                self.buf_process_string_lf(NO_STATE, visitor, buf)
            }
            None => {
                st.error.extend(buf.iter());
                self.state = Some(State::Error(st));
                Ok(())
            }
        }
    }

    fn buf_process_error_lf<T: RespEventsVisitor>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::ErrorLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_integer<T: RespEventsVisitor>(
        &mut self,
        mut st: IntegerState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        while buf.remaining() > 0 {
            // TODO: check integer overflow e.g.: `:999...999`
            match (buf.get_u8(), st.integer.as_mut()) {
                (str_digit @ B_0..=B_9, Some(integer)) => {
                    *integer = (10 * *integer) + digit_to_usize_unchecked(str_digit);
                }
                (str_digit @ B_0..=B_9, None) => {
                    st.integer = Some(digit_to_usize_unchecked(str_digit));
                }
                (B_MINUS, None) => {
                    st.is_negative = true;
                }
                (B_CR, Some(integer)) => {
                    let integer = if st.is_negative {
                        -(*integer as isize)
                    } else {
                        *integer as isize
                    };

                    visitor.on_integer(integer);
                    return self.buf_process_integer_lf(NO_STATE, visitor, buf);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::Integer(st));
        Ok(())
    }

    fn buf_process_integer_lf<T: RespEventsVisitor>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::IntegerLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_bulk_string_len<T: RespEventsVisitor>(
        &mut self,
        mut st: BulkStringLenState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        while buf.remaining() > 0 {
            let ch = buf.get_u8();
            match (ch, st.length.as_mut()) {
                (str_digit @ B_0..=B_9, Some(length)) => {
                    *length = (10 * *length) + digit_to_usize_unchecked(str_digit);
                }
                (str_digit @ B_0..=B_9, None) => {
                    st.length = Some(digit_to_usize_unchecked(str_digit))
                }
                (B_CR, Some(length)) => {
                    return self.buf_process_bulk_string_len_lf(
                        BulkStringDataState {
                            remaining: *length,
                            string: vec![],
                        },
                        visitor,
                        buf,
                    );
                }
                (B_MINUS, None) => {
                    visitor.on_bulk_string(None);
                    return self.buf_process_bulk_string_len_nbs_one(NO_STATE, visitor, buf);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::BulkStringLen(st));
        Ok(())
    }

    impl_step!(
        buf_process_bulk_string_len_nbs_one,
        NoState,
        B_1,
        State::BulkStringLenNbsOne,
        Self::buf_process_bulk_string_len_nbs_cr
    );

    impl_step!(
        buf_process_bulk_string_len_nbs_cr,
        NoState,
        B_CR,
        State::BulkStringLenNbsCr,
        Self::buf_process_bulk_string_len_nbs_lf
    );

    impl_step!(
        buf_process_bulk_string_len_nbs_lf,
        NoState,
        B_LF,
        State::BulkStringLenNbsLf,
        Self::buf_process_start
    );

    impl_step!(
        buf_process_bulk_string_len_lf,
        BulkStringDataState,
        B_LF,
        State::BulkStringData,
        Self::buf_process_bulk_string_data
    );

    fn buf_process_bulk_string_data<T: RespEventsVisitor>(
        &mut self,
        mut st: BulkStringDataState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        if buf.remaining() >= st.remaining {
            let read = st.remaining;
            st.string.extend(&buf[..read]);
            buf.advance(read);

            visitor.on_bulk_string(Some(st.string));

            self.buf_process_bulk_string_data_cr(NO_STATE, visitor, buf)
        } else {
            let read = buf.remaining();
            st.string.extend(&buf[..read]);
            buf.advance(read);

            self.state = Some(State::BulkStringData(st));
            Ok(())
        }
    }

    impl_step!(
        buf_process_bulk_string_data_cr,
        NoState,
        B_CR,
        State::BulkStringDataCr,
        Self::buf_process_bulk_string_data_lf
    );

    impl_step!(
        buf_process_bulk_string_data_lf,
        NoState,
        B_LF,
        State::BulkStringDataLf,
        Self::buf_process_start
    );

    fn buf_process_array_len<T: RespEventsVisitor>(
        &mut self,
        mut st: ArrayLenState,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<()> {
        while buf.remaining() > 0 {
            let ch = buf.get_u8();
            match (ch, st.length.as_mut()) {
                (str_digit @ B_0..=B_9, Some(length)) => {
                    *length = (10 * *length) + digit_to_usize_unchecked(str_digit);
                }
                (str_digit @ B_0..=B_9, None) => {
                    st.length = Some(digit_to_usize_unchecked(str_digit))
                }
                (B_CR, Some(length)) => {
                    visitor.on_array(Some(*length));

                    return self.buf_process_array_len_lf(NoState {}, visitor, buf);
                }
                (B_MINUS, None) => {
                    visitor.on_array(None);
                    return self.buf_process_array_len_na_one(NO_STATE, visitor, buf);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::ArrayLen(st));
        Ok(())
    }

    impl_step!(
        buf_process_array_len_na_one,
        NoState,
        B_1,
        State::ArrayLenNaOne,
        Self::buf_process_array_len_na_cr
    );

    impl_step!(
        buf_process_array_len_na_cr,
        NoState,
        B_CR,
        State::ArrayLenNaCr,
        Self::buf_process_array_len_na_lf
    );

    impl_step!(
        buf_process_array_len_na_lf,
        NoState,
        B_LF,
        State::ArrayLenNaLf,
        Self::buf_process_start
    );

    impl_step!(
        buf_process_array_len_lf,
        NoState,
        B_LF,
        State::ArrayLenLf,
        Self::buf_process_start
    );
}

fn digit_to_usize_unchecked(ch: u8) -> usize {
    (ch - B_0) as usize
}

/// "find" predicate for `buf.iter().enumerate().find(find_cr)`
fn find_cr(item: &(usize, &u8)) -> bool {
    *item.1 == B_CR
}

struct NoState {}

struct StringState {
    string: Vec<u8>,
}

struct ErrorState {
    error: Vec<u8>,
}

struct IntegerState {
    integer: Option<usize>,
    is_negative: bool,
}

struct BulkStringLenState {
    length: Option<usize>,
}

struct BulkStringDataState {
    remaining: usize,
    string: Vec<u8>,
}

struct ArrayLenState {
    length: Option<usize>,
}

enum State {
    Start(NoState),

    // string
    String(StringState),
    StringLf(NoState),

    // error
    Error(ErrorState),
    ErrorLf(NoState),

    // integer
    Integer(IntegerState),
    IntegerLf(NoState),

    // bulk string
    BulkStringLen(BulkStringLenState),
    BulkStringLenNbsOne(NoState),
    BulkStringLenNbsCr(NoState),
    BulkStringLenNbsLf(NoState),
    BulkStringData(BulkStringDataState),
    BulkStringDataCr(NoState),
    BulkStringDataLf(NoState),

    // array
    ArrayLen(ArrayLenState),
    ArrayLenNaOne(NoState),
    ArrayLenNaCr(NoState),
    ArrayLenNaLf(NoState),
    ArrayLenLf(NoState),
}

type Result<T> = std::result::Result<T, RespError>;

pub enum RespError {
    UnrecoverableErrorState,
    ReadingTypeError(u8),
    ExpectedLf(u8),
    UnexpectedChar(u8),
}

impl error::Error for RespError {}

impl fmt::Debug for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespError::UnrecoverableErrorState => {
                writeln!(f, "UnrecoverableErrorState")
            }
            RespError::ReadingTypeError(ch) => {
                writeln!(f, "ReadingTypeError found u8 '{}'", *ch as char)
            }
            RespError::ExpectedLf(ch) => {
                writeln!(f, "ExpectedLf found u8 '{}'", *ch as char)
            }
            RespError::UnexpectedChar(ch) => {
                writeln!(f, "UnexpectedChar found u8 '{}'", *ch as char)
            }
        }
    }
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self, f)
    }
}

pub trait RespEventsVisitor {
    fn on_string(&mut self, string: Vec<u8>);
    fn on_error(&mut self, error: Vec<u8>);
    fn on_integer(&mut self, integer: isize);
    fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>);
    fn on_array(&mut self, length: Option<usize>);
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    #[test]
    fn test_types() {
        let mut collector = EventCollector::new();
        let mut resp = RespEventsTransformer::new();

        resp.process(&mut collector, &mut buf("")).unwrap();
        resp.process(&mut collector, &mut buf("+a string\r\n"))
            .unwrap();
        resp.process(&mut collector, &mut buf("+\r\n")).unwrap();
        resp.process(&mut collector, &mut buf("-an error\r\n"))
            .unwrap();
        resp.process(&mut collector, &mut buf("-\r\n")).unwrap();
        resp.process(&mut collector, &mut buf(":123456\r\n"))
            .unwrap();
        resp.process(&mut collector, &mut buf(":-4321\r\n"))
            .unwrap();
        resp.process(&mut collector, &mut buf(":0\r\n")).unwrap();
        resp.process(&mut collector, &mut buf(":-0\r\n")).unwrap();
        resp.process(&mut collector, &mut buf("$21\r\nThis is a bulk string\r\n"))
            .unwrap();
        resp.process(&mut collector, &mut buf("$-1\r\n")).unwrap();
        resp.process(&mut collector, &mut buf("*-1\r\n")).unwrap();
        resp.process(&mut collector, &mut buf("*0\r\n")).unwrap();
        resp.process(&mut collector, &mut buf("*3\r\n")).unwrap();

        assert_eq!(collector.strings, vec![vec_u8(b"a string"), vec_u8(b"")]);
        assert_eq!(collector.errors, vec![vec_u8(b"an error"), vec_u8(b"")]);
        assert_eq!(collector.integers, vec![123456, -4321, 0, 0]);
        assert_eq!(
            collector.bulk_strings,
            vec![Some(vec_u8(b"This is a bulk string")), None]
        );
        assert_eq!(collector.arrays, vec![None, Some(0), Some(3)]);
    }

    fn buf(string: &str) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.write_str(string).unwrap();

        buf
    }

    fn vec_u8(bytes: &[u8]) -> Vec<u8> {
        let mut v = Vec::<u8>::new();
        v.extend(bytes.iter());
        v
    }

    #[derive(Debug)]
    struct EventCollector {
        strings: Vec<Vec<u8>>,
        integers: Vec<isize>,
        errors: Vec<Vec<u8>>,
        bulk_strings: Vec<Option<Vec<u8>>>,
        arrays: Vec<Option<usize>>,
    }

    impl EventCollector {
        fn new() -> Self {
            Self {
                strings: vec![],
                integers: vec![],
                errors: vec![],
                bulk_strings: vec![],
                arrays: vec![],
            }
        }
    }

    impl RespEventsVisitor for EventCollector {
        fn on_string(&mut self, string: Vec<u8>) {
            self.strings.push(string);
        }

        fn on_integer(&mut self, integer: isize) {
            self.integers.push(integer);
        }

        fn on_error(&mut self, error: Vec<u8>) {
            self.errors.push(error);
        }

        fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) {
            self.bulk_strings.push(bulk_string);
        }

        fn on_array(&mut self, length: Option<usize>) {
            self.arrays.push(length);
        }
    }
}
