use std::{error, fmt};

use bytes::{Buf, BytesMut};

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
        fn $name(&mut self, mut buf: BytesMut, state: $state_type) -> Result<()> {
            if buf.remaining() > 0 {
                let ch = buf.get_u8();
                if ch == $expected {
                    $next(self, buf, state)
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
    ($state:expr, $this:expr, $buf:expr, $( $state_enum:ident => $func:ident, )*) => {
        match $state {
            $(
                Some(State::$state_enum(state)) => Self::$func($this, $buf, state),
            )*
            None => Err(RespError::UnrecoverableErrorState),
        }
    };
}

pub struct RespEventsTransformer<T: RespEventsVisitor> {
    visitor: T,
    state: Option<State>,
}

impl<T: RespEventsVisitor> RespEventsTransformer<T> {
    pub fn new(visitor: T) -> Self {
        Self {
            visitor,
            state: Some(State::Start(NO_STATE)),
        }
    }

    pub fn visitor(&self) -> &T {
        &self.visitor
    }

    pub fn visitor_mut(&mut self) -> &mut T {
        &mut self.visitor
    }

    pub fn process(&mut self, buf: BytesMut) -> Result<()> {
        entry![self.state.take(), self, buf,
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

    fn buf_process_start(&mut self, mut buf: BytesMut, _: NoState) -> Result<()> {
        if buf.remaining() > 0 {
            match buf.get_u8() {
                B_PLUS => self.buf_process_string(buf, StringState { string: vec![] }),
                B_MINUS => self.buf_process_error(buf, ErrorState { error: vec![] }),
                B_COLLON => self.buf_process_integer(
                    buf,
                    IntegerState {
                        integer: None,
                        is_negative: false,
                    },
                ),
                B_DOLLAR => {
                    self.buf_process_bulk_string_len(buf, BulkStringLenState { length: None })
                }
                B_ASTERISK => self.buf_process_array_len(buf, ArrayLenState { length: None }),
                unexpected_ch => Err(RespError::ReadingTypeError(unexpected_ch)),
            }
        } else {
            self.state = Some(State::Start(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_string(&mut self, mut buf: BytesMut, mut st: StringState) -> Result<()> {
        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.string.extend(&buf[..index]);
                buf.advance(index + 1);

                self.visitor.on_string(st.string);

                self.buf_process_string_lf(buf, NO_STATE)
            }
            None => {
                st.string.extend(buf);
                self.state = Some(State::String(st));
                Ok(())
            }
        }
    }

    fn buf_process_string_lf(&mut self, mut buf: BytesMut, _: NoState) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(buf, NO_STATE)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::StringLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_error(&mut self, mut buf: BytesMut, mut st: ErrorState) -> Result<()> {
        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.error.extend(&buf[..index]);
                buf.advance(index + 1);

                self.visitor.on_error(st.error);

                self.buf_process_string_lf(buf, NO_STATE)
            }
            None => {
                st.error.extend(buf);
                self.state = Some(State::Error(st));
                Ok(())
            }
        }
    }

    fn buf_process_error_lf(&mut self, mut buf: BytesMut, _: NoState) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(buf, NO_STATE)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::ErrorLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_integer(&mut self, mut buf: BytesMut, mut st: IntegerState) -> Result<()> {
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

                    self.visitor.on_integer(integer);
                    return self.buf_process_integer_lf(buf, NO_STATE);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::Integer(st));
        Ok(())
    }

    fn buf_process_integer_lf(&mut self, mut buf: BytesMut, _: NoState) -> Result<()> {
        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(buf, NO_STATE)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::IntegerLf(NO_STATE));
            Ok(())
        }
    }

    fn buf_process_bulk_string_len(
        &mut self,
        mut buf: BytesMut,
        mut st: BulkStringLenState,
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
                        buf,
                        BulkStringDataState {
                            remaining: *length,
                            string: vec![],
                        },
                    );
                }
                (B_MINUS, None) => {
                    self.visitor.on_bulk_string(None);
                    return self.buf_process_bulk_string_len_nbs_one(buf, NO_STATE);
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

    fn buf_process_bulk_string_data(
        &mut self,
        mut buf: BytesMut,
        mut st: BulkStringDataState,
    ) -> Result<()> {
        if buf.remaining() >= st.remaining {
            let read = st.remaining;
            st.string.extend(&buf[..read]);
            buf.advance(read);

            self.visitor.on_bulk_string(Some(st.string));

            self.buf_process_bulk_string_data_cr(buf, NO_STATE)
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

    fn buf_process_array_len(&mut self, mut buf: BytesMut, mut st: ArrayLenState) -> Result<()> {
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
                    self.visitor.on_array(Some(*length));

                    return self.buf_process_array_len_lf(buf, NoState {});
                }
                (B_MINUS, None) => {
                    self.visitor.on_array(None);
                    return self.buf_process_array_len_na_one(buf, NO_STATE);
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
        let collector = EventCollector::new();
        let mut resp = RespEventsTransformer::new(collector);

        process(&mut resp, "+a string\r\n");
        process(&mut resp, "+\r\n");
        process(&mut resp, "-an error\r\n");
        process(&mut resp, "-\r\n");
        process(&mut resp, ":123456\r\n");
        process(&mut resp, ":-4321\r\n");
        process(&mut resp, ":0\r\n");
        process(&mut resp, ":-0\r\n");
        process(&mut resp, "$21\r\nThis is a bulk string\r\n");
        process(&mut resp, "$-1\r\n");
        process(&mut resp, "*-1\r\n");
        process(&mut resp, "*0\r\n");
        process(&mut resp, "*3\r\n");

        let recv = resp.visitor();
        assert_eq!(recv.strings, vec![vec_u8(b"a string"), vec_u8(b"")]);
        assert_eq!(recv.errors, vec![vec_u8(b"an error"), vec_u8(b"")]);
        assert_eq!(recv.integers, vec![123456, -4321, 0, 0]);
        assert_eq!(
            recv.bulk_strings,
            vec![Some(vec_u8(b"This is a bulk string")), None]
        );
        assert_eq!(recv.arrays, vec![None, Some(0), Some(3)]);
    }

    fn process<T: RespEventsVisitor>(resp: &mut RespEventsTransformer<T>, string: &str) {
        resp.process(buf(string)).unwrap();
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
