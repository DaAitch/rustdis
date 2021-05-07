use super::{B_0, B_1, B_9, B_ASTERISK, B_COLLON, B_CR, B_DOLLAR, B_LF, B_MINUS, B_PLUS};
use bytes::{Buf, BytesMut};
use log::{debug, trace};
use std::{error, fmt};

const MAX_STACK_DEPTH: u32 = 1000;
const NO_STATE: NoState = NoState {};

macro_rules! impl_step {
    ($name:ident, $state_type:ty, $expected:expr, $state:expr, $next:expr) => {
        fn $name<VE, T: RespEventsVisitor<VE>>(
            &mut self,
            state: $state_type,
            visitor: &mut T,
            buf: &mut BytesMut,
            mut stack_frame: u32,
        ) -> Result<(), VE> {
            if !increment_stack_frame(&mut stack_frame) {
                self.state = Some($state(state));
                return Ok(());
            }

            if buf.remaining() > 0 {
                let ch = buf.get_u8();
                if ch == $expected {
                    $next(self, state, visitor, buf, stack_frame)
                } else {
                    Err(RespError::UnexpectedChar(ch))
                }
            } else {
                self.state = Some($state(state));

                debug_assert!(!buf.has_remaining());
                Ok(())
            }
        }
    };
}

macro_rules! entry {
    ($state:expr, $this:expr, $visitor:expr, $buf:expr, $( $state_enum:ident => $func:ident, )*) => {
        match $state {
            $(
                Some(State::$state_enum(state)) => Self::$func($this, state, $visitor, $buf, 1),
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

    pub fn process<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        visitor: &mut T,
        buf: &mut BytesMut,
    ) -> Result<(), VE> {
        while buf.has_remaining() {
            trace!("start process at state: {:?}", &self.state);

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
                BulkStringLenLf => buf_process_bulk_string_len_lf,
                BulkStringDataCr => buf_process_bulk_string_data_cr,
                BulkStringDataLf => buf_process_bulk_string_data_lf,
                ArrayLen => buf_process_array_len,
                ArrayLenNaOne => buf_process_array_len_na_one,
                ArrayLenNaCr => buf_process_array_len_na_cr,
                ArrayLenNaLf => buf_process_array_len_na_lf,
                ArrayLenLf => buf_process_array_len_lf,
            ]?
        }

        Ok(())
    }

    fn buf_process_start<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::Start(NO_STATE));
            return Ok(());
        }

        if buf.remaining() > 0 {
            match buf.get_u8() {
                B_PLUS => self.buf_process_string(
                    StringState { string: vec![] },
                    visitor,
                    buf,
                    stack_frame,
                ),
                B_MINUS => {
                    self.buf_process_error(ErrorState { error: vec![] }, visitor, buf, stack_frame)
                }
                B_COLLON => self.buf_process_integer(
                    IntegerState {
                        integer: None,
                        is_negative: false,
                    },
                    visitor,
                    buf,
                    stack_frame,
                ),
                B_DOLLAR => self.buf_process_bulk_string_len(
                    BulkStringLenState { length: None },
                    visitor,
                    buf,
                    stack_frame,
                ),
                B_ASTERISK => self.buf_process_array_len(
                    ArrayLenState { length: None },
                    visitor,
                    buf,
                    stack_frame,
                ),
                unexpected_ch => Err(RespError::ReadingTypeError(unexpected_ch)),
            }
        } else {
            self.state = Some(State::Start(NO_STATE));

            debug_assert!(!buf.has_remaining());
            Ok(())
        }
    }

    fn buf_process_string<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: StringState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::String(st));
            return Ok(());
        }

        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.string.extend(&buf[..index]);
                buf.advance(index + 1);

                visitor
                    .on_string(st.string)
                    .map_err(RespError::VisitorError)?;

                self.buf_process_string_lf(NO_STATE, visitor, buf, stack_frame)
            }
            None => {
                st.string.extend(buf.iter());
                self.state = Some(State::String(st));

                buf.clear();
                debug_assert!(!buf.has_remaining());
                Ok(())
            }
        }
    }

    fn buf_process_string_lf<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::StringLf(NO_STATE));
            return Ok(());
        }

        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf, stack_frame)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::StringLf(NO_STATE));

            debug_assert!(!buf.has_remaining());
            Ok(())
        }
    }

    fn buf_process_error<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: ErrorState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::Error(st));
            return Ok(());
        }

        match buf.as_ref().iter().enumerate().find(find_cr) {
            Some((index, _)) => {
                st.error.extend(&buf[..index]);
                buf.advance(index + 1);

                visitor
                    .on_error(st.error)
                    .map_err(RespError::VisitorError)?;

                self.buf_process_string_lf(NO_STATE, visitor, buf, stack_frame)
            }
            None => {
                st.error.extend(buf.iter());
                self.state = Some(State::Error(st));
                buf.clear();

                debug_assert!(!buf.has_remaining());
                Ok(())
            }
        }
    }

    fn buf_process_error_lf<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::ErrorLf(NO_STATE));
            return Ok(());
        }

        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf, stack_frame)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::ErrorLf(NO_STATE));

            debug_assert!(!buf.has_remaining());
            Ok(())
        }
    }

    fn buf_process_integer<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: IntegerState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::Integer(st));
            return Ok(());
        }

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

                    visitor
                        .on_integer(integer)
                        .map_err(RespError::VisitorError)?;
                    return self.buf_process_integer_lf(NO_STATE, visitor, buf, stack_frame);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::Integer(st));

        debug_assert!(!buf.has_remaining());
        Ok(())
    }

    fn buf_process_integer_lf<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        _: NoState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::IntegerLf(NO_STATE));
            return Ok(());
        }

        if buf.remaining() > 0 {
            let lf = buf.get_u8();
            if lf == B_LF {
                self.buf_process_start(NO_STATE, visitor, buf, stack_frame)
            } else {
                Err(RespError::ExpectedLf(lf))
            }
        } else {
            self.state = Some(State::IntegerLf(NO_STATE));

            debug_assert!(!buf.has_remaining());
            Ok(())
        }
    }

    fn buf_process_bulk_string_len<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: BulkStringLenState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::BulkStringLen(st));
            return Ok(());
        }

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
                            string: Vec::with_capacity(*length),
                        },
                        visitor,
                        buf,
                        stack_frame,
                    );
                }
                (B_MINUS, None) => {
                    visitor
                        .on_bulk_string(None)
                        .map_err(RespError::VisitorError)?;
                    return self.buf_process_bulk_string_len_nbs_one(
                        NO_STATE,
                        visitor,
                        buf,
                        stack_frame,
                    );
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::BulkStringLen(st));

        debug_assert!(!buf.has_remaining());
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
        State::BulkStringLenLf,
        Self::buf_process_bulk_string_data
    );

    fn buf_process_bulk_string_data<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: BulkStringDataState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::BulkStringData(st));
            return Ok(());
        }

        if buf.remaining() >= st.remaining {
            let read = st.remaining;
            st.string.extend(&buf[..read]);
            buf.advance(read);

            visitor
                .on_bulk_string(Some(st.string))
                .map_err(RespError::VisitorError)?;

            self.buf_process_bulk_string_data_cr(NO_STATE, visitor, buf, stack_frame)
        } else {
            let remaining = buf.remaining();
            st.string.extend(&buf[..]);
            buf.clear();
            st.remaining -= remaining;

            self.state = Some(State::BulkStringData(st));

            debug_assert!(!buf.has_remaining());
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

    fn buf_process_array_len<VE, T: RespEventsVisitor<VE>>(
        &mut self,
        mut st: ArrayLenState,
        visitor: &mut T,
        buf: &mut BytesMut,
        mut stack_frame: u32,
    ) -> Result<(), VE> {
        if !increment_stack_frame(&mut stack_frame) {
            self.state = Some(State::ArrayLen(st));
            return Ok(());
        }

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
                    visitor
                        .on_array(Some(*length))
                        .map_err(RespError::VisitorError)?;

                    return self.buf_process_array_len_lf(NoState {}, visitor, buf, stack_frame);
                }
                (B_MINUS, None) => {
                    visitor.on_array(None).map_err(RespError::VisitorError)?;
                    return self.buf_process_array_len_na_one(NO_STATE, visitor, buf, stack_frame);
                }
                (unexpected_ch, _) => {
                    return Err(RespError::UnexpectedChar(unexpected_ch));
                }
            }
        }

        self.state = Some(State::ArrayLen(st));

        debug_assert!(!buf.has_remaining());
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

fn increment_stack_frame(stack_frame: &mut u32) -> bool {
    *stack_frame = *stack_frame + 1;
    let can_continue = *stack_frame < MAX_STACK_DEPTH;

    if !can_continue {
        debug!("recommending stack unwind {}", MAX_STACK_DEPTH);
    }

    can_continue
}

#[derive(Debug)]
struct NoState {}

struct StringState {
    string: Vec<u8>,
}

impl std::fmt::Debug for StringState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StringState")
            .field("string", &String::from_utf8(self.string.clone()).unwrap())
            .finish()
    }
}

struct ErrorState {
    error: Vec<u8>,
}

impl std::fmt::Debug for ErrorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorState")
            .field("error", &String::from_utf8(self.error.clone()).unwrap())
            .finish()
    }
}

#[derive(Debug)]
struct IntegerState {
    integer: Option<usize>,
    is_negative: bool,
}

#[derive(Debug)]
struct BulkStringLenState {
    length: Option<usize>,
}

struct BulkStringDataState {
    remaining: usize,
    string: Vec<u8>,
}

impl std::fmt::Debug for BulkStringDataState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkStringDataState")
            .field("remaining", &self.remaining)
            .field("string", &String::from_utf8(self.string.clone()).unwrap())
            .finish()
    }
}

#[derive(Debug)]
struct ArrayLenState {
    length: Option<usize>,
}

#[derive(Debug)]
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
    BulkStringLenLf(BulkStringDataState),
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

type Result<T, VE> = std::result::Result<T, RespError<VE>>;

pub enum RespError<VE> {
    UnrecoverableErrorState,
    ReadingTypeError(u8),
    ExpectedLf(u8),
    UnexpectedChar(u8),
    VisitorError(VE),
}

impl<VE: error::Error> error::Error for RespError<VE> {}

impl<VE: fmt::Debug> fmt::Debug for RespError<VE> {
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
            RespError::VisitorError(err) => {
                writeln!(f, "Visitor error: {:?}", err)
            }
        }
    }
}

impl<VE: fmt::Debug> fmt::Display for RespError<VE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self, f)
    }
}

pub type VisitorResult<E> = std::result::Result<(), E>;

pub trait RespEventsVisitor<E = ()> {
    fn on_string(&mut self, string: Vec<u8>) -> VisitorResult<E>;
    fn on_error(&mut self, error: Vec<u8>) -> VisitorResult<E>;
    fn on_integer(&mut self, integer: isize) -> VisitorResult<E>;
    fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) -> VisitorResult<E>;
    fn on_array(&mut self, length: Option<usize>) -> VisitorResult<E>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use std::fmt::Write;

    #[test]
    fn test_simple_without_breaks() {
        test_scenario(&[
            "",
            "+a string\r\n",
            "+\r\n",
            "-an error\r\n",
            "-\r\n",
            ":123456\r\n",
            ":-4321\r\n",
            ":0\r\n",
            ":-0\r\n",
            "$21\r\nThis is a bulk string\r\n",
            "$-1\r\n",
            "*-1\r\n",
            "*0\r\n",
            "*3\r\n",
        ], "str: a string, str: , err: an error, err: , int: 123456, int: -4321, int: 0, int: 0, bstr: This is a bulk string, bstr: nil, arr: nil, arr: 0, arr: 3, ");
    }

    fn test_scenario(values: &[&str], expected_outcome: &str) {
        {
            let mut visitor = EventCollector::new();
            let mut resp = RespEventsTransformer::new();

            for value in values {
                resp.process(&mut visitor, &mut buf(value)).unwrap();
            }

            assert_eq!(visitor.out, expected_outcome);
        }

        {
            let mut visitor = EventCollector::new();
            let mut resp = RespEventsTransformer::new();

            for value in values {
                for byte in value.as_bytes() {
                    resp.process(&mut visitor, &mut buf_u8(*byte)).unwrap();
                }
            }

            assert_eq!(visitor.out, expected_outcome);
        }
    }

    fn buf(string: &str) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.write_str(string).unwrap();

        buf
    }

    fn buf_u8(byte: u8) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(byte);

        buf
    }

    #[derive(Debug)]
    struct EventCollector {
        out: BytesMut,
    }

    impl EventCollector {
        fn new() -> Self {
            Self {
                out: BytesMut::with_capacity(65536),
            }
        }
    }

    impl RespEventsVisitor for EventCollector {
        fn on_string(&mut self, string: Vec<u8>) -> VisitorResult<()> {
            let string = String::from_utf8(string).unwrap();
            write!(&mut self.out, "str: {}, ", &string).unwrap();
            Ok(())
        }

        fn on_integer(&mut self, integer: isize) -> VisitorResult<()> {
            write!(&mut self.out, "int: {}, ", integer).unwrap();
            Ok(())
        }

        fn on_error(&mut self, error: Vec<u8>) -> VisitorResult<()> {
            let error = String::from_utf8(error).unwrap();
            write!(&mut self.out, "err: {}, ", &error).unwrap();
            Ok(())
        }

        fn on_bulk_string(&mut self, bulk_string: Option<Vec<u8>>) -> VisitorResult<()> {
            match bulk_string {
                Some(bulk_string) => {
                    let bulk_string = String::from_utf8(bulk_string).unwrap();
                    write!(&mut self.out, "bstr: {}, ", &bulk_string).unwrap();
                }
                None => {
                    write!(&mut self.out, "bstr: nil, ").unwrap();
                }
            }
            Ok(())
        }

        fn on_array(&mut self, length: Option<usize>) -> VisitorResult<()> {
            match length {
                Some(length) => {
                    write!(&mut self.out, "arr: {}, ", &length).unwrap();
                }
                None => {
                    write!(&mut self.out, "arr: nil, ").unwrap();
                }
            }

            Ok(())
        }
    }
}
