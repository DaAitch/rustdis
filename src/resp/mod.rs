pub mod read;

const B_CR: u8 = b'\r';
const B_LF: u8 = b'\n';
const B_RESP_STRING_PREFIX: u8 = b'+';
const B_RESP_ERROR_PREFIX: u8 = b'-';
const B_RESP_INTEGER_PREFIX: u8 = b':';
const B_RESP_BULK_STRING_PREFIX: u8 = b'$';
const B_RESP_ARRAY_PREFIX: u8 = b'*';
