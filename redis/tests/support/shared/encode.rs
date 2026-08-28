//! Tooling to encode a [`Value`] as if it was sent by a Redis server

// Importing `Value` via `super`, as that works both in unit and integration tests.
use super::super::Value;
use std::io;

fn encode_iter<W>(values: &[Value], writer: &mut W, prefix: &str) -> io::Result<()>
where
    W: io::Write,
{
    write!(writer, "{}{}\r\n", prefix, values.len()).unwrap();
    for val in values.iter() {
        encode_value(val, writer).unwrap();
    }
    Ok(())
}

fn encode_map<W>(values: &[(Value, Value)], writer: &mut W, prefix: &str) -> io::Result<()>
where
    W: io::Write,
{
    write!(writer, "{}{}\r\n", prefix, values.len()).unwrap();
    for (k, v) in values.iter() {
        encode_value(k, writer).unwrap();
        encode_value(v, writer).unwrap();
    }
    Ok(())
}

pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    #![allow(clippy::write_with_newline)]
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{val}\r\n"),
        Value::BulkString(ref val) => {
            write!(writer, "${}\r\n", val.len()).unwrap();
            writer.write_all(val).unwrap();
            writer.write_all(b"\r\n")
        }
        Value::Array(ref values) => encode_iter(values, writer, "*"),
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::SimpleString(ref s) => write!(writer, "+{s}\r\n"),
        Value::Map(ref values) => encode_map(values, writer, "%"),
        Value::Attribute {
            ref data,
            ref attributes,
        } => {
            encode_map(attributes, writer, "|").unwrap();
            encode_value(data, writer).unwrap();
            Ok(())
        }
        Value::Set(ref values) => encode_iter(values, writer, "~"),
        Value::Double(val) => write!(writer, ",{val}\r\n"),
        Value::Boolean(v) => {
            if v {
                write!(writer, "#t\r\n")
            } else {
                write!(writer, "#f\r\n")
            }
        }
        Value::VerbatimString {
            ref format,
            ref text,
        } => {
            // format is always 3 bytes
            write!(writer, "={}\r\n{}:{}\r\n", 3 + text.len(), format, text)
        }
        Value::BigNumber(ref val) => {
            #[cfg(feature = "num-bigint")]
            return write!(writer, "({val}\r\n");
            #[cfg(not(feature = "num-bigint"))]
            {
                write!(writer, "(").unwrap();
                for byte in val {
                    write!(writer, "{byte}").unwrap();
                }
                write!(writer, "\r\n")
            }
        }
        Value::Push { ref kind, ref data } => {
            write!(writer, ">{}\r\n+{kind}\r\n", data.len() + 1).unwrap();
            for val in data.iter() {
                encode_value(val, writer).unwrap();
            }
            Ok(())
        }
        Value::ServerError(ref err) => match err.details() {
            Some(details) => write!(writer, "-{} {details}\r\n", err.code()),
            None => write!(writer, "-{}\r\n", err.code()),
        },
        _ => panic!("unknown value {value:?}"),
    }
}
