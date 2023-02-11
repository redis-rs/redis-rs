use std::{io, pin::Pin};

use redis::Value;
use {
    futures::{
        ready,
        task::{self, Poll},
    },
    partial_io::{quickcheck_types::GenWouldBlock, quickcheck_types::PartialWithErrors, PartialOp},
    quickcheck::{quickcheck, Gen},
    tokio::io::{AsyncRead, ReadBuf},
};

mod support;
use crate::support::{block_on_all, encode_value};

#[derive(Clone, Debug)]
struct ArbitraryValue(Value);

impl ::quickcheck::Arbitrary for ArbitraryValue {
    fn arbitrary(g: &mut Gen) -> Self {
        let size = g.size();
        ArbitraryValue(arbitrary_value(g, size))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self.0 {
            Value::Nil | Value::Okay => Box::new(None.into_iter()),
            Value::Int(i) => Box::new(i.shrink().map(Value::Int).map(ArbitraryValue)),
            Value::BulkString(ref xs) => {
                Box::new(xs.shrink().map(Value::BulkString).map(ArbitraryValue))
            }
            Value::Array(ref xs) | Value::Set(ref xs) => {
                let ys = xs
                    .iter()
                    .map(|x| ArbitraryValue(x.clone()))
                    .collect::<Vec<_>>();
                Box::new(
                    ys.shrink()
                        .map(|xs| xs.into_iter().map(|x| x.0).collect())
                        .map(Value::Array)
                        .map(ArbitraryValue),
                )
            }
            Value::Map(ref _xs) => Box::new(vec![ArbitraryValue(Value::Map(vec![]))].into_iter()),
            Value::Attribute {
                ref data,
                ref attributes,
            } => Box::new(
                vec![ArbitraryValue(Value::Attribute {
                    data: data.clone(),
                    attributes: attributes.clone(),
                })]
                .into_iter(),
            ),
            Value::Push { ref kind, ref data } => {
                let mut ys = data
                    .iter()
                    .map(|x| ArbitraryValue(x.clone()))
                    .collect::<Vec<_>>();
                ys.insert(0, ArbitraryValue(Value::SimpleString(kind.to_string())));
                Box::new(
                    ys.shrink()
                        .map(|xs| xs.into_iter().map(|x| x.0).collect())
                        .map(Value::Array)
                        .map(ArbitraryValue),
                )
            }
            Value::SimpleString(ref status) => {
                Box::new(status.shrink().map(Value::SimpleString).map(ArbitraryValue))
            }
            Value::Double(i) => Box::new(i.shrink().map(Value::Double).map(ArbitraryValue)),
            Value::Boolean(i) => Box::new(i.shrink().map(Value::Boolean).map(ArbitraryValue)),
            Value::BigNumber(ref i) => {
                Box::new(vec![ArbitraryValue(Value::BigNumber(i.clone()))].into_iter())
            }
            Value::VerbatimString {
                ref format,
                ref text,
            } => Box::new(
                vec![ArbitraryValue(Value::VerbatimString {
                    format: format.clone(),
                    text: text.clone(),
                })]
                .into_iter(),
            ),
        }
    }
}

fn arbitrary_value(g: &mut Gen, recursive_size: usize) -> Value {
    use quickcheck::Arbitrary;
    if recursive_size == 0 {
        Value::Nil
    } else {
        match u8::arbitrary(g) % 6 {
            0 => Value::Nil,
            1 => Value::Int(Arbitrary::arbitrary(g)),
            2 => Value::BulkString(Arbitrary::arbitrary(g)),
            3 => {
                let size = {
                    let s = g.size();
                    usize::arbitrary(g) % s
                };
                Value::Array(
                    (0..size)
                        .map(|_| arbitrary_value(g, recursive_size / size))
                        .collect(),
                )
            }
            4 => {
                let size = {
                    let s = g.size();
                    usize::arbitrary(g) % s
                };

                let mut string = String::with_capacity(size);
                for _ in 0..size {
                    let c = char::arbitrary(g);
                    if c.is_ascii_alphabetic() {
                        string.push(c);
                    }
                }

                if string == "OK" {
                    Value::Okay
                } else {
                    Value::SimpleString(string)
                }
            }
            5 => Value::Okay,
            _ => unreachable!(),
        }
    }
}

struct PartialAsyncRead<R> {
    inner: R,
    ops: Box<dyn Iterator<Item = PartialOp> + Send>,
}

impl<R> AsyncRead for PartialAsyncRead<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.ops.next() {
            Some(PartialOp::Limited(n)) => {
                let len = std::cmp::min(n, buf.remaining());
                buf.initialize_unfilled();
                let mut sub_buf = buf.take(len);
                ready!(Pin::new(&mut self.inner).poll_read(cx, &mut sub_buf))?;
                let filled = sub_buf.filled().len();
                buf.advance(filled);
                Poll::Ready(Ok(()))
            }
            Some(PartialOp::Err(err)) => {
                if err == io::ErrorKind::WouldBlock {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Err(io::Error::new(
                        err,
                        "error during read, generated by partial-io",
                    ))
                    .into()
                }
            }
            Some(PartialOp::Unlimited) | None => Pin::new(&mut self.inner).poll_read(cx, buf),
        }
    }
}

quickcheck! {
    fn partial_io_parse(input: ArbitraryValue, seq: PartialWithErrors<GenWouldBlock>) -> () {

        let mut encoded_input = Vec::new();
        encode_value(&input.0, &mut encoded_input).unwrap();

        let mut reader = &encoded_input[..];
        let mut partial_reader = PartialAsyncRead { inner: &mut reader, ops: Box::new(seq.into_iter()) };
        let mut decoder = combine::stream::Decoder::new();

        let result = block_on_all(redis::parse_redis_value_async(&mut decoder, &mut partial_reader));
        assert!(result.as_ref().is_ok(), "{}", result.unwrap_err());
        assert_eq!(
            result.unwrap(),
            input.0,
        );
    }
}
