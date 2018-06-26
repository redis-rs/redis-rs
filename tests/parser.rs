extern crate redis;

extern crate futures;
extern crate partial_io;
#[macro_use]
extern crate quickcheck;
extern crate tokio;

use std::io::{self, BufReader};

use partial_io::{GenWouldBlock, PartialAsyncRead, PartialWithErrors};

use tokio::executor::current_thread::block_on_all;

use futures::Future;

use redis::Value;

#[derive(Clone, Debug)]
struct ArbitraryValue(Value);
impl ::quickcheck::Arbitrary for ArbitraryValue {
    fn arbitrary<G: ::quickcheck::Gen>(g: &mut G) -> Self {
        let size = g.size();
        ArbitraryValue(arbitrary_value(g, size))
    }
    fn shrink(&self) -> Box<Iterator<Item = Self>> {
        match self.0 {
            Value::Nil | Value::Okay => Box::new(None.into_iter()),
            Value::Int(i) => Box::new(i.shrink().map(Value::Int).map(ArbitraryValue)),
            Value::Data(ref xs) => Box::new(xs.shrink().map(Value::Data).map(ArbitraryValue)),
            Value::Bulk(ref xs) => {
                let ys = xs.iter()
                    .map(|x| ArbitraryValue(x.clone()))
                    .collect::<Vec<_>>();
                Box::new(
                    ys.shrink()
                        .map(|xs| xs.into_iter().map(|x| x.0).collect())
                        .map(Value::Bulk)
                        .map(ArbitraryValue),
                )
            }
            Value::Status(ref status) => {
                Box::new(status.shrink().map(Value::Status).map(ArbitraryValue))
            }
        }
    }
}

fn arbitrary_value<G: ::quickcheck::Gen>(g: &mut G, recursive_size: usize) -> Value {
    use quickcheck::Arbitrary;
    if recursive_size == 0 {
        Value::Nil
    } else {
        match g.gen_range(0, 6) {
            0 => Value::Nil,
            1 => Value::Int(Arbitrary::arbitrary(g)),
            2 => Value::Data(Arbitrary::arbitrary(g)),
            3 => {
                let size = {
                    let s = g.size();
                    g.gen_range(0, s)
                };
                Value::Bulk(
                    (0..size)
                        .map(|_| arbitrary_value(g, recursive_size / size))
                        .collect(),
                )
            }
            4 => {
                let size = {
                    let s = g.size();
                    g.gen_range(0, s)
                };
                let status = g.gen_ascii_chars().take(size).collect();
                if status == "OK" {
                    Value::Okay
                } else {
                    Value::Status(status)
                }
            }
            5 => Value::Okay,
            _ => unreachable!(),
        }
    }
}

#[doc(hidden)]
pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{}\r\n", val),
        Value::Data(ref val) => {
            try!(write!(writer, "${}\r\n", val.len()));
            try!(writer.write_all(val));
            writer.write_all(b"\r\n")
        }
        Value::Bulk(ref values) => {
            try!(write!(writer, "*{}\r\n", values.len()));
            for val in values.iter() {
                try!(encode_value(val, writer));
            }
            Ok(())
        }
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::Status(ref s) => write!(writer, "+{}\r\n", s),
    }
}

quickcheck!{
    fn partial_io_parse(input: ArbitraryValue, seq: PartialWithErrors<GenWouldBlock>) -> () {
        let mut encoded_input = Vec::new();
        encode_value(&input.0, &mut encoded_input).unwrap();

        let mut reader = &encoded_input[..];
        let partial_reader = PartialAsyncRead::new(&mut reader, seq);

        let result = block_on_all(redis::parse_async(BufReader::new(partial_reader))
            .map(|t| t.1));
        assert!(result.as_ref().is_ok(), "{}", result.unwrap_err());
        assert_eq!(
            result.unwrap(),
            input.0,
        );
    }
}
