use connection::Connection;
use enums::*;

mod macros;

pub struct ScanIterator<'a, T> {
    con: &'a mut Connection,
    cmd: &'static str,
    pre_args: ~[CmdArg<'a>],
    post_args: ~[CmdArg<'a>],
    cursor: i64,
    conv_func: 'a |Value| -> Option<T>,
    end: bool,
    buffer: ~[Value],
}


impl<'a, T> ScanIterator<'a, T> {
    fn next_from_buffer(&mut self) -> Option<T> {
        loop {
            match self.buffer.pop() {
                Some(x) => {
                    match (self.conv_func)(x) {
                        Some(x) => { return Some(x); }
                        None => {}
                    }
                }
                None => { break; }
            }
        }
        None
    }

    fn read_from_connection(&mut self) -> bool {
        if self.end {
            return false;
        }

        let mut args = ~[];
        if self.pre_args.len() > 0 {
            args.push_all(self.pre_args);
        }
        args.push(IntArg(self.cursor));
        if self.post_args.len() > 0 {
            args.push_all(self.post_args);
        }

        match self.con.execute(self.cmd, args) {
            Bulk(items) => {
                let mut iter = items.move_iter();
                let new_cursor = (try_unwrap!(iter.next(), false))
                    .get_as::<i64>().unwrap_or(0);

                match try_unwrap!(iter.next(), false) {
                    Bulk(mut buffer) => {
                        buffer.reverse();
                        self.buffer = buffer;
                    },
                    _ => { return false; }
                }

                if new_cursor == 0 {
                    self.end = true;
                } else {
                    self.cursor = new_cursor;
                }
            },
            _ => { return false; }
        }

        self.buffer.len() > 0
    }
}

impl<'a, T> Iterator<T> for ScanIterator<'a, T> {

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.next_from_buffer() {
            Some(x) => { return Some(x); }
            None => {}
        };
        if self.buffer.len() == 0 && !self.read_from_connection() {
            self.end = true;
            return None;
        }
        match self.next_from_buffer() {
            Some(x) => Some(x),
            None => { self.end = true; None }
        }
    }
}
