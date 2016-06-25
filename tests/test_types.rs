extern crate redis;


#[test]
fn test_is_single_arg() {
    use redis::ToRedisArgs;

    let sslice : &[_] = &["foo"][..];
    let nestslice : &[_] = &[sslice][..];
    let nestvec = vec![nestslice];
    let bytes = b"Hello World!";
    let twobytesslice : &[_] = &[bytes, bytes][..];
    let twobytesvec = vec![bytes, bytes];

    assert_eq!("foo".is_single_arg(), true);
    assert_eq!(sslice.is_single_arg(), true);
    assert_eq!(nestslice.is_single_arg(), true);
    assert_eq!(nestvec.is_single_arg(), true);
    assert_eq!(bytes.is_single_arg(), true);
    assert_eq!(twobytesslice.is_single_arg(), false);
    assert_eq!(twobytesvec.is_single_arg(), false);
}
