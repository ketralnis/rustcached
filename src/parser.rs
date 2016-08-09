/// The parser. Takes a stream of bytes and turns it into a series of parsed
/// commands ready to be send to the store

use std::str::from_utf8;
use std::str::FromStr;

use nom::{crlf, space, digit};
use regex::Regex; // used for the size parser

pub use nom::{IResult, Needed};

use store::ServerCommand;
use store::IncrementerType;
use store::GetterType;
use store::SetterType;

#[derive(Debug,PartialEq,Eq)]
pub struct CommandConfig<'a> {
    pub should_reply: bool,
    pub command: ServerCommand<'a>,
}

named!(key_parser<&[u8], &[u8]>, is_not!(" \t\r\n\0"));

fn unwrap_noreply(tag: Option<&[u8]>) -> bool {
    match tag {
        Some(b"noreply") => false,
        Some(_) => panic!(format!("can't unwrap noreply tag {:?}", tag)),
        None => true,
    }
}

named!(u32_digit<u32>,
  map_res!(
    map_res!(
      digit,
      from_utf8
    ),
    FromStr::from_str
  )
);

named!(usize_digit<usize>,
    map_res!(
        map_res!(
            digit,
            from_utf8
        ),
        FromStr::from_str
    )
);


named!(u64_digit<u64>,
  map_res!(
    map_res!(
      digit,
      from_utf8
    ),
    FromStr::from_str
  )
);

fn map_setter_name(res: &[u8]) -> SetterType {
    match res {
        b"set" => SetterType::Set,
        b"add" => SetterType::Add,
        b"prepend" => SetterType::Prepend,
        b"replace" => SetterType::Replace,
        b"append" => SetterType::Append,
        _ => panic!(format!("unknown setter mapped? {:?}", res)),
    }
}

named!(parse_setter_name,
    alt!(
        tag!("set") |
        tag!("add") |
        tag!("prepend") |
        tag!("replace") |
        tag!("append")
    )
);

// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
named!(cmd_cas<&[u8], CommandConfig>,
    chain!(
        tag!("cas") ~
        space ~
        key: key_parser ~
        space ~
        flags: u32_digit ~
        space ~
        ttl: u32_digit ~
        space ~
        bytes: usize_digit ~
        space ~
        cas_unique: u64_digit ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf ~
        payload: take!(bytes) ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Setter{
                    setter: SetterType::Cas(cas_unique),
                    key: key,
                    data: payload,
                    ttl: ttl,
                    flags: flags,
                }
            }
        }
    )
);

// setters:
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\npayload\r\n
named!(cmd_set<&[u8], CommandConfig>,
    chain!(
        setter_name: parse_setter_name ~
        space ~
        key: key_parser ~
        space ~
        flags: u32_digit ~
        space ~
        ttl: u32_digit ~
        space ~
        bytes: u32_digit ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf ~
        payload: take!(bytes) ~ // assuming this is where the payload is
        crlf,
        || {
            let setter = map_setter_name(setter_name);

            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Setter {
                    setter: setter,
                    key: key,
                    data: payload,
                    ttl: ttl,
                    flags: flags,
                }
            }
        }
    )
);

fn map_getter_name(res: &[u8]) -> GetterType {
    match res {
        b"get" => GetterType::Get,
        b"gets" => GetterType::Gets,
        _ => panic!(format!("unknown getter mapped? {:?}", res)),
    }
}

named!(parse_getter_name,
    alt!(
        tag!("gets") |
        tag!("get")
    )
);


// get <key>*\r\n
// gets <key>*\r\n
named!(cmd_get<&[u8], CommandConfig>,
    chain!(
        getter_name: parse_getter_name ~
        space ~
        keys: separated_nonempty_list!(space, key_parser) ~
        crlf,
        || {
            CommandConfig {
                should_reply: true,
                command: ServerCommand::Getter {
                    getter: map_getter_name(getter_name),
                    keys: keys
                }
            }
        }
    )
);

// delete <key> [noreply]\r\n
// TODO there's a rumour that this can take a time?
named!(cmd_delete<&[u8], CommandConfig>,
    chain!(
        tag!("delete") ~
        space ~
        key: key_parser ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Delete {
                    key: key
                }
            }
        }
    )
);

// touch <key> <exptime> [noreply]\r\n
named!(cmd_touch<&[u8], CommandConfig>,
    chain!(
        tag!("touch") ~
        space ~
        key: key_parser ~
        space ~
        ttl: u32_digit ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Touch {
                    key: key,
                    ttl: ttl,
                }
            }
        }
    )
);

fn map_incr_name(res: &[u8]) -> IncrementerType {
    match res {
        b"incr" => IncrementerType::Incr,
        b"decr" => IncrementerType::Decr,
        _ => panic!(format!("unknown getter mapped? {:?}", res)),
    }
}

named!(parse_incr_name,
    alt!(
        tag!("incr") |
        tag!("decr")
    )
);

// incr <key> <value> [noreply]\r\n
// decr <key> <value> [noreply]\r\n
named!(cmd_incr<&[u8], CommandConfig>,
    chain!(
        incr_name: parse_incr_name ~
        space ~
        key: key_parser ~
        space ~
        value: u64_digit ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Incrementer {
                    incrementer: map_incr_name(incr_name),
                    key: key,
                    value: value,
                }
            }
        }
    )
);

// verbosity <amount>\r\n
named!(cmd_verbosity<&[u8], CommandConfig>,
    chain!(
        tag!("verbosity") ~
        space ~
        u32_digit ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::Verbosity
            }
        }
    )
);

// version\r\n
named!(cmd_version<&[u8], CommandConfig>,
    chain!(
        tag!("version") ~
        crlf,
        || {
            CommandConfig {
                should_reply: true,
                command: ServerCommand::Version
            }
        }
    )
);

// quit\r\n
named!(cmd_quit<&[u8], CommandConfig>,
    chain!(
        tag!("quit") ~
        crlf,
        || {
            CommandConfig {
                should_reply: true,
                command: ServerCommand::Quit
            }
        }
    )
);

// flush_all\r\n
named!(cmd_flushall<&[u8], CommandConfig>,
    chain!(
        tag!("flush_all") ~
        noreply: chain!(space ~ x: tag!("noreply"), || {x})? ~
        crlf,
        || {
            CommandConfig {
                should_reply: unwrap_noreply(noreply),
                command: ServerCommand::FlushAll
            }
        }
    )
);

// anything else is a malformed command
named!(cmd_bad<&[u8], CommandConfig>,
    chain!(
        bad_stuff: is_not!("\r\n")? ~
        crlf,
        || {
            CommandConfig {
                should_reply: true,
                command: ServerCommand::Bad(bad_stuff.unwrap_or(b""))
            }
        }
    )
);

named!(pub parse_command<&[u8], CommandConfig>,
    alt!(
        // these short ones need to go first to work around a bug in nom where
        // it thinks it needs more data than it does
        cmd_quit | cmd_version | cmd_flushall | cmd_verbosity
        | cmd_set | cmd_cas | cmd_get | cmd_delete | cmd_incr | cmd_touch
        | cmd_bad
    )
);

pub fn parse_size(size_str: &str) -> Option<usize> {
    let re = Regex::new(r"^(\d+)([kmgt]?)b?$").unwrap();
    match re.captures(size_str) {
        None => None,
        Some(matches) => {
            let digits = matches.at(1).unwrap();
            let number: usize = FromStr::from_str(digits).unwrap();
            let suffix = matches.at(2);
            let mult = match suffix {
                None | Some("b") | Some("") => 1,
                Some("k") => 1024,
                Some("m") => 1024 * 1024,
                Some("g") => 1024 * 1024 * 1024,
                Some("t") => 1024 * 1024 * 1024 * 1024,
                bad_mult => {
                    unreachable!(format!("weird suffix {:?}", bad_mult))
                }
            };
            Some(number * mult)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::ServerCommand;
    use store::IncrementerType;
    use store::GetterType;
    use store::SetterType;

    #[test]
    pub fn commands() {
        let tests: Vec<(&str, IResult<&[u8], CommandConfig>)> = vec![
            ("set foo 12 34 5\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Setter {setter: SetterType::Set, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("set foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Set, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("add foo 12 34 5\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Setter {setter: SetterType::Add, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("add foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Add, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("append foo 12 34 5\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Setter { setter: SetterType::Append, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("append foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Append, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("prepend foo 12 34 5\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Setter { setter: SetterType::Prepend, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("prepend foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Prepend, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("replace foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Replace, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("replace foo 12 34 5 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Replace, key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),

            ("cas foo 12 34 5 89\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Setter { setter: SetterType::Cas(89), key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),
            ("cas foo 12 34 5 89 noreply\r\ndata!\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Setter { setter: SetterType::Cas(89), key: b"foo", data: b"data!", ttl: 34, flags: 12 } })),

            ("get foo\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Getter { getter: GetterType::Get, keys: vec![b"foo"] } })),
            ("get foo1 foo2\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Getter { getter: GetterType::Get, keys: vec![b"foo1", b"foo2"] } })),
            ("gets foo\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Getter { getter: GetterType::Gets, keys: vec![b"foo"] } })),
            ("gets foo1 foo2\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Getter { getter: GetterType::Gets, keys: vec![b"foo1", b"foo2"] } })),

            ("delete foo\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Delete { key: b"foo" } })),
            ("delete foo noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Delete { key: b"foo" } })),

            ("incr foo 5\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Incrementer { incrementer: IncrementerType::Incr, key: b"foo", value: 5 } })),
            ("incr foo 5 noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Incrementer { incrementer: IncrementerType::Incr, key: b"foo", value: 5 } })),
            ("decr foo 5\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Incrementer { incrementer: IncrementerType::Decr, key: b"foo", value: 5 } })),
            ("decr foo 5 noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Incrementer { incrementer: IncrementerType::Decr, key: b"foo", value: 5 } })),

            ("touch foo 5\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Touch { key: b"foo", ttl: 5 } })),
            ("touch foo 5 noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Touch { key: b"foo", ttl: 5 } })),

            ("flush_all\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::FlushAll })),
            ("flush_all noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::FlushAll })),
            ("version\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Version })),
            ("quit\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Quit })),
            ("verbosity 10\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Verbosity })),
            ("verbosity 10 noreply\r\n",
             IResult::Done(b"", CommandConfig { should_reply: false, command: ServerCommand::Verbosity })),

            ("foo bar\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Bad(b"foo bar") })),
            ("version foo bar\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Bad(b"version foo bar") })),
            ("\r\n",
             IResult::Done(b"", CommandConfig { should_reply: true, command: ServerCommand::Bad(b"") } )),

        ];

        for &(ref command, ref expected_result) in &tests {
            // let (command, expected_result) = item;
            println!("command: {:?}", *command);
            let parsed = parse_command(command.as_bytes());
            println!("expect:  {:?}", expected_result);
            println!("got:     {:?}", parsed);
            if *expected_result == parsed {
                println!("good!");
            } else {
                println!("bad :(");
            }
            assert_eq!(*expected_result, parsed);
        }
    }

    #[test]
    pub fn parse_sizes() {
        let tests = vec![
            ("0", Some(0)),
            ("1", Some(1)),
            ("1b", Some(1)),
            ("10", Some(10)),
            ("100", Some(100)),
            ("1k", Some(1024)),
            ("2k", Some(2048)),
            ("1m", Some(1024*1024)),
            ("2m", Some(2*1024*1024)),
            ("2mb", Some(2*1024*1024)),
            ("garbage", None),
            ("1.5gb", None), // might be nice to support this some day
        ];

        for &(ref text, ref expected_result) in &tests {
            println!("Parsing {:?}", text);
            println!("Expect {:?}", expected_result);
            let parsed = parse_size(text);
            println!("Got {:?}", parsed);
            assert_eq!(*expected_result, parsed);
        }
    }
}
