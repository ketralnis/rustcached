use std::thread::spawn;
use std::sync::{Arc, Mutex};
use std::net::{TcpListener, TcpStream};

use std::io;
use std::io::{Read, Write};

use store::Store;
use store::Response;
use store::ServerCommand;
use parser::CommandConfig;
use parser;

pub const NAME: &'static [u8] = b"rustcache";
pub const VERSION: &'static [u8] = b"0.1.0";

fn format_response(response: Response, socket: &mut Write) -> io::Result<()> {
    match response {
        Response::Data{responses} => {
            for response in &responses {
                try!(socket.write(b"VALUE "));
                try!(socket.write(response.key));
                try!(socket.write(b" "));
                try!(socket.write(format!("{}", response.flags).as_bytes()));
                try!(socket.write(b" "));
                try!(socket.write(format!("{}", response.data.len()).as_bytes()));
                try!(socket.write(b"\r\n"));
                try!(socket.write(&response.data));
                try!(socket.write(b"\r\n"));
            }
            try!(socket.write(b"END\r\n"));
        }
        Response::Gets{responses} => {
            for response in &responses {
                try!(socket.write(b"VALUE "));
                try!(socket.write(response.key));
                try!(socket.write(b" "));
                try!(socket.write(format!("{}", response.flags).as_bytes()));
                try!(socket.write(b" "));
                try!(socket.write(format!("{}", response.data.len()).as_bytes()));
                try!(socket.write(b" "));
                try!(socket.write(format!("{}", response.unique).as_bytes()));
                try!(socket.write(b" "));
                try!(socket.write(b"\r\n"));
                try!(socket.write(&response.data));
                try!(socket.write(b"\r\n"));
            }
            try!(socket.write(b"END\r\n"));
        }
        Response::Incr{value} => {
            try!(socket.write(format!("{}", value).as_bytes()));
            try!(socket.write(b"\r\n"));
        }
        Response::Deleted => {
            try!(socket.write(b"DELETED\r\n"));
        }
        Response::Touched => {
            try!(socket.write(b"TOUCHED\r\n"));
        }
        Response::Ok => {
            try!(socket.write(b"OK\r\n"));
        }
        Response::Stored => {
            try!(socket.write(b"STORED\r\n"));
        }
        Response::NotStored => {
            try!(socket.write(b"NOT_STORED\r\n"));
        }
        Response::Exists => {
            try!(socket.write(b"EXISTS\r\n"));
        }
        Response::NotFound => {
            try!(socket.write(b"NOT_FOUND\r\n"));
        }
        Response::Error => {
            try!(socket.write(b"ERROR\r\n"));
        }
        Response::ClientError{message} => {
            try!(socket.write(b"CLIENT_ERROR "));
            try!(socket.write(message));
            try!(socket.write(b"\r\n"));
        },
        Response::ServerError{message} => {
            try!(socket.write(b"SERVER_ERROR "));
            try!(socket.write(message));
            try!(socket.write(b"\r\n"));
        }
        Response::TooBig => {
            try!(socket.write(b"SERVER_ERROR object too large for cache"));
        }
        Response::Version => {
            try!(socket.write(b"VERSION "));
            try!(socket.write(NAME));
            try!(socket.write(b" "));
            try!(socket.write(VERSION));
            try!(socket.write(b"\r\n"));
        }
    }

    try!(socket.flush());

    Ok(())
}

fn client(locked_store: Arc<Mutex<Store>>, mut socket: TcpStream, verbose: bool) {
    if verbose {
        println!("client connect");
    }

    // this buffer on our stack is the largest amount that we can read from the
    // wire in a single go. bigger means fewer copies but more memory used per
    // client connection
    let mut buff: [u8; 10240] = [0; 10240];

    // the accumulated data that's been read but not parsed yet.  TODO we can
    // avoid a lot of copies here by trying to use buff directly when possible
    // and only spilling onto the heap when necessary. TODO this can be become
    // infinite in size. We need provisions for booting clients that grow it too
    // big, and for shrinking it occasionally so every client doesn't have
    // megabytes of buffer just because they used that much once in the past
    let mut parse_state: Vec<u8> = Vec::with_capacity(buff.len());

    loop {
        match socket.read(&mut buff) {
            Err(err) => {
                if verbose {
                    println!("client err: {:?}", err)
                }
                return;
            }
            Ok(size) if size == 0 => {
                if verbose {
                    println!("client disconnect");
                }
                return; // eof
            }
            Ok(size) => {
                parse_state.extend_from_slice(&buff[0..size]);

                // TODO this is all sorts of slow. we hold the lock until the
                // client is done receiving all of our bits!

                match parser::parse_command(&parse_state.to_vec()) { // TODO copy
                    parser::IResult::Done(remaining, command_config) => {
                        let CommandConfig {should_reply, command} = command_config;

                        let response = match command {
                            ServerCommand::Quit => {
                                // no response, just disconnect them and quit
                                return;
                            }
                            ServerCommand::Bad(text) => {
                                if verbose {
                                    println!("bad client command: {:?}",
                                             String::from_utf8_lossy(text))
                                }
                                Response::Error
                            }
                            _ => {
                                // all others must be sent to the store
                                let mut unlocked_store = locked_store.lock().unwrap();
                                unlocked_store.apply(command)
                            }
                        };
                        if should_reply {
                            match format_response(response, &mut socket) {
                                Result::Ok(_) => (),
                                Result::Err(err) => {
                                    if verbose {
                                        println!("client write error {:?}", err);
                                    }
                                    // TODO right now we just disconnect them
                                    return;
                                }
                            }
                        }
                        // TODO this does all sorts of copying
                        parse_state.clear();
                        parse_state.extend_from_slice(remaining);
                    }
                    parser::IResult::Error(err) => {
                        if verbose {
                            println!("parser error? {:?}", err);
                        }
                        // TODO can we recover from this?
                        return;
                    }
                    parser::IResult::Incomplete(_needed) => {
                        continue;
                    }
                }
            }
        }
    }
}

fn start_client(locked_store: Arc<Mutex<Store>>, socket: TcpStream, verbose: bool) {
    spawn(move || client(locked_store, socket, verbose));
}

pub fn start(port: u16, capacity: usize, verbose: bool) {
    let store = Store::new(capacity);
    let locked_store = Arc::new(Mutex::new(store));
    let uri = format!("0.0.0.0:{}", port);
    let uri: &str = &uri;

    if verbose {
        println!("starting server");
    }

    for client_stream in TcpListener::bind(&uri).unwrap().incoming() {
        match client_stream {
            Ok(client_stream) => {
                start_client(locked_store.clone(), client_stream, verbose);
            }
            Err(err) => {
                println!("client accept error: {:?}", err);
            }
        }
    }
}
