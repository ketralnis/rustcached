use std::env;
use std::process;
use std::str::FromStr;
use std::io::Write;

use getopts::Options;

use server;
use parser::parse_size;

macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

pub fn main() {
    let mut port = 11211;
    let mut capacity = 64*1024*1024;
    let mut verbose = false;

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("p", "port", "port to listen on (default: 11211)", "PORT");
    opts.optopt("m", "memory", "port to listen on (default: 64mb)", "MEMORY");
    opts.optflag("v", "verbose", "be really verbose");
    opts.optflag("h", "help", "print help and exit");

    let print_usage_and_die = |exit_code: i32| -> ! {
        let brief = format!("Usage: {} [options]", program);
        println_stderr!("{}", opts.usage(&brief));
        process::exit(exit_code);
    };

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => {
            println_stderr!("{}", f);
            return print_usage_and_die(1);
        }
    };

    if matches.opt_present("h") || !matches.free.is_empty() {
        return print_usage_and_die(1);
    }

    if let Some(digits) = matches.opt_str("p") {
        if let Result::Ok(port_num) = FromStr::from_str(&digits) {
            port = port_num;
        } else {
            println_stderr!("couldn't parse port num {}", digits);
            return print_usage_and_die(1);
        }
    }

    if let Some(size_spec) = matches.opt_str("m") {
        if let Some(size) = parse_size(&size_spec) {
            capacity = size;
        } else {
            println_stderr!("couldn't parse size {}", size_spec);
            return print_usage_and_die(1);
        }
    }

    if matches.opt_present("v") {
        verbose = true;
    }

    server::start(port, capacity, verbose);
}
