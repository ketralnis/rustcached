#[macro_use]
extern crate nom;
extern crate time;
extern crate getopts;
extern crate regex;

mod parser;
mod store;
mod lru;
mod server;
mod cmd;

pub fn main() {
    cmd::main()
}
