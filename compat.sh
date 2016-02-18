#!/bin/sh -e

sep() {
    echo ----------------------------
}

echo killing

pkill memcached||true
pkill rustcache||true

echo building
cargo test
cargo clean

echo building for release
cargo build --release
sep

echo starting servers
memcached -p 11212 &
cargo run --release -- -p 11211 &
sep

# I'd like to use these tests but they fail intermittently on real memcached
# echo memcached memcapable
# memcapable -t1 -a -p 11212 -v || true
# sep
# echo rustcache memcapable
# memcapable -t1 -a -p 11211 -v || true
# sep

echo memcached memslap
time memslap --servers=localhost:11212 --concurrency=10 --execute-number=100000 || true
time memslap --servers=localhost:11212 --concurrency=10 --execute-number=100000 || true
echo rustcache memslap
time memslap --servers=localhost:11211 --concurrency=10 --execute-number=100000 || true
time memslap --servers=localhost:11211 --concurrency=10 --execute-number=100000 || true

kill %1 %2
wait
