A memcached-compatible clone written in rust

# Features:

* Aside from what's listed in Missing below, we support all memcached commands and are fully compatible

# Missing:

* binary protocol
* `stats`
* UDP
* unix sockets
* `delete` with expires (memcached dropped this support in 1.4)
* `flush_all` with expires
* `verbosity` is recognised but ignored
* SASL

# Performance

rustcache is about 9 times slower than memcached, according to `memslap` from libmemcached:

rustcached:

    $ memslap --servers=localhost:11211 --concurrency=100 --debug --execute-number=1000 --flag --flush
        Threads connecting to servers 100
        Took 9.035 seconds to load data

memcached:

    $ memslap --servers=localhost:11212 --concurrency=100 --debug --execute-number=1000 --flag --flush
        Threads connecting to servers 100
        Took 1.391 seconds to load data

* Code organisation:

* `cmd.rs`: control starts here, command line arguments parsed, and the server started
* `store.rs`: houses the memcached application logic (e.g. what does "add" mean and how do I apply it?)
* `parser.rs`: protocol parsing (both ascii and binary)
* `server.rs`: socket handling and response writing

# Todo:

* We're using the default rust allocator (jemalloc). We can probably do better
* you can exhaust memory in the parser with an unlimited key/value length
* you can't `set` values larger than the canonical length limits, but you can `append`/`prepend` past them
* `store.rs` tests don't currently test that flags, TTL, and CAS uniques are propogated in all cases
* We copy a lot of stuff around right now that we don't have to, especially in the response builder
* `lru.rs` uses `Arc<T>` but there's probably a way to just use `Rc<T>` when we go single-threaded

# Future features:

* we use LRU right now like memcached does, but I want to try ARC in a real environment
* eager deletion of expired items during idle periods
