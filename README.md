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

rustcache is about half as fast as memcached according to [memslap](http://docs.libmemcached.org/bin/memslap.html):

rustcached:

    $ memslap --servers=localhost:11211 --test=set --concurrency=10 --debug --execute-number=10000 --flag --flush
        Threads connecting to servers 10
        Took 3.145 seconds to load data
    $ memslap --servers=localhost:11211 --test=get --concurrency=10 --debug --execute-number=10000 --flag --flush
        Threads connecting to servers 10
        Took 4.976 seconds to read data

memcached:

    $ memslap --servers=localhost:11212 --test=set --concurrency=10 --debug --execute-number=10000 --flag --flush
        Threads connecting to servers 10
        Took 2.736 seconds to load data
    $ memslap --servers=localhost:11212 --test=get --concurrency=10 --debug --execute-number=10000 --flag --flush
        Threads connecting to servers 10
        Took 3.933 seconds to read data

# Code organisation:

* `cmd.rs`: control starts here, command line arguments parsed, and the server started
* `store.rs`: houses the memcached application logic (e.g. what does "add" mean and how do I apply it?)
* `lru.rs`: the LRU cache
* `parser.rs`: protocol parsing
* `server.rs`: socket handling and response writing

# Todo:

* We're using the default rust allocator (jemalloc) for everything, instead of a slab allocator like memcached
* you can exhaust memory in the parser with an unlimited key/value length
* you can't `set` values larger than the memcached length limits, but you can `append`/`prepend` past them
* We copy a lot of stuff around right now that we don't have to, especially in the response builder
* `lru.rs` uses `Arc<T>` but there's probably a way to just use `Rc<T>` when we go single-threaded

# Future features:

* we use LRU right now like memcached does, but I want to try ARC in a real environment
* eager deletion of expired items during idle periods
