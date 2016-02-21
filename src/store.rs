/// The storage server. This implements all of the memcached semantics,
/// including mapping parsed commands into actual fetches and mutations on an
/// LRU that it contains
#[allow(unused_imports)]
use time;

use std::str;
use std::mem;

use lru;

// Keys as we get them from the client
pub type Key<'a> = &'a [u8];
// Keys as we store them
pub type StoredKey = Vec<u8>;
// Keys as we return them to the client (for now we're only returning them keys
// that they gave us, so we're using the same type to return it to them so we
// can just use that same pointer instead of copying it)
pub type ReturnedKey<'a> = &'a [u8];

// Data as we get it from the client
pub type Data<'a> = &'a [u8];
// Data as we store it (copied from the client connection's memory)
pub type StoredData = Vec<u8>;
// Data as we return it to a client (copied for now)
pub type ReturnedData = Vec<u8>;

pub type Ttl = u32;
pub type Flags = u32;
pub type CasUnique = u64;
pub type IncrValue = u64;

pub type Capacity = usize;

#[derive(Debug,PartialEq)]
struct DataContainer {
    data: StoredData,
    flags: Flags,

    // TODO we are updating these by hand in the individual Setter handlers that
    // can change it, but we'll want to change that
    unique: CasUnique,
}

#[derive(Debug,PartialEq,Eq)]
pub enum SetterType {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas(CasUnique),
}

#[derive(Debug,PartialEq,Eq)]
pub enum GetterType {
    Get,
    Gets,
}

#[derive(Debug,PartialEq,Eq)]
pub enum IncrementerType {
    Incr,
    Decr,
}

#[derive(Debug,PartialEq,Eq)]
pub enum ServerCommand<'a> {
    Setter {
        setter: SetterType,
        key: Key<'a>,
        data: Data<'a>,
        ttl: Ttl,
        flags: Flags,
    },
    Getter {
        getter: GetterType,
        keys: Vec<Key<'a>>,
    },
    Delete {
        key: Key<'a>,
    },
    Touch {
        key: Key<'a>,
        ttl: Ttl,
    },
    Incrementer {
        incrementer: IncrementerType,
        key: Key<'a>,
        value: IncrValue,
    },
    FlushAll,
    Bad(&'a [u8]),
    Quit,
    Version,
    Verbosity,
}

#[derive(Debug,PartialEq,Eq)]
pub struct SingleGetResponse<'a> {
    pub key: ReturnedKey<'a>,
    pub data: ReturnedData,
    pub flags: Flags,
    pub unique: CasUnique,
}

#[derive(Debug,PartialEq,Eq)]
pub enum Response<'a> {
    // DataResponse and GetsResponse share the SingleGetResponse format for
    // simplicity. If copying a bunch of unneeded unique values turns out to be
    // a problem we can revisit but this makes the response builder much simpler
    DataResponse {
        responses: Vec<SingleGetResponse<'a>>,
    },
    GetsResponse {
        responses: Vec<SingleGetResponse<'a>>,
    },
    IncrResponse {
        value: IncrValue,
    },
    DeletedResponse,
    TouchedResponse,
    OkResponse,
    StoredResponse,
    NotStoredResponse,
    ExistsResponse,
    NotFoundResponse,
    ErrorResponse,
    ClientErrorResponse {
        message: &'a [u8],
    },
    ServerError {
        message: &'a [u8],
    },
    VersionResponse,
    TooBig,
}

fn forgetful_parse_int(current_data: &StoredData) -> Option<IncrValue> {
    // try to interpret it as an int
    let as_string = str::from_utf8(&current_data);
    if as_string.is_err() {
        return None;
    }
    let as_string = as_string.unwrap();
    let as_int = as_string.parse::<IncrValue>();
    if as_int.is_err() {
        return None;
    }

    return Option::Some(as_int.unwrap());
}

enum _IncrSubResult {
    NotFound,
    BadInt,
    NewValue(IncrValue, Option<lru::Timestamp>, Flags),
}

// the number of seconds in a TTL after which we start recognising it as a
// timestamp instead. This is a magic number used by memcached so we're cloning
// its behaviour here. used by wrap_ttl
const MAGIC_DATE: Ttl = 60 * 60 * 24 * 30;

// right now these are enforced here but it would be nice if the parser could do
// some of it too
const MAX_KEY: usize = 255;
const MAX_DATA: usize = 1024 * 1024; // 1MB

pub fn wrap_ttl(ttl: Ttl, now: Ttl) -> Option<Ttl> {
    if ttl == 0 {
        None
    } else if ttl < MAGIC_DATE {
        Option::Some(now + ttl)
    } else {
        Option::Some(ttl)
    }
}

#[cfg(not(test))]
fn epoch_time() -> Ttl {
    time::get_time().sec as Ttl
}
#[cfg(test)]
pub fn epoch_time() -> Ttl {
    // so the tests can use a known value
    1455082881
}

#[derive(Debug)]
pub struct Store {
    store: lru::LruCache<StoredKey, DataContainer>,
    last_cas_id: CasUnique,
}

impl Store {
    pub fn new(capacity: Capacity) -> Store {
        Store {
            store: lru::LruCache::new(capacity),
            last_cas_id: 0,
        }
    }

    fn make_cas_id(&mut self) -> CasUnique {
        self.last_cas_id += 1;
        self.last_cas_id
    }

    pub fn apply<'a>(&mut self, command: ServerCommand<'a>) -> Response<'a> {
        let now = epoch_time(); // TODO lazy?

        match command {
            ServerCommand::Setter{key: ckey, data: cdata, .. } if ckey.len() > MAX_KEY ||
                                                                  cdata.len() > MAX_DATA => {
                Response::TooBig
            }
            ServerCommand::Setter{setter, key: ckey, data: cdata,
                                  ttl: cttl, flags} => {
                let new_cas = self.make_cas_id(); // TODO too many IDs
                let ttl = wrap_ttl(cttl, now);
                let skey = ckey.to_vec();

                let container = |data: &[u8], flags| {
                    DataContainer {
                        data: data.to_vec(), // does a copy
                        flags: flags,
                        unique: new_cas,
                    }
                };

                match setter {
                    SetterType::Set => {
                        self.store.set(skey, container(cdata, flags), ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Add if self.store.contains(&skey, now) => {
                        Response::NotStoredResponse
                    }
                    SetterType::Add => {
                        self.store.set(skey, container(cdata, flags), ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Replace if self.store.contains(&skey, now) => {
                        self.store.set(skey, container(cdata, flags), ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Replace => {
                        Response::NotStoredResponse
                    }
                    SetterType::Append if self.store.contains(&skey, now) => {
                        // this is pretty slow because we use immutable data in
                        // the lru. it's possible to make this faster by using
                        // mutable data structures instead that we can just
                        // directly modify, but then we'd need to make sure to
                        // keep the weights and stuff in sync and that's a pain
                        let (new_vec, old_ttl, old_flags) = {
                            let current_entry = self.store.get_full_entry(&skey, now).unwrap();
                            let ref current_container = current_entry.data;
                            let new_size = cdata.len() + current_container.data.len();
                            let mut new_vec = Vec::with_capacity(new_size);
                            new_vec.extend_from_slice(&current_container.data);
                            new_vec.extend_from_slice(cdata);
                            (new_vec, current_entry.expires, current_container.flags)
                        };
                        self.store.set(skey, container(&new_vec, old_flags), old_ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Append => {
                        Response::NotStoredResponse
                    }
                    SetterType::Prepend if self.store.contains(&skey, now) => {
                        let (new_vec, old_ttl, old_flags) = {
                            let current_entry = self.store.get_full_entry(&skey, now).unwrap();
                            let ref current_container = current_entry.data;
                            let new_size = cdata.len() + current_container.data.len();
                            let mut new_vec = Vec::with_capacity(new_size);
                            new_vec.extend_from_slice(cdata);
                            new_vec.extend_from_slice(&current_container.data);
                            (new_vec, current_entry.expires, current_container.flags)
                        };
                        self.store.set(skey, container(&new_vec, old_flags), old_ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Prepend => {
                        Response::NotStoredResponse
                    }
                    SetterType::Cas(_) if !self.store.contains(&skey, now) => {
                        Response::NotFoundResponse
                    }
                    SetterType::Cas(unique) if (self.store
                                                    .fast_get(&skey, now)
                                                    .map(|cont| cont.unique) ==
                                                Some(unique)) => {
                        self.store.set(skey, container(cdata, flags), ttl, now);
                        Response::StoredResponse
                    }
                    SetterType::Cas(_) => {
                        // n.b. failed cas updates don't update the lru
                        Response::ExistsResponse
                    }
                }
            }

            ServerCommand::Getter{getter, keys} => {
                let mut found = Vec::with_capacity(keys.len());
                for ckey in keys {
                    let skey = ckey.to_vec();
                    if let Some(item) = self.store.get(&skey, now) {
                        found.push(SingleGetResponse {
                            key: ckey,
                            data: item.data.clone(), // does a copy
                            flags: item.flags,
                            unique: item.unique,
                        });
                    }
                }
                // and turn that into the right result format for the request
                // (does this really have to be this repetetive?)
                match getter {
                    GetterType::Get => {
                        Response::DataResponse { responses: found }
                    }
                    GetterType::Gets => {
                        Response::GetsResponse { responses: found }
                    }
                }
            }
            ServerCommand::Delete{key: ckey} => {
                let skey = ckey.to_vec();

                if self.store.delete(&skey) {
                    Response::DeletedResponse
                } else {
                    Response::NotFoundResponse
                }
            }
            ServerCommand::Touch{key: ckey, ttl: cttl} => {
                let skey = ckey.to_vec();
                let ttl = wrap_ttl(cttl, now);

                match self.store.contains(&skey, now) {
                    false => Response::NotFoundResponse,
                    true => {
                        self.store.touch(&skey, ttl, now);
                        Response::TouchedResponse
                    }
                }
            }
            ServerCommand::Incrementer{incrementer, key: ckey, value} => {
                let new_cas = self.make_cas_id();
                let skey = ckey.to_vec();

                let isr = match self.store.get_full_entry(&skey, now) {
                    None => _IncrSubResult::NotFound,
                    Some(full_entry) => {
                        let ref item = (*full_entry).data;
                        let ref current_data = (*item).data;
                        let as_int = forgetful_parse_int(&current_data);
                        match as_int {
                            None => _IncrSubResult::BadInt,
                            Some(current_int) => {
                                let new_int = match incrementer {
                                    // memcached is saturating in the negative
                                    // direction
                                    IncrementerType::Decr => current_int.saturating_sub(value),
                                    // ...but wrapping in the positive direction
                                    IncrementerType::Incr => current_int.wrapping_add(value),
                                };
                                _IncrSubResult::NewValue(new_int, full_entry.expires, item.flags)
                            }
                        }
                    }
                };
                match isr {
                    _IncrSubResult::NotFound => Response::NotFoundResponse,
                    _IncrSubResult::BadInt => Response::ClientErrorResponse {
                        message: b"cannot increment or decrement non-numeric value",
                    },
                    _IncrSubResult::NewValue(new_int, sttl, flags) => {
                        let re_str = new_int.to_string();
                        let re_bytes = re_str.as_bytes();
                        let new_data = re_bytes.to_vec();
                        let new_container = DataContainer {
                            data: new_data.to_vec(),
                            flags: flags,
                            unique: new_cas,
                        };
                        self.store.set(skey, new_container, sttl, now);
                        Response::IncrResponse { value: new_int }
                    }
                }
            }
            ServerCommand::FlushAll => {
                self.store.clear(); // weeeeee
                Response::OkResponse
            }
            ServerCommand::Bad(_) => Response::ErrorResponse,
            ServerCommand::Version => Response::VersionResponse,
            // we ignore this, we just support it to make memcapable happy
            ServerCommand::Verbosity => Response::OkResponse,
            ServerCommand::Quit => {
                unreachable!("this should have been handled by the server dispatch loop")
            }

        }
    }

    #[cfg(test)]
    pub fn simple_get(&mut self, key: &str) -> Option<String> {
        let as_bytes = key.as_bytes();
        let as_vec = as_bytes.to_vec();
        match self.store.fast_get(&as_vec, epoch_time()) {
            None => None,
            Some(container) => {
                let ref container_data = container.data;
                let container_as_string = String::from_utf8_lossy(&container_data);
                let mut new_string = String::new();
                new_string.push_str(&container_as_string);
                Some(new_string)
            }
        }
    }

    #[cfg(test)]
    pub fn simple_get_flags(&mut self, key: &str) -> Option<Flags> {
        let as_bytes = key.as_bytes();
        let as_vec = as_bytes.to_vec();
        self.store.get(&as_vec, epoch_time()).map(|c| c.flags)
    }

    #[cfg(test)]
    pub fn simple_get_ttl(&mut self, key: &str) -> Option<Ttl> {
        let as_bytes = key.as_bytes();
        let as_vec = as_bytes.to_vec();
        match self.store.get_full_entry(&as_vec, epoch_time()) {
            None => None,
            Some(entry) => {
                (*entry).expires
            }
        }
    }

    #[cfg(test)]
    pub fn simple_set(&mut self, key: &str, data: &str) {
        self.simple_set_cas(key, data, 0);
    }

    #[cfg(test)]
    pub fn simple_set_cas(&mut self, key: &str, data: &str, unique: CasUnique) {
        let mut key_vec: Vec<u8> = Vec::new();
        key_vec.extend_from_slice(key.as_bytes());
        let mut data_vec: Vec<u8> = Vec::new();
        data_vec.extend_from_slice(data.as_bytes());

        self.store.set(key_vec,
                       DataContainer {
                           data: data_vec,
                           flags: 0,
                           unique: unique,
                       },
                       Option::None,
                       epoch_time());
    }

}

impl lru::HasWeight for DataContainer {
    fn weight(&self) -> lru::Weight {
        (1 * self.data.capacity() + mem::size_of::<CasUnique>() + mem::size_of::<Flags>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn set() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Set,
            key: b"foo",
            data: b"bar",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn add_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Add,
            key: b"foo",
            data: b"bar",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn add_present() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Add,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::NotStoredResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn replace_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Replace,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::NotStoredResponse, res);
        assert_eq!(None, store.simple_get("foo"));
    }

    #[test]
    pub fn replace_present() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Replace,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("baz".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn append_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Append,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::NotStoredResponse, res);
        assert_eq!(None, store.simple_get("foo"));
    }

    #[test]
    pub fn append_present() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Append,
            key: b"foo",
            data: b"baz",
            flags: 12,
            ttl: 34,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("barbaz".to_string()), store.simple_get("foo"));
        // make sure we didn't update the flags or ttl
        assert_eq!(Some(0), store.simple_get_flags("foo"));
        assert_eq!(None, store.simple_get_ttl("bar"));
    }

    #[test]
    pub fn prepend_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Prepend,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::NotStoredResponse, res);
        assert_eq!(None, store.simple_get("foo"));
    }

    #[test]
    pub fn prepend_present() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Prepend,
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("bazbar".to_string()), store.simple_get("foo"));
        // make sure we didn't update the flags or ttl
        assert_eq!(Some(0), store.simple_get_flags("foo"));
        assert_eq!(None, store.simple_get_ttl("bar"));
    }

    #[test]
    pub fn cas_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Cas(50),
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::NotFoundResponse, res);
        assert_eq!(None, store.simple_get("foo"));
    }

    #[test]
    pub fn cas_wrong() {
        let mut store = Store::new(100);
        store.simple_set_cas("foo", "bar", 100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Cas(200),
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::ExistsResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn cas_right() {
        let mut store = Store::new(100);
        store.simple_set_cas("foo", "bar", 100);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Cas(100),
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("baz".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn cas_refreshes() {
        let mut store = Store::new(100);
        store.simple_set_cas("foo", "bar", 100);
        store.simple_set("foo", "quux");
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Cas(100),
            key: b"foo",
            data: b"baz",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::ExistsResponse, res);
        assert_eq!(Some("quux".to_string()), store.simple_get("foo"));
    }

    #[test]
    pub fn get() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Getter {
            getter: GetterType::Get,
            keys: vec!["foo".as_bytes()],
        });
        assert_eq!(res,
                   Response::DataResponse {
                       responses: vec![SingleGetResponse {
                                           key: "foo".as_bytes(),
                                           data: b("bar"),
                                           flags: 0,
                                           unique: 0,
                                       }],
                   });
    }

    #[test]
    pub fn get_multi() {
        let mut store = Store::new(100);
        store.simple_set("foo1", "bar1");
        store.simple_set("foo2", "bar2");
        let res = store.apply(ServerCommand::Getter {
            getter: GetterType::Get,
            keys: vec!["foo1".as_bytes(), "foo2".as_bytes(), "foo3".as_bytes()],
        });
        assert_eq!(res,
                   Response::DataResponse {
                       responses: vec![SingleGetResponse {
                                           key: "foo1".as_bytes(),
                                           data: b("bar1"),
                                           flags: 0,
                                           unique: 0,
                                       },
                                       SingleGetResponse {
                                           key: "foo2".as_bytes(),
                                           data: b("bar2"),
                                           flags: 0,
                                           unique: 0,
                                       }],
                   });
    }

    #[test]
    pub fn gets() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Getter {
            getter: GetterType::Get,
            keys: vec!["foo".as_bytes()],
        });
        assert_eq!(res,
                   Response::DataResponse {
                       responses: vec![SingleGetResponse {
                                           key: "foo".as_bytes(),
                                           data: b("bar"),
                                           flags: 0,
                                           unique: 0,
                                       }],
                   });
    }

    #[test]
    pub fn gets_multi() {
        let mut store = Store::new(100);
        store.simple_set_cas("foo1", "bar1", 100);
        store.simple_set_cas("foo2", "bar2", 100);
        let res = store.apply(ServerCommand::Getter {
            getter: GetterType::Gets,
            keys: vec!["foo1".as_bytes(), "foo2".as_bytes(), "foo3".as_bytes()],
        });
        assert_eq!(res,
                   Response::GetsResponse {
                       responses: vec![SingleGetResponse {
                                           key: "foo1".as_bytes(),
                                           data: b("bar1"),
                                           flags: 0,
                                           unique: 100,
                                       },
                                       SingleGetResponse {
                                           key: "foo2".as_bytes(),
                                           data: b("bar2"),
                                           flags: 0,
                                           unique: 100,
                                       }],
                   });
    }

    #[test]
    pub fn incr_present_and_good() {
        let mut store = Store::new(100);
        store.simple_set("foo", "1");
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Incr,
            key: b"foo",
            value: 5,
        });
        assert_eq!(res, Response::IncrResponse { value: 6 });
    }

    #[test]
    pub fn incr_present_and_bad() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Incr,
            key: b"foo",
            value: 5,
        });
        assert_eq!(res,
                   Response::ClientErrorResponse {
                       message: b"cannot increment or decrement non-numeric value",
                   });
    }

    #[test]
    pub fn incr_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Incr,
            key: b"foo",
            value: 5,
        });
        assert_eq!(res, Response::NotFoundResponse);
    }

    #[test]
    pub fn incr_refreshes_cas() {
        let mut store = Store::new(100);
        store.simple_set_cas("foo", "20", 100);
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Incr,
            key: b"foo",
            value: 5,
        });
        assert_eq!(Response::IncrResponse { value: 25 }, res);
        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Cas(100),
            key: b"foo",
            data: b"30",
            flags: 0,
            ttl: 0,
        });
        assert_eq!(Response::ExistsResponse, res);
        assert_eq!(Some("25".to_string()), store.simple_get("foo"));
    }


    #[test]
    pub fn decr() {
        let mut store = Store::new(100);
        store.simple_set("foo", "20");
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Decr,
            key: b"foo",
            value: 5,
        });
        assert_eq!(res, Response::IncrResponse { value: 15 });
    }

    #[test]
    pub fn decr_saturates() {
        let mut store = Store::new(100);
        store.simple_set("foo", "20");
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Decr,
            key: b"foo",
            value: 100,
        });
        assert_eq!(res, Response::IncrResponse { value: 0 });
    }

    #[test]
    pub fn incr_wraps() {
        let mut store = Store::new(100);
        store.simple_set("foo", "18446744073709551615");
        let res = store.apply(ServerCommand::Incrementer {
            incrementer: IncrementerType::Incr,
            key: b"foo",
            value: 2,
        });
        assert_eq!(res, Response::IncrResponse { value: 1 });
    }

    #[test]
    pub fn delete_present() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        let res = store.apply(ServerCommand::Delete { key: b"foo" });
        assert_eq!(res, Response::DeletedResponse);
    }

    #[test]
    pub fn delete_not_present() {
        let mut store = Store::new(100);
        let res = store.apply(ServerCommand::Delete { key: b"foo" });
        assert_eq!(res, Response::NotFoundResponse);
    }

    #[test]
    pub fn touch_not_present() {
        let mut store = Store::new(100);

        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: 0,
        });
        assert_eq!(Response::NotFoundResponse, res);
        assert_eq!(None, store.simple_get("foo"));
    }

    #[test]
    pub fn touch() {
        let mut store = Store::new(1000);

        // will get the version marked cfg(test)!
        let now: Ttl = epoch_time();

        store.simple_set("foo", "bar");
        assert_eq!(store.simple_get_ttl("foo"), None);

        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: 0,
        });
        assert_eq!(Response::TouchedResponse, res);
        assert_eq!(store.simple_get_ttl("foo"), None);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));

        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: 100,
        });
        assert_eq!(Response::TouchedResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
        assert_eq!(store.simple_get_ttl("foo"), Some(now + 100));

        // make sure we cna set it back to 0
        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: 0,
        });
        assert_eq!(Response::TouchedResponse, res);
        assert_eq!(store.simple_get_ttl("foo"), None);
    }

    #[test]
    pub fn wrapping_ttl() {
        // memcached accepts timestamps in seconds-in-the-future or in absolute
        // epoch seconds. It uses a heuristic magic number (MAGIC_DATE above) to
        // guess which one to use, so we have to make sure we properly support
        // this
        let mut store = Store::new(100);

        // will get the version marked cfg(test)!
        let now: Ttl = epoch_time();

        assert_eq!(wrap_ttl(0, now), None);
        assert_eq!(wrap_ttl(1, now), Some(now + 1));
        assert_eq!(wrap_ttl(2, now), Some(now + 2));
        assert_eq!(wrap_ttl(now, now), Some(now));
        assert_eq!(wrap_ttl(now + 1, now), Some(now + 1));

        store.simple_set("foo", "bar");

        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: 100,
        });
        assert_eq!(Response::TouchedResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
        assert_eq!(store.simple_get_ttl("foo"), Some(now + 100));

        let res = store.apply(ServerCommand::Touch {
            key: b"foo",
            ttl: now + 200,
        });
        assert_eq!(Response::TouchedResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
        assert_eq!(store.simple_get_ttl("foo"), Some(now + 200));

        let res = store.apply(ServerCommand::Setter {
            setter: SetterType::Set,
            key: b"foo",
            data: b"bar",
            ttl: now + 300,
            flags: 0,
        });
        assert_eq!(Response::StoredResponse, res);
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));
        assert_eq!(store.simple_get_ttl("foo"), Some(now + 300));
    }

    #[test]
    pub fn flushall() {
        let mut store = Store::new(100);
        store.simple_set("foo", "bar");
        assert_eq!(Some("bar".to_string()), store.simple_get("foo"));

        let res = store.apply(ServerCommand::FlushAll);
        assert_eq!(res, Response::OkResponse);

        assert_eq!(None, store.simple_get("foo"));
    }

    fn b(inp: &'static str) -> Vec<u8> {
        // syntactic sugar for tests
        let mut s = String::new();
        s.push_str(inp);
        s.into_bytes()
    }
}
