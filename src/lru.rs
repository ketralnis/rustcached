/// The LRU storage engine

use std::cmp::Ord;
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::mem;
use std::hash::Hash;
use std::sync::Arc;

pub type Weight = usize;
pub type Timestamp = u32;

type LruEntryUsed<K> = (Timestamp, Arc<K>);
type LruEntryExpires<K> = (Timestamp, Arc<K>);

#[derive(Debug)]
pub struct LruCache<K: HasWeight + Ord + Hash + Clone, V: HasWeight> {
    map: HashMap<K, LruEntry<V>>,
    lru: BTreeSet<LruEntryUsed<K>>,
    expires: BTreeSet<LruEntryExpires<K>>,
    capacity: Weight,
    weight: Weight, // TODO store this?
}

pub trait HasWeight {
    fn weight(&self) -> Weight;
}

#[derive(Debug)]
pub struct LruEntry<V> {
    pub data: V,
    used: Timestamp,
    pub expires: Option<Timestamp>,
    weight: Weight,
}

impl<K: HasWeight + Ord + Hash + Clone, V: HasWeight> LruCache<K, V> {
    pub fn new(capacity: Weight) -> LruCache<K, V> {
        LruCache {
            map: HashMap::new(),
            lru: BTreeSet::new(),
            expires: BTreeSet::new(),
            capacity: capacity,
            weight: 0,
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.lru.clear();
        self.expires.clear();
        self.weight = 0;
    }

    pub fn get_full_entry(&mut self, key: &K, now: Timestamp) -> Option<&LruEntry<V>> {
        let entry = self._get_full_entry(key, now);
        entry.map(|e| &*e)
    }

    fn _get_full_entry(&mut self, key: &K, now: Timestamp) -> Option<&mut LruEntry<V>> {
        match self.map.get_mut(&key) {
            None => Option::None,

            Some(ref entry) if expired((*entry).expires, now) => {
                // we found it, but it's expired. we could theoretically
                // pre-emptively remove it on discovering this, but for the
                // moment we'll leave it there and clean it up during the normal
                // cleaup process (thereby keeping our reads fast and paying the
                // cost on writes instead)
                Option::None
            }

            Some(entry) => {
                // we found it and it hasn't expired. Since it's being used now
                // we need to update its position in the LRU
                let k2 = Arc::new(key.clone());
                if ((*entry).used) != now {
                    // only update it if the value would change
                    let old_lru_key = ((*entry).used, k2.clone());
                    self.lru.remove(&old_lru_key);

                    entry.used = now;

                    let new_lru_key = (now, k2.clone());
                    self.lru.insert(new_lru_key);
                }

                Some(entry)
            }
        }
    }

    pub fn get(&mut self, key: &K, now: Timestamp) -> Option<&V> {
        self.get_full_entry(key, now).map(|entry| &entry.data)
    }

    pub fn set(&mut self, key: K, value: V, expires: Option<Timestamp>, now: Timestamp) -> bool {
        if expired(expires, now) {
            // if it's already expired there's no need to store it
            return false;
        }

        // if it's already in here, we need to get rid of it
        self.delete(&key);

        let weight = compute_weight(&key, &value);

        if weight > self.capacity {
            // we'll never be able to store this
            return false;
        }

        // free up any space that we need to in order to fit this
        let capacity = self.capacity;
        self.deweight(capacity - weight, now);

        let entry = LruEntry {
            data: value,
            expires: expires,
            weight: weight,
            used: now,
        };

        let k2 = Arc::new(key.clone());

        self.map.insert(key, entry);
        self.weight += weight;

        let lru_key = (now, k2.clone());
        self.lru.insert(lru_key);

        if let Some(expires_ts) = expires {
            // if it expires, add it to the expiration queue
            let expires_key = (expires_ts, k2.clone());
            self.expires.insert(expires_key);
        }

        // TODO Store always ignores this return value. Do we care?
        return true;
    }

    pub fn fast_get(&self, key: &K, now: Timestamp) -> Option<&V> {
        // fetch the value of a key without updating the LRU
        match self.map.get(key) {
            Some(entry) if expired((*entry).expires, now) => None,
            Some(entry) => Some(&(*entry).data),
            None => None,
        }
    }

    pub fn contains(&self, key: &K, now: Timestamp) -> bool {
        // checks for the presence of a key (without updating the LRU)
        self.fast_get(key, now).is_some()
    }

    pub fn touch(&mut self, key: &K, expires: Option<Timestamp>, now: Timestamp) -> bool {
        // update the timestamp and last-used field of a row without copying the
        // whole contents
        let (old_expires, old_used) = match self._get_full_entry(key, now) {
            None => {
                // just bail, it was never in here anyway
                return false;
            }
            Some(full_entry) => {
                // change it in-place
                let old_expires = (*full_entry).expires;
                let old_used = (*full_entry).used;
                (*full_entry).expires = expires;
                (*full_entry).used = now;
                (old_expires, old_used)
            }
        };

        // update our data structures
        self._touch(key, old_expires, expires, old_used, now);
        true
    }

    fn _touch(&mut self,
              key: &K,
              old_expires: Option<Timestamp>,
              new_expires: Option<Timestamp>,
              old_used: Timestamp,
              now: Timestamp) {

        let k2 = Arc::new((*key).clone());

        if old_expires != new_expires {
            if let Some(old_expires_ts) = old_expires {
                // if it expired before, we have to remove it
                let old_expires_key = (old_expires_ts, k2.clone());
                self.expires.remove(&old_expires_key);
            }

            if let Some(expires_ts) = new_expires {
                // if it expires now, we have to add it
                let expires_key = (expires_ts, k2.clone());
                self.expires.insert(expires_key);
            }
        }

        if old_used != now {
            let old_lru_key = (old_used, k2.clone());
            self.lru.remove(&old_lru_key);
            let new_lru_key = (now, k2.clone());
            self.lru.insert(new_lru_key);
        }
    }

    pub fn delete(&mut self, key: &K) -> bool {
        let found = {
            match self.map.get(key) {
                None => None,
                Some(entry) => {
                    Some(((*entry).expires, (*entry).used, (*entry).weight))
                }
            }
        };

        if let Some((expires, used, weight)) = found {
            let k2 = Arc::new((*key).clone());
            self.map.remove(key);
            let lru_key = (used, k2.clone());
            self.lru.remove(&lru_key);
            if let Some(expires_ts) = expires {
                let expires_key = (expires_ts, k2.clone());
                self.lru.remove(&expires_key);
            }
            self.weight -= weight;
            true
        } else {
            false
        }
    }

    fn deweight(&mut self, target_weight: Weight, now: Timestamp) {
        // we're trying to add more data, but there isn't room for it. We need
        // to delete at least `weight` worth of data to fit this new entry

        while self.weight > target_weight && !self.map.is_empty() {
            self.deweight_once(now);
        }

        assert!(self.capacity >= target_weight);
    }

    fn deweight_once(&mut self, now: Timestamp) {
        if self.map.is_empty() {
            // nothing we can delete if it's already empty
            return;
        }

        let expired_key = {
            // check the expiration queue for stuff that's already expired that
            // we can just delete
            let ref maybe_expirable = self.expires;
            let mut maybe_expirable = maybe_expirable.into_iter();
            let maybe_expirable = maybe_expirable.next();
            match maybe_expirable {
                None => None,
                Some(found_tuple) => {
                    let (ref expired_ts, ref expired_key) = *found_tuple;
                    if _expired(*expired_ts, now) {
                        Some(expired_key.clone())
                    } else {
                        None
                    }
                }
            }
        };

        if let Some(key_ref) = expired_key {
            self.delete(&*key_ref);
            return;
        }

        // otherwise we have to use the LRU
        let lru_key = {
            let ref lru = self.lru;
            let mut lru = lru.into_iter();
            let lru = lru.next();
            match lru {
                None => None,
                Some(found_tuple) => {
                    let (_, ref lru_key) = *found_tuple;
                    Some(lru_key.clone())
                }
            }
        };

        if let Some(key_ref) = lru_key {
            self.delete(&*key_ref);
            return;
        }

        unreachable!("there's nothing on the LRU?");
    }

    #[cfg(test)]
    pub fn all_keys(&self, now: Timestamp) -> Vec<K> {
        // very expensive operation that fetches a full list of all of the keys
        // that we know about that aren't expired
        let mut ret = Vec::new();
        for (ref key, ref value) in &self.map {
            if !expired((*value).expires, now) {
                ret.push((*key).clone());
            }
        }
        ret.sort();
        ret
    }
}

fn expired(timestamp: Option<Timestamp>, now: Timestamp) -> bool {
    match timestamp {
        Some(ts) if _expired(ts, now) => {
            true
        }
        _ => false,
    }
}

fn _expired(timestamp: Timestamp, now: Timestamp) -> bool {
    return timestamp < now;
}

pub fn compute_weight<K: HasWeight, V: HasWeight>(key: &K, value: &V) -> Weight {
    // this isn't perfect because it ignores some hashtable and btreeset
    // overhead, but it's a pretty good guess at the memory usage of an entry
    let mut sum = 0;
    sum += 3 * key.weight();
    sum += value.weight();
    sum += mem::size_of::<Weight>();
    sum += mem::size_of::<Timestamp>();
    sum += mem::size_of::<Option<Timestamp>>();
    sum
}

impl HasWeight for Vec<u8> {
    fn weight(&self) -> Weight {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: Timestamp = 100;
    const FUTURE: Timestamp = NOW + 1;
    const FUTURE2: Timestamp = NOW + 2;
    const PAST: Timestamp = NOW - 1;
    const CAPACITY: Weight = 200;

    #[test]
    fn basic_set() {
        let mut store = make_store();

        store.set(b("foo"), b("data"), None, NOW);
        assert!(store.contains(&b("foo"), NOW));
        assert_eq!(store.all_keys(NOW), vec![b("foo")]);
    }

    #[test]
    fn set_already_expired() {
        let mut store = make_store();

        store.set(b("foo"), b("data"), Some(PAST), NOW);
        assert!(store.all_keys(NOW).is_empty());
    }

    #[test]
    fn set_expires() {
        let mut store = make_store();

        store.set(b("foo"), b("data"), Some(PAST), NOW);
        assert!(store.all_keys(FUTURE).is_empty());
    }

    #[test]
    fn too_big() {
        let mut store = make_store();

        store.set(b("foo1"), b("data"), None, PAST);

        let big = make_big(CAPACITY * 2);

        store.set(b("foo2"), big, None, PAST);

        // setting that big item should have been rejected, so the old data
        // should still be there
        assert!(store.contains(&b("foo1"), NOW));
        assert_eq!(store.all_keys(NOW), vec![b("foo1")]);
    }

    #[test]
    fn outgrow() {
        let mut store = make_store();

        store.set(b("foo1"), b("data"), None, PAST);

        // these should push that guy out
        store.set(b("foo2"), make_big(30), None, NOW);
        store.set(b("foo3"), make_big(30), None, FUTURE);

        assert!(!store.contains(&b("foo1"), FUTURE2));
        assert!(store.contains(&b("foo2"), FUTURE2));
        assert!(store.contains(&b("foo3"), FUTURE2));
    }

    #[test]
    fn prefer_expired() {
        // make sure we prefer to remove expired members over live members
        let mut store = make_store();

        store.set(b("foo1"), make_big(30), None, PAST);
        store.set(b("foo2"), make_big(30), Some(NOW), NOW);

        // this has to push one of them out
        store.set(b("foo3"), make_big(30), None, FUTURE);

        assert!(store.contains(&b("foo1"), FUTURE2));
        assert!(!store.contains(&b("foo2"), FUTURE2));
        assert!(store.contains(&b("foo3"), FUTURE2));
    }

    #[test]
    fn clear() {
        let mut store = make_store();

        store.set(b("foo1"), b("data"), None, PAST);
        store.clear();

        assert!(!store.contains(&b("foo1"), NOW));
    }


    fn make_store() -> LruCache<Vec<u8>, Vec<u8>> {
        let store = LruCache::new(CAPACITY);
        store
    }

    fn make_big(size: usize) -> Vec<u8> {
        let mut big = Vec::new();
        for x in 0..size * 2 {
            big.push(x as u8);
        }
        big
    }

    fn b(inp: &'static str) -> Vec<u8> {
        // syntactic sugar for tests
        let mut s = String::new();
        s.push_str(inp);
        s.into_bytes()
    }
}
