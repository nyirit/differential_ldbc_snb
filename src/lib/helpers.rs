use timely::dataflow::Scope;
use differential_dataflow::Collection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::InputSession;

use chrono::Utc;
use differential_dataflow::trace::{TraceReader, BatchReader, Cursor};
use std::fmt::Display;

pub fn format_timestamp(timestamp: u64) -> String {
    chrono::DateTime::<Utc>::from(std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp)).to_rfc3339()
}

// https://github.com/frankmcsherry/ttc2018liveContest/blob/aa622a83cc889bab9c6844e1e7db32d2738e6f6e/solutions/differential/src/main.rs#L476
/// Return the top `limit` distinct keys in input, by keys.
pub fn limit<G, K, V>(input: &Collection<G,(K,V)>, limit: usize) -> Collection<G,Vec<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: ExchangeData+std::hash::Hash,
    V: ExchangeData,
{
    use differential_dataflow::hashable::Hashable;
    use differential_dataflow::operators::reduce::Reduce;

    let input = input.map(|(key,val)| (key.hashed(),(key, val)));

    let top_k =
    input
        .map(|(hash,(val, key))| (hash % 10000, (val, key)))
        .reduce(move |_key, input, output| {
            for ((val, key), _wgt) in input.iter().rev().take(limit) {
                output.push(((val.clone(), key.clone()), 1));
            }
        })
        .map(|(hash, (val, key))| (hash % 100, (val, key)))
        .reduce(move |_key, input, output| {
            for ((val, key), _wgt) in input.iter().rev().take(limit) {
                output.push(((val.clone(), key.clone()), 1));
            }
        })
        .map(|(hash, (val, key))| (hash % 1, (val, key)));

    top_k
        .reduce(move |_zero, input, output| {
            let mut result = Vec::new();
            result.extend(input.iter().rev().take(limit).map(|((_val, key),_wgt)| key.clone()));
            output.push((result, 1));
        })
        .map(|(_hash, vec)| vec)
}

// Adds a vector to the InputSession and advances time.
pub fn input_insert_vec<T: Data>(data: Vec<T>, input: &mut InputSession<usize, T, isize>, next_time: usize) {
    for element in data {
        input.insert(element);
    }

    input.advance_to(next_time);
    input.flush();
}

pub fn print_trace<T, Tr>(trace: &mut Tr, round: usize)
where
    Tr: TraceReader<Time=usize, Key=Vec<T>> + Clone,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, usize, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, usize, Tr::R>,
    T: Display
{
    if let Some((mut cursor, storage)) = trace.cursor_through(&[round+1]) {
        while let Some(key) = cursor.get_key(&storage) {
            while let Some(_val) = cursor.get_val(&storage) {
                let count: isize = 1;
                /*FIXME is this needed?
                cursor.map_times(&storage, |time, &diff| {
                    if time.le(&(round+1)) {
                        count += diff;
                    }
                });*/
                if count > 0 {
                    for element in key {
                        println!("{}", element);
                    }
                }
                cursor.step_val(&storage)
            }
            cursor.step_key(&storage);
        }
    } else {
        println!("Failed to get cursor :(")
    }
}