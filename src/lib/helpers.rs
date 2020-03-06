use timely::dataflow::Scope;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::ExchangeData;

use chrono::Utc;

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
//pub fn print_trace<G, Tr>(trace: &mut Tr, round: usize)
//where
//    G: Scope,               // timely dataflow scope
//    G::Timestamp: Lattice,  // differential dataflow timestamp constraint
//    G::Timestamp: Lattice+Ord,
//    Tr: TraceReader<Time=G::Timestamp> + Clone,
//    Tr::Batch: BatchReader<Tr::Key, Tr::Val, G::Timestamp, Tr::R>,
//    Tr::Cursor: Cursor<Tr::Key, Tr::Val, G::Timestamp, Tr::R>,
//    Tr::R: isize
//{
//    if let Some((mut cursor, storage)) = trace.cursor_through(&[round+1]) {
//        while let Some(_key) = cursor.get_key(&storage) {
//            while let Some(val) = cursor.get_val(&storage) {
//                let mut count: isize = 0;
//                cursor.map_times(&storage, |time, &diff| {
//                    if time.le(&(round+1)) {
//                        count += diff;
//                    }
//                });
//                if count > 0 {
//                    println!("{} {:?} {:?}", count, _key, val);
//                }
//                cursor.step_val(&storage)
//            }
//            cursor.step_key(&storage);
//        }
//    } else {
//        println!("COULDN'T GET CURSOR")
//    }
//}