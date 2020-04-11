#[macro_use]
extern crate named_tuple;
extern crate chrono;

mod lib;
mod queries;

fn main() {
    let query_id = std::env::args().nth(1)
        .expect("Must supply query.")
        .parse::<usize>().expect("Cannot parse query id.");
    let path = std::env::args().nth(2).expect("Must supply data path.");
    let params = lib::loader::load_bi_param(path.as_str(), query_id);

    let runner = match query_id {
        4 => queries::q4::run,
        6 => queries::q6::run,
        8 => queries::q8::run,
        14 => queries::q14::run,
        _ => panic!("Query {} is not yet implemented.", query_id)
    };

    for param in params {
        println!("========= RUNNING QUERY {} WITH PARAMS {:?} =========", query_id, param);
        runner(path.clone(), &param);
    }
}
