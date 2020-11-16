#[macro_use]
extern crate named_tuple;
extern crate chrono;

mod lib;
mod queries;

fn main() {
    let mut args: Vec<String> = std::env::args().collect();

    // exec path not needed
    args.remove(0);

    let query_id = args.remove(0)
        .parse::<isize>().expect("Cannot parse query id.");

    let path = args.remove(0);

    let change_path = args.remove(0);

    // User load_bi_param to load a set of predefined query parameters
    // let params = lib::loader::load_bi_param(path.as_str(), query_id);

    let runner = match query_id {
        3 => queries::q3::run,
        5 => queries::q5::run,
        7 => queries::q7::run,
        114 => queries::q114::run, // 114, because it was dropped in newer LDBC specs.
        15 => queries::q15::run,
        19 => queries::q19::run,
        _ => panic!("Query {} is not yet implemented.", query_id)
    };

    runner(path.clone(), change_path.clone(), &args);
}
