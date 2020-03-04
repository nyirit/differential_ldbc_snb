#[macro_use]
extern crate named_tuple;
extern crate chrono;

use timely::dataflow::ProbeHandle;

mod lib;

use lib::loader::load_post;
use lib::types::Post;


fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        //let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let path = std::env::args().nth(1).expect("Must describe path");

        let probe = ProbeHandle::new();

        let mut posts_input = worker.dataflow(|scope| {
            use differential_dataflow::input::Input;

            let (posts_input, posts) = scope.new_collection::<Post, _>();

            posts.inspect(|x| println!("{:?}", (x.0).id()));

            return posts_input;
        });

        // add inputs
        let posts = load_post(&format!("{}dynamic/post_0_0.csv", path), index, peers);
        for post in posts {
            posts_input.insert(post);
        }

        posts_input.advance_to(1); posts_input.flush();

        //

        while probe.less_than(posts_input.time()) {
            worker.step();
        }


    }).expect("Timely computation failed");
}
