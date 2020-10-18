/*
Legacy LDBC SNB BI query 14. Top thread initiator
https://arxiv.org/pdf/2001.02299.pdf#page=69

Note: in the newer version of LDBC SNB BI this query is omitted.
*/

use timely::dataflow::ProbeHandle;
use differential_dataflow::input::Input;

use crate::lib::helpers::{print_trace, input_insert_vec, limit};
use crate::lib::loader::{load_person, load_dynamic_connection, parse_datetime};
use crate::lib::types::*;
use differential_dataflow::operators::{Count, Iterate, Join, Threshold, Consolidate};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::operators::Probe;
use std::time::Instant;

pub fn run(path: String, params: &Vec<String>) {
    // unpack parameters
    let param_begin_ = params[0].clone();
    let param_end_ = params[1].clone();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // bind param
        let param_begin = parse_datetime(param_begin_.clone());
        let param_end = parse_datetime(param_end_.clone());

        // create dataflow
        let (
            mut trace,
            mut person_input,
            mut post_has_creator_input,
            mut reply_of_input,
        ) =
            worker.dataflow::<usize, _, _>(|scope| {
                let (person_input, person) = scope.new_collection::<Person, _>();
                let (post_has_creator_input, post_has_creator) = scope.new_collection::<DynamicConnection, _>();

                // replyOf for comments AND posts
                let (reply_of_input, reply_of) = scope.new_collection::<DynamicConnection, _>();

                // filter out posts that were not created in the given time frame
                let filtered_posts = post_has_creator
                    .filter(move |conn| param_begin <= *conn.created() && *conn.created() <= param_end)
                    ;

                // get number of posts for each person
                let person_posts = filtered_posts
                    .map(|conn| conn.b().clone()) // -> person_id
                    .count()
                    ;   // -> person_id, count

                // get all direct replies of filtered posts, add also created datetime
                let trans_initial = reply_of
                    .map(|conn| (conn.b().clone(), (conn.a().clone(), conn.created().clone()))) // -> parent_id, (child_id, child_created)
                    .semijoin( // only keep direct replies
                        &filtered_posts.map(|conn| conn.a().clone()) // -> post_id
                    )
                    ;

                // map replies to be used later in the iterate closure
                let replies = reply_of
                    .map(|conn| (conn.b().clone(), (conn.a().clone(), conn.created().clone())));

                // get the number of (transitive) replies for each person
                let trans_replies = trans_initial
                    // calculate transitive replies
                    .iterate(|transitive| {
                        let initial = trans_initial.enter(&transitive.scope());
                        let replies = replies.enter(&transitive.scope());

                        // add next level replies, but also keep all replies found so far
                        return transitive
                            // join on child id
                            .map(|(parent, (child, created))| (child, (parent, created)))
                            // join next level replies
                            .join(&replies)
                            // keep only the parent_id (as key), child_id, and child created date
                            .map(|(_child, ((parent, _ccreated), (gchild, gcreated)))| (parent, (gchild, gcreated)))
                            // concat initial values to have all the steps
                            .concat(&initial)
                            // ensure convergence, as final values may duplicate
                            .distinct()
                            ;
                    })
                    // filter out messages which are not in the given time frame
                    .filter(move |(_parent, (_child, created))| param_begin <= *created && *created <= param_end)
                    // only the number of replies matter, not the replies themselves
                    .map(|(parent, (_child, _created))| (parent.clone(), ()))
                    // consolidate stream. Probably not really needed.
                    .consolidate()
                    // join creator person
                    .join_map(
                        &post_has_creator.map(|conn| (conn.a().clone(), conn.b().clone())), // -> post_id, person_id
                        |_post_id, _, person_id| person_id.clone()
                    )
                    // make sure to have each person at least once to have an outer join later
                    // (this introducing an additional reply, but this will be corrected later)
                    .concat(&person_posts.map(|(person_id, _count)| person_id))
                    // count the replies for each person
                    .count()
                    ; // -> person_id, number of replies

                // create the final result
                let result = person_posts
                    // join replies to posts
                    .join_map(
                        &trans_replies,
                        // messageCount has to include the number of posts as well. Remove the earlier added +1 reply.
                        |person_id, posts_count:&isize, replies_count| (person_id.clone(), (posts_count.clone(), replies_count + posts_count - 1)) // fix off-by-one
                    )
                    // join Person objects to have names as well
                    .join_map(
                        &person.map(|person| (person.id().clone(), (person.first_name().clone(), person.last_name().clone()))),
                        |person_id, (posts_count, replies_count), (first_name, last_name)|
                            (
                                (std::isize::MAX - replies_count, person_id.clone()), // sort: -messageCount, +person_id
                                vec![person_id.to_string(), first_name.to_string(), last_name.to_string(), posts_count.to_string(), replies_count.to_string()]
                            )
                    )
                    ;

                // limit the number of results
                let arrangement = limit(&result, 100)
                    .arrange_by_self();

                arrangement.stream.probe_with(&mut probe);

                return (
                    arrangement.trace,
                    person_input, post_has_creator_input, reply_of_input
                );
            });

        // add inputs
        let next_time: usize = 1;
        input_insert_vec(load_person(path.as_str(), index, peers), &mut person_input, next_time);
        input_insert_vec(
            load_dynamic_connection("dynamic/post_hasCreator_person_0_0.csv", path.as_str(), index, peers),
            &mut post_has_creator_input,
            next_time,
        );
        // insert replyOf relations both for posts and comments, to handle them together in the dataflow
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_replyOf_post_0_0.csv", path.as_str(), index, peers),
            &mut reply_of_input,
            0, // do not advance just yet
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_replyOf_comment_0_0.csv", path.as_str(), index, peers),
            &mut reply_of_input,
            next_time,
        );

        eprintln!("LOADED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // Compute...
        while probe.less_than(person_input.time()) {
            worker.step();
        }

        eprintln!("CALCULATED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // print results
        print_trace(&mut trace, next_time);

        eprintln!("PRINTED;{:}", timer.elapsed().as_secs_f64());
    }).expect("Timely computation failed");
}
