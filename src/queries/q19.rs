/*
LDBC SNB BI query 19. Interaction path between cities
https://ldbc.github.io/ldbc_snb_docs_snapshot/bi-read-19.pdf
*/

use differential_dataflow::collection::AsCollection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Join, Count, Iterate, Reduce};
use timely::dataflow::ProbeHandle;

use crate::lib::loader::*;
use crate::lib::types::*;
use crate::lib::helpers::{input_insert_vec, limit, print_trace};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::operators::{Probe, Map, Delay};
use std::time::Instant;
use std::cmp::{min, max};

pub fn run(path: String, change_path: String, params: &Vec<String>) {
    // unpack parameters
    let param_city1 = params[0].parse::<u64>().unwrap();
    let param_city2 = params[1].parse::<u64>().unwrap();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // create dataflow
        let (
            mut trace,
            mut located_in_input,
            mut knows_input,
            mut has_creator_input,
            mut reply_of_input,
        ) =
        worker.dataflow::<usize,_,_>(|scope| {
            let (located_in_input, locatedin) = scope.new_collection::<DynamicConnection, _>();
            let (knows_input, knows) = scope.new_collection::<DynamicConnection, _>();

            // creators for comments AND posts
            let (has_creator_input, has_creator) = scope.new_collection::<DynamicConnection, _>();
            // replyOf for comments AND posts
            let (reply_of_input, reply_of) = scope.new_collection::<DynamicConnection, _>();

            // people of city1
            let people1 = locatedin
                .filter(move |conn| param_city1.eq(conn.b()))
                .map(|conn| conn.a().clone())
                ;

            // people of city2
            let people2 = locatedin
                .filter(move |conn| param_city2.eq(conn.b()))
                .map(|conn| conn.a().clone())
                ;

            // bidirectional knows relation
            let bi_knows = knows
                .map(|conn| (conn.b().clone(), conn.a().clone()))
                .concat(
                    &knows.map(|conn| (conn.a().clone(), conn.b().clone()))
                )
                ;

            // calculate weights starting from personB
            let weights = bi_knows
                .join_map( // join messages of personB
                    &has_creator.map(|conn| (conn.b().clone(), conn.a().clone())),
                    |pb, pa, m| (m.clone(), (pb.clone(), pa.clone())),
                )
                .join_map( // join a reply of the message
                    &reply_of.map(|conn| (conn.b().clone(), conn.a().clone())),
                    |_mp, (pb, pa), mc| (mc.clone(), (pb.clone(), pa.clone()))
                )
                .join_map( // join creator of last message (personA)
                    &has_creator.map(|conn| (conn.a().clone(), conn.b().clone())),
                    |_m, (pb, pa), pm| (pb.clone(), pa.clone(), pm.clone())
                )
                .filter( // check if the last message's creator is the other person we started with
                    |(_pb, pa, pm)| pa.eq(pm)
                )
                // drop duplicated message creator, and make sure the lower id is the first in the tuple
                // this is needed for the aggregation
                .map(
                    |(pb, pa, _)| (min(pa, pb), max(pa, pb))
                )
                // aggregate (p1, p2) pairs,
                // which will result in the number of interactions between these people
                .count()
                // map result for next steps
                .map(
                    // fixme: hack solution as floats cannot be used directly (not implementing Ord)
                    |((p1, p2), c)| (p1, (p2, (((1.0/c as f32)*10000000000.0) as isize)))
                )
                ; // -> (src, (dst, weight))

            // create bidirectional weights
            let weights = weights
                .concat(&weights.map(|(p1, (p2, c))| (p2, (p1, c))))
                ;

            // root nodes are people from city one.
            // ((p, p) 0) represents an initial path from p to p with 0 weight.
            let nodes = people1
                .map(|p| ((p, p), 0))
                ;

            // calculate shortest paths from people in city 1
            // based on https://github.com/frankmcsherry/blog/blob/master/posts/2019-05-20.md
            let shortest_paths = nodes
                .iterate(|dists| { // calculate shortest path to every other node
                    let edges = weights.enter(&dists.scope());
                    let nodes = nodes.enter(&dists.scope());

                    dists // -> ((src, dst), distance)
                        .map(|((root, dst), dist)| (dst.clone(), (root.clone(), dist.clone())))
                        // join next step and calculate weights
                        .join_map(
                            &edges,
                            |_src, (root, distance), (dst, weight)| ((root.clone(), dst.clone()), distance + weight)
                        ) // -> ((root, dst), distance),
                        // add original nodes
                        .concat(&nodes) // -> ((root, dst), distance)

                        // Timely magic, to speed up updates with some time manipulation (see blogpost above)
                        .inner
                        .map_in_place(|((_d, w), t, _r)|
                            t.inner = std::cmp::max(t.inner, *w as u64)
                        )
                        .delay(|(_, t, _), _| t.clone())
                        .as_collection()

                        // finally keep only the shortest path between two nodes (grouped by (src, dst))
                        .reduce(|_key, input, output| output.push((*input[0].0, 1)))
                }) // -> ((src, dst), distance)
                // map for semijoin
                .map(|((src, dst), distance)| (dst, (src, distance)))
                // filter out result which are not between people from city1 and city2
                .semijoin(&people2)
                ;

            // add sorting options and final results
            let result = shortest_paths
                .map(|(dst, (src, distance))| (
                    (std::isize::MAX - distance, src, dst), // sort: -distance, +src, +dst
                    vec![src.to_string(), dst.to_string(), (distance as f64/10000000000.0).to_string()]
                ))
                ;

            let arrangement = limit(&result, 20)
                .arrange_by_self();

            arrangement.stream.probe_with(&mut probe);

            return (
                arrangement.trace,
                located_in_input, knows_input, has_creator_input, reply_of_input
            );
        });

        // add inputs
        let mut next_time: usize = 1;

        input_insert_vec(
            load_dynamic_connection("dynamic/person_isLocatedIn_place_0_0.csv", path.as_str(), index, peers),
            &mut located_in_input,
            next_time
        );

        input_insert_vec(
          load_dynamic_connection("dynamic/person_knows_person_0_0.csv", path.as_str(), index, peers),
            &mut knows_input,
            next_time
        );

        // insert hasCreator relations
        input_insert_vec(
            load_dynamic_connection("dynamic/post_hasCreator_person_0_0.csv", path.as_str(), index, peers),
            &mut has_creator_input,
            0 // do not advance just yet
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_hasCreator_person_0_0.csv", path.as_str(), index, peers),
            &mut has_creator_input,
            next_time
        );
        // insert replyOf relations
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_replyOf_post_0_0.csv", path.as_str(), index, peers),
            &mut reply_of_input,
            0 // do not advance just yet
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_replyOf_comment_0_0.csv", path.as_str(), index, peers),
            &mut reply_of_input,
            next_time
        );

        eprintln!("LOADED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // Compute...
        while probe.less_than(knows_input.time()) {
            worker.step();
        }

        eprintln!("CALCULATED;{:.10}", timer.elapsed().as_secs_f64());

        // print results
        print_trace(&mut trace, next_time);

        if change_path.eq(&"-".to_string()) {
            eprintln!("No change set was given.");
            return;
        }

        println!(" ---------------------------------------------------------------------- ");

        // introduce change set
        next_time += 1;
        timer = Instant::now();

        // parse change set file
        for mut change_row in load_data(change_path.as_str(), index, peers) {
            let create = match change_row.remove(0).as_str() {
                "create" => true,
                "remove" => false,
                x => { panic!("Unknown change. It should be 'remove' or 'create': {}", x); }
            };

            let input = change_row.remove(0);

            let mut row_iter = change_row.into_iter();
            let created = parse_datetime(row_iter.next().unwrap());
            let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
            let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
            let d = DynamicConnection::new(created, id1, id2);

            match input.as_str() {
                "person-knows-person" => {
                    if create {
                        knows_input.insert(d);
                    } else {
                        knows_input.remove(d);
                    }
                },
                "person-islocatedin-place" => {
                    if create {
                        located_in_input.insert(d);
                    } else {
                        located_in_input.remove(d);
                    }
                }
                x => { panic!("Unknown change type: {}", x); }
            }
        }

        // advance and flush all inputs...
        has_creator_input.advance_to(next_time);
        has_creator_input.flush();
        reply_of_input.advance_to(next_time);
        reply_of_input.flush();
        knows_input.advance_to(next_time);
        knows_input.flush();
        located_in_input.advance_to(next_time);
        located_in_input.flush();

        // Compute change set...
        while probe.less_than(&next_time) {
            worker.step();
        }

        eprintln!("CHANGE_CALCULATED;{:.10}", timer.elapsed().as_secs_f64());

        // print changed results
        print_trace(&mut trace, next_time);
    }).expect("Timely computation failed");
}