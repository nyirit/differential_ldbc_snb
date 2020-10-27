/*
LDBC SNB BI query 7. Related topics
https://ldbc.github.io/ldbc_snb_docs_snapshot/bi-read-07.pdf
*/
use differential_dataflow::input::Input;
use timely::dataflow::ProbeHandle;

use crate::lib::types::*;
use crate::lib::loader::*;
use crate::lib::helpers::{input_insert_vec, limit, print_trace};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::operators::Probe;
use differential_dataflow::operators::{Join, Count};
use std::time::Instant;

pub fn run(path: String, change_path: String, params: &Vec<String>) {
    // unpack parameter
    let param_tag_ = params[0].clone();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // bind param
        let param_tag = param_tag_.clone();

        // create dataflow
        let (
            mut trace,
            mut tag_input,
            mut has_tag_input,
            mut reply_of_input,
        ) =
        worker.dataflow::<usize, _, _>(|scope| {
            let (tag_input, tag) = scope.new_collection::<Tag, _>();

            // replyOf for comments AND posts
            let (reply_of_input, reply_of) = scope.new_collection::<DynamicConnection, _>();

            // tags for comments AND posts
            let (has_tag_input, has_tag) = scope.new_collection::<DynamicConnection, _>();

            // get the id of the given Tag.
            let needed_tag = tag
                .filter(move |x| param_tag.eq(x.name()))
                .map(|tag| tag.id().clone());

            // collect all the messages (posts and comments) which were created with the given Tag
            let messages_with_tag = has_tag
                .map(|conn| (conn.b().clone(), conn.a().clone())) // -> tag_id, message_id
                .semijoin(&needed_tag)
                .map(|(_tagid, messageid)| messageid)
                ;

            let result = messages_with_tag
                .map(|message_id| (message_id, ()))
                .join_map(
                    &reply_of.map(|conn| (conn.b().clone(), conn.a().clone())), // -> parent_id, reply_id
                    |_parent_id, _dummy, message_id| (message_id.clone(), ())
                )
                .antijoin(&messages_with_tag)
                .join_map(
                    &has_tag.map(|conn| (conn.a().clone(), conn.b().clone())), // -> message_id, tag_id
                    |_message_id, _dummy, tag_id| tag_id.clone()
                )
                .count()
                .join_map(
                    &tag.map(|tag| (tag.id().clone(), tag.name().clone())),
                    |_tag_id, count: &isize, name: &String| (
                        (std::isize::MAX - count, name.clone()),  // sort: -count, +name
                        vec![count.to_string(), name.to_string()]         // output vec
                    )
                )
                ;


            let arrangement = limit(&result, 100)
                .arrange_by_self();

            arrangement.stream.probe_with(&mut probe);

            return (
                arrangement.trace,
                tag_input, has_tag_input, reply_of_input
            )
        });

        // add inputs...
        let mut next_time: usize = 1;
        input_insert_vec(load_tag(path.as_str(), index, peers), &mut tag_input, next_time);

        // insert hasTag relations both for posts and comments, to handle them together in the dataflow
        input_insert_vec(
            load_dynamic_connection("dynamic/post_hasTag_tag_0_0.csv", path.as_str(), index, peers),
            &mut has_tag_input,
            0 // do not advance just yet
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/comment_hasTag_tag_0_0.csv", path.as_str(), index, peers),
            &mut has_tag_input,
            next_time
        );
        // insert replyOf relations both for posts and comments, to handle them together in the dataflow
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
        while probe.less_than(tag_input.time()) {
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
                "comment-replyof-message" => {
                    if create {
                        reply_of_input.insert(d);
                    } else {
                        reply_of_input.remove(d);
                    }
                },
                "comment-hastag-tag" => {
                    if create {
                        has_tag_input.insert(d);
                    } else {
                        has_tag_input.remove(d);
                    }
                },
                x => { panic!("Unknown change type: {}", x); }
            }
        }

        // advance and flush all inputs...
        tag_input.advance_to(next_time);
        tag_input.flush();
        has_tag_input.advance_to(next_time);
        has_tag_input.flush();
        reply_of_input.advance_to(next_time);
        reply_of_input.flush();

        // Compute change set...
        while probe.less_than(&next_time) {
            worker.step();
        }

        eprintln!("CHANGE_CALCULATED;{:.10}", timer.elapsed().as_secs_f64());

        // print changed results
        print_trace(&mut trace, next_time);
    }).expect("Timely computation failed");
}