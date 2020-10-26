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

pub fn run(path: String, params: &Vec<String>) {
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
        let next_time: usize = 1;
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

        eprintln!("CALCULATED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // print results
        print_trace(&mut trace, next_time);
    }).expect("Timely computation failed");
}