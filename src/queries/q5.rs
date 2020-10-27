/*
LDBC SNB BI query 5. Most active posters of a given topic
https://ldbc.github.io/ldbc_snb_docs_snapshot/bi-read-05.pdf
*/
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Join, Count, Threshold};
use timely::dataflow::ProbeHandle;

use crate::lib::loader::*;
use crate::lib::types::*;
use crate::lib::helpers::{input_insert_vec, limit, print_trace};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::operators::Probe;
use std::time::Instant;

pub fn run(path: String, change_path: String, params: &Vec<String>) {
    // unpack parameters
    let param_tag_ = params[0].clone();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // bind parameters
        let param_tag = param_tag_.clone();

        // create dataflow
        let (
            mut trace,
            mut tag_input,
            mut has_tag_input,
            mut has_creator_input,
            mut likes_input,
            mut reply_of_input,
        ) =
        worker.dataflow::<usize,_,_>(|scope| {
            let (tag_input, tag) = scope.new_collection::<Tag, _>();

            // tags for comments AND posts
            let (has_tag_input, has_tag) = scope.new_collection::<DynamicConnection, _>();
            // creators for comments AND posts
            let (has_creator_input, has_creator) = scope.new_collection::<DynamicConnection, _>();
            // likes for comments AND posts
            let (likes_input, likes) = scope.new_collection::<DynamicConnection, _>();
            // replyOf for comments AND posts
            let (reply_of_input, reply_of) = scope.new_collection::<DynamicConnection, _>();

            // get the id of the given Tag.
            let needed_tag = tag
                .filter(move |x| param_tag.eq(x.name()))
                .map(|tag| tag.id().clone());

            // collect all the messages (posts and comments) which were created with the given Tag
            let messages = has_tag
                .map(|conn| (conn.b().clone(), conn.a().clone()))
                .semijoin(&needed_tag)
                .map(|(_tagid, messageid)| (messageid, ()))
                .join_map(
                    &has_creator.map(|conn| (conn.a().clone(), conn.b().clone())), // -> message_id, person_id
                    |message_id, _dummy, person_id| (message_id.clone(), person_id.clone())
                )
                ; // -> (message_id, creator_id)

            // count the messages per person
            let score_messages = messages
                .map(|(_msgid, person_id)| person_id.clone())
                .count()
                ; // -> (person_id, message_count)

            // count likes on each post
            let score_likes = likes
                .map(|conn| (conn.b().clone(), conn.a().clone())) // -> message_id, liker_person_id
                .distinct() // in case someone liked, disliked, and like again the same message
                .join_map(
                    &messages,
                    |_message_id, _liker_person, message_creator| message_creator.clone()
                ) // join creator for messages
                .concat(
                    &score_messages.map(|(person_id, _message_count) : (Id, isize)| person_id)
                ) // make sure to have each person, so add an extra value
                .count()
                ; // -> (person_id, like_count)

            // count replies
            let score_replies = reply_of
                .map(|conn| (conn.b().clone(), ())) // original_message_id
                .join_map(
                    &messages,
                    |_message_id, _dummy, message_creator| message_creator.clone()
                )
                .concat(
                    &score_messages.map(|(person_id, _message_count)| person_id.clone())
                ) // make sure to have each person, so add an extra value
                .count()
                ; // -> (person_id, reply_count)

            let result = score_messages
                .map(|(person, score)| (person, (score,)))
                .join(&score_likes.map(|(person, score)| (person, (score,))))
                .join(&score_replies.map(|(person, score)| (person, (score,))))
                .map(
                    |(person_id, (((messages,), (likes,)), (replies,)))|
                        (
                            (std::isize::MAX - (messages + 2*(replies-1) + 10*(likes-1)), person_id), // sort: -sort, +person_id
                            vec![
                                person_id.to_string(), (replies-1).to_string(), (likes-1).to_string(), messages.to_string(),
                                (messages + 2*(replies-1) + 10*(likes-1)).to_string()
                            ]
                        )
                ) // map to final format
                ;

            let arrangement = limit(&result, 100)
                .arrange_by_self();

            arrangement.stream.probe_with(&mut probe);

            return (
                arrangement.trace,
                tag_input, has_tag_input, has_creator_input, likes_input, reply_of_input,
            );
        });

        // add inputs
        let mut next_time: usize = 1;
        input_insert_vec(load_tag(path.as_str(), index, peers), &mut tag_input, next_time);

        // insert hasTag relations
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
        // insert likes relations
        input_insert_vec(
            load_dynamic_connection("dynamic/person_likes_post_0_0.csv", path.as_str(), index, peers),
            &mut likes_input,
            0 // do not advance just yet
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/person_likes_comment_0_0.csv", path.as_str(), index, peers),
            &mut likes_input,
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

            match input.as_str() {
                "person-likes-message" => {
                    let mut row_iter = change_row.into_iter();
                    let created = parse_datetime(row_iter.next().unwrap());
                    let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
                    let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
                    let d = DynamicConnection::new(created, id1, id2);
                    if create {
                        likes_input.insert(d);
                    } else {
                        likes_input.remove(d);
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
        has_creator_input.advance_to(next_time);
        has_creator_input.flush();
        likes_input.advance_to(next_time);
        likes_input.flush();
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