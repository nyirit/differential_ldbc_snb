/*
LDBC SNB BI query 3. Popular topics in a country
https://ldbc.github.io/ldbc_snb_docs_snapshot/bi-read-03.pdf
*/


use differential_dataflow::input::Input;
use differential_dataflow::operators::{Join, Count, Threshold};
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;

use crate::lib::loader::*;
use crate::lib::types::*;
use crate::lib::helpers::{limit, format_timestamp, input_insert_vec, print_trace};
use std::time::Instant;

pub fn run(path: String, change_path: String, params: &Vec<String>) {
    // unpack parameters
    let param_tag_class_ = params[0].clone();
    let param_country_ = params[1].clone();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // bind parameters
        let param_tag_class = param_tag_class_.clone();
        let param_country = param_country_.clone();

        // create dataflow
        let
            (
                mut trace,
                mut forum_input,
                mut tag_classes_input,
                mut tag_hastype_tagclass_input,
                mut post_hastag_tag_input,
                mut forum_cointainerof_post_input,
                mut place_input,
                mut place_is_part_of_place_input,
                mut forum_hasmod_input,
                mut located_in_input
            ) =
        worker.dataflow::<usize,_,_>(|scope| {
            let (forum_input, forum) = scope.new_collection::<Forum, _>();
            let (tag_classes_input, tag_classes) = scope.new_collection::<TagClass, _>();
            let (tag_hastype_tagclass_input, tag_hastype_tagclass) = scope.new_collection::<Connection, _>();
            let (post_hastag_tag_input, post_hastag_tag) = scope.new_collection::<DynamicConnection, _>();
            let (forum_cointainerof_post_input, forum_cointainerof_post) = scope.new_collection::<DynamicConnection, _>();
            let (place_input, place) = scope.new_collection::<Place, _>();
            let (place_is_part_of_place_input, place_is_part_of_place) = scope.new_collection::<Connection, _>();
            let (forum_hasmod_input, forum_hasmod) = scope.new_collection::<DynamicConnection, _>();
            let (located_in_input, located_in) = scope.new_collection::<DynamicConnection, _>();

            let tag_ids = tag_classes
                // filter for selected TagClass
                .filter(move |x| param_tag_class.eq(x.name()))
                .map(|tag| (tag.id().clone(), ()))
                // join Tag based on TagClass
                .join_map(
                    &tag_hastype_tagclass.map(|conn| (conn.b().clone(), conn.a().clone())),
                    |_tag_class_id, _dummy, c| c.clone(),
                );

            // get post count for forums, which contain the desired tagclass
            // (forum_id, count, title, created)
            let counted_posts = post_hastag_tag
                .map(|x| (x.b().clone(), x.a().clone()))  // (Tag.id, Post.id)
                .semijoin(&tag_ids)
                .map(|x| (x.1, ()))
                .distinct()  // one Post can have multiple Tags
                .join_map(
                    &forum_cointainerof_post.map(|x| (x.b().clone(), x.a().clone())),
                    |_post_id, _dummy, forum_id| forum_id.clone()
                )
                .count()
                .join_map(
                    &forum.map(|forum| (forum.id().clone(), (forum.title().clone(), forum.created().clone()))),
                    |forum_id, count:&isize, forum_data|
                        (forum_id.clone(), (*count, forum_data.0.clone(), forum_data.1.clone()))
                );

            let cities = place
                .filter(move |x| param_country.eq(x.name()))
                .map(|x| (x.id().clone(), ()))
                .join_map(
                    &place_is_part_of_place.map(|x| (x.b().clone(), x.a().clone())),
                    |_country, _dummy, city| city.clone()
                );

            let forums_in_cities = forum_hasmod
                .map(|forum_mod| (forum_mod.b().clone(), forum_mod.a().clone())) // -> (person.id, forum.id)
                .join_map(
                    &located_in.map(|x| (x.a().clone(), x.b().clone())),
                    |person, forum, place| (place.clone(), (forum.clone(), person.clone()))
                )
                .semijoin(&cities)
                .map(|(_place, (forum, person))| (forum.clone(), person.clone()));

            let result = counted_posts
                .join(&forums_in_cities)
                .map(|(forum_id, ((count, title, created), person_id))| (
                    (std::isize::MAX - count, forum_id), // sort: -count, +forum_id
                    vec![forum_id.to_string(), title, format_timestamp(created as u64),
                         person_id.to_string(), count.to_string()] // output vec
                ))
                ;

            let arrangement = limit(&result, 20)
                .arrange_by_self();

            arrangement.stream.probe_with(&mut probe);

            return (
                arrangement.trace,
                forum_input,
                tag_classes_input, tag_hastype_tagclass_input, post_hastag_tag_input,
                forum_cointainerof_post_input, place_input, place_is_part_of_place_input,
                forum_hasmod_input, located_in_input
            );
        });

        // add inputs
        let mut next_time: usize = 1;
        input_insert_vec(load_forum(path.as_str(), index, peers), &mut forum_input, next_time);
        input_insert_vec(load_tag_class(path.as_str(), index, peers), &mut tag_classes_input, next_time);
        input_insert_vec(
            load_connection("static/tag_hasType_tagclass_0_0.csv", path.as_str(), index, peers),
            &mut tag_hastype_tagclass_input,
            next_time
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/post_hasTag_tag_0_0.csv", path.as_str(), index, peers),
            &mut post_hastag_tag_input,
            next_time
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/forum_containerOf_post_0_0.csv", path.as_str(), index, peers),
            &mut forum_cointainerof_post_input,
            next_time
        );
        input_insert_vec(load_place(path.as_str(), index, peers), &mut place_input, next_time);
        input_insert_vec(
            load_connection("static/place_isPartOf_place_0_0.csv", path.as_str(), index, peers),
            &mut place_is_part_of_place_input,
            next_time
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/forum_hasModerator_person_0_0.csv", path.as_str(), index, peers),
            &mut forum_hasmod_input,
            next_time
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/person_isLocatedIn_place_0_0.csv", path.as_str(), index, peers),
            &mut located_in_input,
            next_time
        );

        eprintln!("LOADED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // Compute...
        while probe.less_than(&next_time) {
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
                "person-locatedin-place" => {
                    let mut row_iter = change_row.into_iter();
                    let created = parse_datetime(row_iter.next().unwrap());
                    let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
                    let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
                    let d = DynamicConnection::new(created, id1, id2);
                    if create {
                        located_in_input.insert(d);
                    } else {
                        located_in_input.remove(d);
                    }
                },
                x => { panic!("Unknown change type: {}", x); }
            }
        }

        // advance and flush all inputs...
        located_in_input.advance_to(next_time);
        located_in_input.flush();
        forum_input.advance_to(next_time);
        forum_input.flush();
        tag_classes_input.advance_to(next_time);
        tag_classes_input.flush();
        tag_hastype_tagclass_input.advance_to(next_time);
        tag_hastype_tagclass_input.flush();
        post_hastag_tag_input.advance_to(next_time);
        post_hastag_tag_input.flush();
        forum_cointainerof_post_input.advance_to(next_time);
        forum_cointainerof_post_input.flush();
        place_input.advance_to(next_time);
        place_input.flush();
        place_is_part_of_place_input.advance_to(next_time);
        place_is_part_of_place_input.flush();
        forum_hasmod_input.advance_to(next_time);
        forum_hasmod_input.flush();

        // Compute change set...
        while probe.less_than(&next_time) {
            worker.step();
        }

        eprintln!("CHANGE_CALCULATED;{:.10}", timer.elapsed().as_secs_f64());

        // print changed results
        print_trace(&mut trace, next_time);

    }).expect("Timely computation failed");
}
