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

pub fn run(path: String, params: &Vec<String>) {
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
                mut located_in_input,
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
        let next_time: usize = 1;
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
        while probe.less_than(tag_classes_input.time()) {
            worker.step();
        }

        eprintln!("CALCULATED;{:.20}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // print results
        print_trace(&mut trace, next_time);
    }).expect("Timely computation failed");
}
