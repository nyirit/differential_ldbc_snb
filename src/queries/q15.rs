/*
Legacy LDBC SNB BI query 15. Trusted connection paths through forums created in a given timeframe
https://ldbc.github.io/ldbc_snb_docs_snapshot/bi-read-15.pdf
*/

use timely::dataflow::{ProbeHandle, Scope};
use differential_dataflow::input::Input;
use differential_dataflow::operators::{Reduce, Join, Iterate, Consolidate, Threshold, Count};
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::order::Product;
use differential_dataflow::operators::iterate::Variable;

use crate::lib::helpers::input_insert_vec;
use crate::lib::loader::{load_dynamic_connection, load_forum, parse_datetime};
use crate::lib::types::*;
use std::time::Instant;

pub fn run(path: String, _change_path: String, params: &Vec<String>) {
    // FIXME: WIP not properly implemented.

    // Based on the work of Frank McSherry:
    // https://github.com/frankmcsherry/blog/blob/81e9555bbee110954f2c3d35caf86ea7e7612fa6/posts/2019-06-13.md

    // unpack params
    let param_person_a_ = params[0].clone();
    let param_person_b_ = params[1].clone();
    let param_from_ = params[2].clone();
    let param_to_ = params[3].clone();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = ProbeHandle::new();

        // bind param
        let param_person_a = param_person_a_.parse::<Id>().unwrap();
        let param_person_b = param_person_b_.parse::<Id>().unwrap();
        let param_from = parse_datetime(param_from_.clone());
        let param_to = parse_datetime(param_to_.clone());

        // create dataflow
        let (
            mut query_input,
            mut knows_input,
            mut post_hascreator_input,
            mut comment_hascreator_input,
            mut forum_containerof_post_input,
            mut reply_of_input,
            mut forum_input
        ) =
        worker.dataflow::<usize, _, _>(|scope| {
            let (query_input, query) = scope.new_collection::<((Id, Id),(Date, Date)), _>(); // FIXME shouldn't be a collection?
            let (knows_input, knows) = scope.new_collection::<DynamicConnection, _>();
            let (post_hascreator_input, post_hascreator) = scope.new_collection::<DynamicConnection, _>();
            let (forum_containerof_post_input, forum_containerof_post) = scope.new_collection::<DynamicConnection, _>();

            // replyOf for comments AND posts
            let (reply_of_input, reply_of) = scope.new_collection::<DynamicConnection, _>();

            let (comment_hascreator_input, comment_hascreator) = scope.new_collection::<DynamicConnection, _>();

            let (forum_input, forum) = scope.new_collection::<Forum, _>();

            // posts should be: (post_id, (creator_id, forum_id)
            let posts = post_hascreator
                .map(|conn| (conn.a().clone(), conn.b().clone())) // -> post_id, person_id
                .join(
                    &forum_containerof_post.map(|conn| (conn.b().clone(), conn.a().clone())) // -> post_id, forum_id
                )
                ; // -> post_id, (creator_id, forum_id)

            // comments should be: (comment_id, (creator_id, parent_id),
            // where parent_id can be a post id or comment id
            let comms = comment_hascreator
                .map(|conn| (conn.a().clone(), conn.b().clone())) // -> comment_id, person_id
                .join(
                    &reply_of.map(|conn| (conn.a().clone(), conn.b().clone())) // -> comment_id, parent_id
                )
                ; // -> comment_id, (creator_id, parent_id)


            // 1. Determine edges in shortest paths, for each query.
            let goals = query.map(|(goal,_bounds)| goal).distinct();
            let shortest_edges = shortest_paths(&knows.map(|conn| (conn.a().clone(), conn.b().clone())), &goals);

            // 2. Score each edge, broken down by the root post.
            let oriented_edges = shortest_edges.map(|(_,(x,y))| {
                let min = std::cmp::min(x, y);
                let max = std::cmp::max(x, y);
                (min, max)
            }).distinct();
            let edge_scores = score_edges(&oriented_edges, &posts, &comms);

            // orient edges in both directions
            let edge_scores = edge_scores.map(|((x,y),f)| ((y,x),f)).concat(&edge_scores);

            // 3. Merge queries and scores, filter by start and end dates.
            let _scored_edges =
            query
                .join_map(&shortest_edges, |&goal, &bounds, &edge| (edge, (goal, bounds)))
                .join_map(&edge_scores, |&edge, &(goal, bounds), &forum| (forum, (goal, edge, bounds)))
                .join_map(
                    &forum.map(|forum| (forum.id().clone(), forum.created().clone())),
                    |_forum, &(goal, edge, (from_bound, to_bound)), &time|
                        (time >= from_bound && time <= to_bound, goal, edge),
                )
                .filter(|x| x.0)
                .map(|(_, goal, edge)| (goal, edge))
                .concat(&shortest_edges)
                .count()
                .map(|(x,c)| (x,c-1))
                .inspect(|x| println!("SCORED: {:?}", x))
                .probe_with(&mut probe)
                ;

            // let filtered_edges: Collection<_, ((Node, Node), (Edge, usize))>
            //     = unimplemented!();

            // // 4. Reconstruct paths and scores.
            // let scored_paths: Collection<_, ((Node, Node), (Vec<Node>, usize))>
            //     = unimplemented!();

            // 5. Announce massive success!
            // shortest_edges.inspect(|x| println!("WOW:\t{:?}", x));

            return (
                query_input, knows_input, post_hascreator_input, comment_hascreator_input,
                forum_containerof_post_input, reply_of_input,
                forum_input
            );
        });

        eprintln!("LOADED;{:}", timer.elapsed().as_secs_f64());
        timer = Instant::now();

        // add inputs
        let next_time: usize  = 1;
        input_insert_vec(
            load_dynamic_connection("dynamic/person_knows_person_0_0.csv", path.as_str(), index, peers),
            &mut knows_input,
            next_time,
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/post_hasCreator_person_0_0.csv", path.as_str(), index, peers),
            &mut post_hascreator_input,
            next_time,
        );
        input_insert_vec(
            load_dynamic_connection("dynamic/forum_containerOf_post_0_0.csv", path.as_str(), index, peers),
            &mut forum_containerof_post_input,
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

        input_insert_vec(
            load_dynamic_connection("dynamic/comment_hasCreator_person_0_0.csv", path.as_str(), index, peers),
            &mut comment_hascreator_input,
            next_time,
        );

        input_insert_vec(load_forum(path.as_str(), index, peers), &mut forum_input, next_time);

        query_input.insert(( // FIXME
            (param_person_a, param_person_b),
            (param_from, param_to)
        ));
        query_input.advance_to(next_time);
        query_input.flush();

        eprintln!("CALCULATED;{:.10}", timer.elapsed().as_secs_f64());
        // timer = Instant::now();

        while probe.less_than(&1) {
            worker.step();
        }

    }).expect("Timely computation failed");
}

/// Describes shortest paths between query node pairs.
///
/// The `bijkstra` method takes as input a collection of edges (described as pairs
/// of source and target node) and a collection of query node pairs (again, source
/// and target nodes). Its intent is to describe the set of shortest paths between
/// each pair of query nodes.
///
/// The method's output describes a set of directed edges for each input query pair.
/// The set of directed edges are the directed acyclic graph of edges on shortest
/// paths from the source of the query pair to the target of the query pair.
/// There may be multiple paths, and the paths may fork apart and merge together.
fn shortest_paths<G>(
    edges: &Collection<G, (Id, Id)>,
    goals: &Collection<G, (Id, Id)>,
) -> Collection<G, ((Id, Id), (Id, Id))>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    // Iteratively develop reachability information.
    edges.scope().iterative::<u64, _, _>(|inner| {

        // Our plan is to start evolving distances from both sources and destinations.
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        let goals = goals.enter(inner);
        let edges = edges.enter(inner);

        // forward: ((mid1,mid2), (src, len)) can be read as
        //      src -len-> mid1 -> mid2 is a shortest path from src to mid2.
        let forward = Variable::new(inner, Product::new(Default::default(), 1));
        // reverse: ((mid1,mid2), (dst, len)) can be read as
        //      mid1 -> mid2 -len-> dst is a shortest path from mid1 to dst.
        let reverse = Variable::new(inner, Product::new(Default::default(), 1));

        // reached((src, dst), (mid1, mid2)) can be read as
        //      src -*-> mid1 -> mid2 -*-> dst is a shortest path.
        let reached: Collection<_, ((Id, Id), (Id, Id))> =
        forward
            .join_map(&reverse, |&(m1,m2), &(src,len1), &(dst,len2)| {
                ((src, dst), (len1 + len2, (m1,m2)))
            })
            .semijoin(&goals)
            .reduce(|&_src_dst, source, target| {
                let min_len = (source[0].0).0;
                for &(&(len,edge),_wgt) in source.iter() {
                    if len == min_len {
                        target.push((edge, 1));
                    }
                }
            });

        // reached.inspect(|x| println!("\tREACHED\t{:?}", x));

        // Subtract from goals any goal pairs that can reach each other.
        let active =
        reached
            .map(|((src,dst),_mid)| (src,dst))
            .distinct()
            .negate()
            .concat(&goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x, _y)| x).distinct();
        let forward_next = forward
            .map(|((_mid0,mid1), (src, len))| (src, (mid1, len)))
            .semijoin(&forward_active)
            .map(|(src, (mid1, len))| (mid1, (src, len)))
            .join_map(&edges, |&mid1, &(src, len), &mid2| {
                ((mid1,mid2), (src, len + 1))
            })
            .concat(&*forward)
            .map(|((mid1,mid2),(src,len))| ((mid2,src),(len,mid1)))
            .reduce(|_key, s, t| {
                let min_len = (s[0].0).0;
                for (&(len,mid1), _weight) in s.iter() {
                    if len == min_len {
                        t.push(((len, mid1), 1));
                    }
                }
            })
            .map(|((mid2, src), (len, mid1))| ((mid1,mid2),(src,len)))
            ;

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x, y)| y).distinct();
        let reverse_next = reverse
            .map(|((mid1,_mid2), (rev, len))| (rev, (mid1, len)))
            .semijoin(&reverse_active)
            .map(|(rev, (mid1, len))| (mid1, (rev, len)))
            .join_map(&edges.map(|(x, y)| (y, x)), |&mid1, &(rev, len), &mid0| {
                ((mid0,mid1), (rev, len + 1))
            })
            .concat(&reverse)
            .map(|((mid0,mid1),(rev,len))| ((mid0,rev),(len,mid1)))
            // .map(|(edge, (rev, len))| ((edge, rev), len))
            .reduce(|_key, s, t| {
                let min_len = (s[0].0).0;
                for (&(len,mid1), _weight) in s.iter() {
                    if len == min_len {
                        t.push(((len, mid1), 1));
                    }
                }
            })
            .map(|((mid0, rev), (len, mid1))| ((mid0,mid1), (rev, len)));

        // `reached` are edges on a shortest path;
        // we want to unwind them back to their roots.

        // reached((src, dst), (mid1, mid2)) means that
        //      src -*-> mid1 -> mid2 -*-> dst is a shortest path.

        // forward_back(src, dst, )
        let shortest = Variable::new(inner, Product::new(Default::default(), 1));

        let forward_dag = forward.map(|((mid1,mid2),(src,_len))| ((src,mid2),mid1));
        let reverse_dag = reverse.map(|((mid1,mid2),(dst,_len))| ((dst,mid1),mid2));

        let short_forward =
        shortest
            .map(|((src,dst),(mid1,_mid2))| ((src,mid1),dst))
            .join_map(&forward_dag, |&(src,mid1),&dst,&mid0| ((src,dst),(mid0,mid1)));

        let short_reverse =
        shortest
            .map(|((src,dst),(_mid0,mid1))| ((dst,mid1),src))
            .join_map(&reverse_dag, |&(dst,mid1),&src,&mid2| ((src,dst),(mid1,mid2)));

        let short =
        short_forward
            .concat(&short_reverse)
            .concat(&reached)
            .distinct();

        shortest.set(&short);

        forward.set(&forward_next.concat(&goals.map(|(x, _)| ((x,x),(x,0)))));
        reverse.set(&reverse_next.concat(&goals.map(|(_, y)| ((y,y),(y,0)))));

        short
            .filter(|(_,(x,y))| x != y)
            .leave()
    })
}

/// Assigns a score to each edge, based on posts and comments.
///
/// This method assigns an integer score to each edge, where an edge (src, tgt) gets
/// one point for each reply by either src or tgt to a comment of the other, and two
/// points for each reply by either src or tgt to a post of the other.
///
/// The result type is a multiset of (Edge, Post), where the multiplicity of
/// each record indicates the score the edge derives from the post.
/// One may filter by post and accumulate to get a final score.
fn score_edges<G>(
    edges: &Collection<G, (Id, Id)>,                    // (source, target)
    posts: &Collection<G, (Id, (Id, Id))>,   // (id, author, in_forum)
    comms: &Collection<G, (Id, (Id, Id))>,    // (id, author, reply_to)
) -> Collection<G, ((Id, Id), Id)>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{

    use differential_dataflow::operators::arrange::ArrangeByKey;
    let comms_by_link = comms
        .map(|(_id, (auth, link))| (link, auth))
         .arrange_by_key()
        ;

    // Perhaps a comment links to a post ...
    let comm_post = comms_by_link
        .join_map(&posts, |_post, &auth_c, &(auth_p, forum)| {
            let min = std::cmp::min(auth_c, auth_p);
            let max = std::cmp::max(auth_c, auth_p);
            ((min, max), forum)
        })
        .semijoin(&edges)
        ;

    // Perhaps a comment links to a comment ...
    let comm_comm = comms_by_link
        .join_map(&comms, |_comm, &auth_c, &(auth_p,link)| {
            let min = std::cmp::min(auth_c, auth_p);
            let max = std::cmp::max(auth_c, auth_p);
            ((min, max), link)
        })
        .semijoin(&edges)
        ;

    // All comment -> parent links.
    let links = comms
        .map(|(id, (_, link))| (id, link))
        .concat(&posts.map(|(id, _)| (id, id)))
        ;

    // unroll
    let scores = comm_comm
        .map(|(edge, link)| (link, edge))
        .iterate(|scores|
            links.enter(&scores.scope())
                 .join_map(&scores, |_src, &dst, &edge| (dst, edge))
        )
        .join_map(&posts, |_post,&edge,&(_, forum)| (edge, forum))
        .concat(&comm_post)
        .concat(&comm_post)
        .consolidate()
        ;

    return scores;
}