use super::types::*;

fn load_data(filename: &str, index: usize, peers: usize) -> Vec<Vec<String>> {
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut data = Vec::new();

    let file = BufReader::new(File::open(filename).expect(&format!("Could open file {}", filename)));
    let lines = file.lines();
    for (count, readline) in lines.enumerate() {
        // skip header row
        if count == 0 {
            continue;
        }

        if count % peers == index {
            if let Ok(line) = readline {
                let text : Vec<String> = line.split('|').map(|x| x.to_string()).collect();
                data.push(text);
            }
        }
    }

    return data;
}

fn parse_date(date: String) -> Date {
    return chrono::DateTime::parse_from_rfc3339(date.as_str()).expect("Failed to parse DateTime").timestamp();
}

pub fn load_forum(base_path: &str, index: usize, peers: usize) -> Vec<Forum> {
    let data = load_data(&format!("{}dynamic/forum_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Forum>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_date(row_iter.next().unwrap());
        let deleted = parse_date(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let title = row_iter.next().unwrap().parse::<String>().unwrap();
        result.push(Forum::new(id, created, deleted, title));
    }

    return result;
}

// FIXME
#[allow(dead_code)]
pub fn load_post(base_path: &str, index: usize, peers: usize) -> Vec<Post> {
    let data = load_data(&format!("{}dynamic/post_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Post>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_date(row_iter.next().unwrap());
        let deleted = parse_date(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let image = row_iter.next().unwrap().parse::<String>().unwrap();
        let ip = row_iter.next().unwrap().parse::<String>().unwrap();
        let browser = row_iter.next().unwrap().parse::<String>().unwrap();
        let lang = row_iter.next().unwrap().parse::<String>().unwrap();
        let content = row_iter.next().unwrap().parse::<String>().unwrap();
        let length = row_iter.next().unwrap().parse().unwrap();
        result.push(Post::new(id, created, deleted, image, ip, browser, lang, content, length));
    }

    return result;
}

// FIXME
#[allow(dead_code)]
pub fn load_tag(base_path: &str, index: usize, peers: usize) -> Vec<Tag> {
    let data = load_data(&format!("{}static/tag_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Tag>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let name = row_iter.next().unwrap().parse::<String>().unwrap();
        let url = row_iter.next().unwrap().parse::<String>().unwrap();
        result.push(Tag::new(id, name, url));
    }

    return result;
}


pub fn load_tag_class(base_path: &str, index: usize, peers: usize) -> Vec<TagClass> {
    let data = load_data(&format!("{}static/tagclass_0_0.csv", base_path), index, peers);

    let mut result = Vec::<TagClass>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let name = row_iter.next().unwrap().parse::<String>().unwrap();
        let url = row_iter.next().unwrap().parse::<String>().unwrap();
        result.push(TagClass::new(id, name, url));
    }

    return result;
}

pub fn load_place(base_path: &str, index: usize, peers: usize)-> Vec<Place> {
    let data = load_data(&format!("{}static/place_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Place>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let name = row_iter.next().unwrap().parse::<String>().unwrap();
        let url = row_iter.next().unwrap().parse::<String>().unwrap();
        let tp = row_iter.next().unwrap().parse::<String>().unwrap();
        result.push(Place::new(id, name, url, tp));
    }

    return result;
}

pub fn load_connection(filename: &str, base_path: &str, index: usize, peers: usize) -> Vec<Connection> {
    let data = load_data(&format!("{}{}", base_path, filename), index, peers);

    let mut result = Vec::<Connection>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
        let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
        result.push(Connection::new(id1, id2));
    }

    return result;
}

pub fn load_dynamic_connection(filename: &str, base_path: &str, index: usize, peers: usize)
        -> Vec<DynamicConnection> {
    let data = load_data(&format!("{}{}", base_path, filename), index, peers);

    let mut result = Vec::<DynamicConnection>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_date(row_iter.next().unwrap());
        let deleted = parse_date(row_iter.next().unwrap());
        let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
        let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
        result.push(DynamicConnection::new(created, deleted, id1, id2));
    }

    return result;
}

pub fn load_bi_param(base_path: &str, bi_number: usize) -> Vec<Vec<String>> {
    return load_data(&format!("{}substitution_parameters/bi_{}_param.txt", base_path, bi_number), 0, 1);
}
