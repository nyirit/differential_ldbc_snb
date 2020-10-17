use super::types::*;
use unwrap::unwrap;

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

pub fn parse_datetime(date: String) -> Date {
    let parsed = chrono::DateTime::parse_from_rfc3339(date.as_str());
    return unwrap!(parsed, "Failed to parse DateTime: '{}'", date).timestamp();
}

pub fn load_person(base_path: &str, index: usize, peers: usize) -> Vec<Person> {
    let data = load_data(&format!("{}dynamic/person_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Person>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_datetime(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let first_name = row_iter.next().unwrap().parse::<String>().unwrap();
        let last_name = row_iter.next().unwrap().parse::<String>().unwrap();
        let gender = row_iter.next().unwrap().parse::<String>().unwrap();
        let birthday = row_iter.next().unwrap();
        let location_ip = row_iter.next().unwrap().parse::<String>().unwrap();
        let browser_used = row_iter.next().unwrap().parse::<String>().unwrap();

        result.push(Person::new(id, created, first_name, last_name, gender, birthday, location_ip, browser_used));
    }

    return result;
}

pub fn load_forum(base_path: &str, index: usize, peers: usize) -> Vec<Forum> {
    let data = load_data(&format!("{}dynamic/forum_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Forum>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_datetime(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let title = row_iter.next().unwrap().parse::<String>().unwrap();
        result.push(Forum::new(id, created, title));
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
        let created = parse_datetime(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let image = row_iter.next().unwrap().parse::<String>().unwrap();
        let ip = row_iter.next().unwrap().parse::<String>().unwrap();
        let browser = row_iter.next().unwrap().parse::<String>().unwrap();
        let lang = row_iter.next().unwrap().parse::<String>().unwrap();
        let content = row_iter.next().unwrap().parse::<String>().unwrap();
        let length = row_iter.next().unwrap().parse().unwrap();
        result.push(Post::new(id, created, image, ip, browser, lang, content, length));
    }

    return result;
}

// FIXME
#[allow(dead_code)]
pub fn load_comment(base_path: &str, index: usize, peers: usize) -> Vec<Comment> {
    let data = load_data(&format!("{}dynamic/comment_0_0.csv", base_path), index, peers);

    let mut result = Vec::<Comment>::new();

    for row in data.into_iter() {
        let mut row_iter = row.into_iter();
        let created = parse_datetime(row_iter.next().unwrap());
        let id = row_iter.next().unwrap().parse::<Id>().unwrap();
        let ip = row_iter.next().unwrap().parse::<String>().unwrap();
        let browser = row_iter.next().unwrap().parse::<String>().unwrap();
        let content = row_iter.next().unwrap().parse::<String>().unwrap();
        let length = row_iter.next().unwrap().parse().unwrap();
        result.push(Comment::new(id, created, ip, browser, content, length));
    }

    return result;
}

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
        let created = parse_datetime(row_iter.next().unwrap());
        let id1 = row_iter.next().unwrap().parse::<Id>().unwrap();
        let id2 = row_iter.next().unwrap().parse::<Id>().unwrap();
        result.push(DynamicConnection::new(created, id1, id2));
    }

    return result;
}

#[allow(dead_code)]
pub fn load_bi_param(base_path: &str, bi_number: usize) -> Vec<Vec<String>> {
    return load_data(&format!("{}substitution_parameters/bi_{}_param.txt", base_path, bi_number), 0, 1);
}
