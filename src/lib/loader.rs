use super::types::{Post, Date, Id};

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

pub fn load_post(filename: &str, index: usize, peers: usize) -> Vec<Post> {
    let data = load_data(filename, index, peers);

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
