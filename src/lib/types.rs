use std::cmp::Ordering;

pub type Date = i64;
pub type Id = u64;  // todo

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Person {
        pub id: Id,
        created: Date,
        pub first_name: String,
        pub last_name: String,
        gender: String,
        birthday: String,
        ip: String,
        browser: String
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Forum {
        pub id: Id,
        pub created: Date,
        pub title: String,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Post {
        pub id: Id,
        pub created: Date,
        image: String,
        ip: String,
        browser: String,
        lang: String,
        content: String,
        length: usize,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Comment {
        pub id: Id,
        created: Date,
        ip: String,
        browser: String,
        content: String,
        length: usize,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Tag {
        pub id: Id,
        pub name: String,
        url: String
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Connection {
        pub a: Id,
        pub b: Id,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct DynamicConnection {
        pub created: Date,
        pub a: Id,
        pub b: Id,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Place {
        pub id: Id,
        pub name: String,
        url: String,
        pub type_: String
    }
);


pub type TagClass = Tag;

// todo remove duplicate implementations?
impl Ord for Person {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Forum {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Post {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Place {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Connection {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for DynamicConnection {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}

impl Ord for Comment {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}
