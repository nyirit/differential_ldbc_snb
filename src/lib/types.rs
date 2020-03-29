use std::cmp::Ordering;

pub type Date = i64;
pub type Id = u64;  // todo

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Forum {
        pub id: Id,
        pub created: Date,
        deleted: Date,
        pub title: String,
    }
);

named_tuple!(
    #[derive(Clone, Debug, Default, Hash, PartialEq, PartialOrd, Eq)]
    pub struct Post {
        pub id: Id,
        created: Date,
        deleted: Date,
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
        deleted: Date,
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
        created: Date,
        deleted: Date,
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
