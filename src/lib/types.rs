use std::cmp::Ordering;

pub type Date = i64;
pub type Id = u64;  // todo

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

impl Ord for Post {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.field_values().cmp(&other.field_values());
    }
}