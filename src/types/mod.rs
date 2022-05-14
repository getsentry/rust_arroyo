use chrono::{DateTime, Utc};
use std::any::type_name;
use std::cmp::Eq;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
}

impl PartialEq for Topic {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Topic {}

impl Hash for Topic {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic({})", self.name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Partition {
    // TODO: Make this a reference to 'static Topic.
    pub topic: Topic,
    pub index: u16,
}

impl Eq for Partition {}

impl Hash for Partition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.index.hash(state);
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Partition({} topic={})", self.index, &self.topic)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, PartialEq)]
pub struct Message<T: Clone> {
    pub partition: Partition,
    pub offset: u64,
    pub payload: T,
    pub timestamp: DateTime<Utc>,
    pub next_offset: u64,
}

impl<T: Clone> Message<T> {
    pub fn new(
        partition: Partition,
        offset: u64,
        payload: T,
        timestamp: DateTime<Utc>,
        next_offset: Option<u64>,
    ) -> Self {
        Self {
            partition: partition,
            offset: offset,
            payload: payload,
            timestamp: timestamp,
            next_offset: match next_offset {
                Some(v) => v,
                None => offset + 1,
            },
        }
    }
}

impl<T: Clone> Clone for Message<T> {
    fn clone(&self) -> Message<T> {
        Message::new(
            self.partition.clone(),
            self.offset,
            self.payload.clone(),
            self.timestamp.clone(),
            Some(self.next_offset),
        )
    }
}

impl<T: Clone> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn type_of<V>(_: &V) -> String {
            format!("{}", type_name::<V>())
        }

        write!(
            f,
            "Message<{}>(partition={}), offset={}",
            type_of(&self.payload),
            &self.partition,
            &self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Message, Partition, Topic};
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn message_with_next() {
        let now = Utc::now();
        let topic = Topic {
            name: "test".to_string(),
        };
        let part = Partition {
            topic: topic,
            index: 10,
        };
        let message = Message::new(part, 10, "payload".to_string(), now, Some(20));

        assert_eq!(message.partition.topic.name, "test");
        assert_eq!(message.partition.index, 10);
        assert_eq!(message.offset, 10);
        assert_eq!(message.payload, "payload");
        assert_eq!(message.timestamp, now);
        assert_eq!(message.next_offset, 20)
    }

    #[test]
    fn message_without_next() {
        let now = Utc::now();
        let part = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 10,
        };
        let message = Message::new(part, 10, "payload".to_string(), now, None::<u64>);

        assert_eq!(message.next_offset, 11)
    }

    #[test]
    fn fmt_display() {
        let now = Utc::now();
        let part = Partition {
            topic: Topic {
                name: "test".to_string(),
            },
            index: 10,
        };
        let message = Message::new(part, 10, "payload".to_string(), now, None::<u64>);

        assert_eq!(
            message.to_string(),
            "Message<alloc::string::String>(partition=Partition(10 topic=Topic(test))), offset=10"
        )
    }

    #[test]
    fn test_eq() {
        let a = Topic {
            name: "test".to_string(),
        };
        let b = Topic {
            name: "test".to_string(),
        };
        assert!(a == b);

        let c = Topic {
            name: "test2".to_string(),
        };
        assert!(a != c);
    }

    #[test]
    fn test_hash() {
        let mut content = HashMap::new();
        content.insert(
            Topic {
                name: "test".to_string(),
            },
            "test_value".to_string(),
        );

        let b = Topic {
            name: "test".to_string(),
        };
        let c = content.get(&b).unwrap();
        assert_eq!(&"test_value".to_string(), c);
    }

    #[test]
    fn test_clone() {
        let topic = Topic {
            name: "test".to_string(),
        };
        let part = Partition {
            topic: topic,
            index: 10,
        };

        let part2 = part.clone();
        assert_eq!(part, part2);
        assert_ne!(&part as *const Partition, &part2 as *const Partition);

        let now = Utc::now();
        let message = Message::new(part, 10, "payload".to_string(), now, None::<u64>);
        let message2 = message.clone();

        assert_eq!(message, message2);
        assert_ne!(
            &message as *const Message<String>,
            &message2 as *const Message<String>
        );
    }
}
