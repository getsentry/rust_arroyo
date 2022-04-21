extern crate rust_arroyo;

use rust_arroyo::backends::kafka::KafkaConsumer;
use std::collections::{HashMap, HashSet};
use rust_arroyo::backends::{AssignmentCallbacks};
use rust_arroyo::types::{Partition, Topic, Position};
use rust_arroyo::backends::Consumer;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

fn main() {
    let config = HashMap::from([
        ("group.id".to_string(), "my_group".to_string()),
        ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
    ]);
    let mut consumer = KafkaConsumer::new("my_group".to_string(), config);
    let topic = Topic {name: "test_static".to_string()};
    let res = consumer.subscribe(&vec![topic], Box::new(EmptyCallbacks{}));
    assert_eq!(res.is_ok(), true);
    println!("Subscribed");
    for x in 0..20 {
        println!("Polling");
        let res = consumer.poll(None);
        match res.unwrap() {
            Some(x) => {println!("MSG {}", x)},
            None => {}
        }
    }
}
