extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::{create_and_subscribe, KafkaConsumer};
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::backends::Consumer;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

#[tokio::main]
async fn main() {
    let config = KafkaConfig::new_consumer_config(
        vec!["localhost:9092".to_string()],
        "my_group".to_string(),
        "latest".to_string(),
        false,
        None,
    );
    let mut consumer = create_and_subscribe(Box::new(EmptyCallbacks {}), config).unwrap();
    let topic = Topic {
        name: "test_static".to_string(),
    };
    let res = consumer.subscribe(&[topic]);
    assert!(res.is_ok());
    println!("Subscribed");
    for _ in 0..20 {
        println!("Polling");
        let res = consumer.poll(None).await;
        match res.unwrap() {
            Some(x) => {
                println!("MSG {}", x)
            }
            None => {}
        }
    }
}
