use rdkafka::config::ClientConfig as RdKafkaConfig;
use std::collections::HashMap;

const DEFAULT_QUEUED_MAX_MESSAGE_KBYTES: u32 = 50_000;
const DEFAULT_QUEUED_MIN_MESSAGES: u32 = 10_000;

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    config_map: HashMap<String, String>,
}

impl KafkaConfig {
    pub fn new_config(bootstrap_servers: Vec<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("bootstrap.servers".to_string(), bootstrap_servers.join(","));

        Self { config_map: config }
    }

    pub fn new_consumer_config(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: String,
    ) -> Self {
        let mut config = KafkaConfig::new_config(bootstrap_servers);
        config.config_map.insert("group.id".to_string(), group_id);
        config
            .config_map
            .insert("enable.auto.commit".to_string(), "false".to_string());
        config.config_map.insert(
            "auto.offset.reset".to_string(),
            auto_offset_reset.to_string(),
        );
        config.config_map.insert(
            "queued.max.messages.kbytes".to_string(),
            DEFAULT_QUEUED_MAX_MESSAGE_KBYTES.to_string(),
        );
        config.config_map.insert(
            "queued.min.messages".to_string(),
            DEFAULT_QUEUED_MIN_MESSAGES.to_string(),
        );
        config
    }

    pub fn new_producer_config(bootstrap_servers: Vec<String>) -> Self {
        KafkaConfig::new_config(bootstrap_servers)
    }

    pub fn set_queued_max_messages_kbytes(&mut self, max_messages_kbytes: u32) {
        // Consumer configuration
        self.config_map.insert(
            "queued.max.messages.kbytes".to_string(),
            max_messages_kbytes.to_string(),
        );
    }

    pub fn set_queued_min_messages(&mut self, min_messages: u32) {
        // Consumer configuration
        self.config_map
            .insert("queued.min.messages".to_string(), min_messages.to_string());
    }

    pub fn set_strict_offset_reset(&mut self, _strict_offset_reset: bool) {
        // Consumer configuration
        unimplemented!();
    }

    pub fn set_override_param(&mut self, param: String, value: String) {
        self.config_map.insert(param, value);
    }
}

impl From<KafkaConfig> for RdKafkaConfig {
    fn from(item: KafkaConfig) -> Self {
        let mut config_obj = RdKafkaConfig::new();
        for (key, val) in item.config_map.iter() {
            config_obj.set(key, val);
        }
        config_obj
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaConfig, DEFAULT_QUEUED_MIN_MESSAGES};
    use rdkafka::config::ClientConfig as RdKafkaConfig;

    #[test]
    fn test_build_consumer_configuration() {
        let mut config = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
            "my-group".to_string(),
            "error".to_string(),
        );

        config.set_queued_max_messages_kbytes(1_000_000);
        let rdkafka_config: RdKafkaConfig = config.into();
        assert_eq!(
            rdkafka_config.get("queued.min.messages"),
            Some(&DEFAULT_QUEUED_MIN_MESSAGES.to_string()[..])
        );
        assert_eq!(
            rdkafka_config.get("queued.max.messages.kbytes"),
            Some("1000000")
        );
    }
}
