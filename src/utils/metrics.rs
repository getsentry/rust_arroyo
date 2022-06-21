use cadence::{
    BufferedUdpMetricSink, Counted, Gauged, QueuingMetricSink, StatsdClient, Timed, UdpMetricSink,
};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::Arc;

pub trait Metrics {
    fn counter(&self, key: &str, value: Option<i64>, tags: Option<HashMap<&str, &str>>);

    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);

    fn time(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>);
}

pub struct MetricsClient {
    pub statsd_client: StatsdClient,
    prefix: String,
}

impl Metrics for MetricsClient {
    fn counter(&self, key: &str, value: Option<i64>, tags: Option<HashMap<&str, &str>>) {
        let count_value: i64;
        if value.is_none() {
            count_value = 1;
        } else {
            count_value = value.unwrap();
        }

        if tags.is_none() {
            let result = self.statsd_client.count(key, count_value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        } else {
            assert!(tags.is_some());
            let mut metric_builder = self.statsd_client.count_with_tags(key, count_value);
            for (key, value) in tags.unwrap() {
                metric_builder = metric_builder.with_tag(key, value);
            }
            let result = metric_builder.try_send();
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        }
    }

    fn gauge(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if tags.is_none() {
            let result = self.statsd_client.gauge(key, value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        } else {
            assert!(tags.is_some());
            let mut metric_builder = self.statsd_client.gauge_with_tags(key, value);
            for (key, value) in tags.unwrap() {
                metric_builder = metric_builder.with_tag(key, value);
            }
            let result = metric_builder.try_send();
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        }
    }

    fn time(&self, key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
        if tags.is_none() {
            let result = self.statsd_client.time(key, value);
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        } else {
            assert!(tags.is_some());
            let mut metric_builder = self.statsd_client.time_with_tags(key, value);
            for (key, value) in tags.unwrap() {
                metric_builder = metric_builder.with_tag(key, value);
            }
            let result = metric_builder.try_send();
            match result {
                Ok(_) => {}
                Err(_err) => {
                    println!("Failed to send metric {}: {}", key, _err)
                }
            }
        }
    }
}

lazy_static! {
    static ref METRICS_CLIENT: RwLock<Option<Arc<MetricsClient>>> = RwLock::new(None);
}

const METRICS_MAX_QUEUE_SIZE: usize = 1024;

pub fn init<A: ToSocketAddrs>(prefix: &str, host: A) {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();

    let buffered_udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
    let queuing_sink = QueuingMetricSink::with_capacity(buffered_udp_sink, METRICS_MAX_QUEUE_SIZE);
    // If metrics need to be sent out immediately without waiting then use the udp_sink below.
    //let udp_sink = UdpMetricSink::from(host, socket).unwrap();
    let statsd_client = StatsdClient::from_sink(prefix, queuing_sink);

    let metrics_client = MetricsClient {
        statsd_client,
        prefix: String::from(prefix),
    };
    *METRICS_CLIENT.write() = Some(Arc::new(metrics_client));
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn increment(key: &str, value: Option<i64>, tags: Option<HashMap<&str, &str>>) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .counter(key, value, tags);
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn gauge(key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .gauge(key, value, tags);
}

// TODO: Remove cloning METRICS_CLIENT each time this is called using thread local storage.
pub fn time(key: &str, value: u64, tags: Option<HashMap<&str, &str>>) {
    METRICS_CLIENT
        .read()
        .clone()
        .unwrap()
        .time(key, value, tags);
}

#[cfg(test)]
mod tests {
    use crate::utils::metrics::{gauge, increment, init, time};
    use std::collections::HashMap;

    #[test]
    fn test_metrics() {
        init("my_host", "0.0.0.0:8125");
        increment("a", Some(1), Some(HashMap::from([("tag1", "value1")])));
        gauge(
            "b",
            20,
            Some(HashMap::from([("tag2", "value2"), ("tag4", "value4")])),
        );
        time("c", 30, Some(HashMap::from([("tag3", "value3")])));
    }
}
