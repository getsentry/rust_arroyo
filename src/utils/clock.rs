use std::time::{Duration, SystemTime};
use std::thread::sleep;

pub trait Clock {
    fn time(&self) -> SystemTime;

    fn sleep(self, duration: Duration);
}

pub struct SystemClock {}
impl Clock for SystemClock {
    fn time(&self) -> SystemTime {
        SystemTime::now()
    }

    fn sleep(self, duration: Duration) {
        sleep(duration)
    }
}