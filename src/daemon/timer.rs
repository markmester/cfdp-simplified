use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Counter {
    start_time: Instant,
    timeout: Duration,
    max_count: u32,
    count: u32,
    occurred: bool,
    paused: bool,
}
impl Counter {
    pub fn new(timeout: Duration, max_count: u32) -> Self {
        Self {
            max_count,
            timeout,
            count: 0,
            start_time: Instant::now(),
            occurred: false,
            paused: true,
        }
    }
    /// start the timer (if it was paused), setting the start_time
    /// clear the occurred flag
    /// the counter value is not reset
    fn restart(&mut self) {
        self.update();
        self.start_time = Instant::now();
        self.paused = false;
        self.occurred = false;
    }

    /// start the timer (if it was paused), setting the start_time
    /// clear the occurred flag
    /// reset the  counter value to 0
    fn reset(&mut self) {
        self.start_time = Instant::now();
        self.paused = false;
        self.occurred = false;
        self.count = 0;
    }

    fn update(&mut self) {
        if self.paused {
            return;
        }
        let now = Instant::now();
        while now.duration_since(self.start_time) >= self.timeout {
            self.count = (self.count + 1).clamp(0, self.max_count);
            self.start_time += self.timeout;
            self.occurred = true;
        }
    }

    pub fn pause(&mut self) {
        self.update();
        self.paused = true;
    }

    pub fn start(&mut self) {
        self.paused = false;
    }

    pub fn limit_reached(&mut self) -> bool {
        self.update();
        self.count == self.max_count
    }

    pub fn timeout_occurred(&mut self) -> bool {
        self.update();
        self.occurred
    }

    pub fn until_timeout(&self) -> Duration {
        let next_timeout = self.start_time + self.timeout;
        let now = Instant::now();
        if next_timeout > now {
            next_timeout.duration_since(now)
        } else {
            Duration::ZERO
        }
    }

    #[cfg(test)]
    pub fn get_count(&self) -> u32 {
        self.count
    }

    #[cfg(test)]
    pub fn is_ticking(&self) -> bool {
        !self.paused
    }
}

#[derive(Debug)]
pub struct Timer {
    pub inactivity: Counter,
    pub nak: Counter,
    pub eof: Counter,
    pub progress_report: Counter,
}
impl Timer {
    pub fn new(
        inactivity_timeout: i64,
        inactivity_max_count: u32,
        eof_timeout: i64,
        eof_max_count: u32,
        nak_timeout: i64,
        nak_max_count: u32,
        progress_report_interval_secs: i64,
    ) -> Self {
        Self {
            inactivity: Counter::new(
                Duration::from_secs(inactivity_timeout as u64),
                inactivity_max_count,
            ),
            eof: Counter::new(Duration::from_secs(eof_timeout as u64), eof_max_count),
            nak: Counter::new(Duration::from_secs(nak_timeout as u64), nak_max_count),
            progress_report: Counter::new(
                Duration::from_secs(progress_report_interval_secs as u64),
                u32::MAX,
            ),
        }
    }

    pub fn restart_inactivity(&mut self) {
        self.inactivity.restart()
    }

    pub fn reset_inactivity(&mut self) {
        self.inactivity.reset()
    }

    pub fn restart_eof(&mut self) {
        self.eof.restart()
    }

    pub fn reset_eof(&mut self) {
        self.eof.reset()
    }

    pub fn restart_nak(&mut self) {
        self.nak.restart()
    }

    pub fn reset_nak(&mut self) {
        self.nak.reset()
    }

    pub fn reset_progress_report(&mut self) {
        self.progress_report.reset()
    }
    /// returns the duration until one of the timers timeouts
    /// if all timers are paused, returns Duration::MAX
    pub fn until_timeout(&self) -> Duration {
        let mut min = Duration::MAX;

        if !self.eof.paused {
            min = Duration::min(min, self.eof.until_timeout());
        }

        if !self.nak.paused {
            min = Duration::min(min, self.nak.until_timeout());
        }

        if !self.inactivity.paused {
            min = Duration::min(min, self.inactivity.until_timeout());
        }

        if !self.progress_report.paused {
            min = Duration::min(min, self.progress_report.until_timeout());
        }
        min
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{thread, time::Duration};

    #[test]
    fn timeout() {
        let mut timer = Timer::new(1_i64, 5, 1_i64, 5, 1_i64, 5, 1_i64);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(2.5_f32));
        timer.inactivity.pause();
        assert_eq!(timer.inactivity.get_count(), 2);
        assert!(!timer.inactivity.limit_reached());
        assert!(timer.inactivity.timeout_occurred())
    }

    #[test]
    fn no_timeout() {
        let mut timer = Timer::new(3_i64, 5, 1_i64, 5, 1_i64, 5, 1_i64);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.1_f32));
        timer.inactivity.pause();
        assert!(!timer.inactivity.timeout_occurred());
        // sleep again but make sure to cross the threshold from the original time out
        thread::sleep(Duration::from_secs_f32(1.5_f32));
        assert!(!timer.inactivity.timeout_occurred());

        assert_eq!(timer.inactivity.get_count(), 0)
    }

    #[test]
    fn limit() {
        let mut timer = Timer::new(1_i64, 1, 1_i64, 5, 1_i64, 5, 1_i64);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.limit_reached());
        // make sure the clamping works right
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.limit_reached());
        assert_eq!(timer.inactivity.get_count(), 1);
    }

    #[test]
    fn restart_no_fail() {
        let mut timer = Timer::new(2_i64, 5, 1_i64, 5, 1_i64, 5, 1_64);
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(!timer.inactivity.timeout_occurred());
        timer.restart_inactivity();
        thread::sleep(Duration::from_secs_f32(2.2));
        timer.inactivity.pause();

        assert!(timer.inactivity.timeout_occurred())
    }

    #[test]
    fn timeout_all() {
        let mut timer = Timer::new(1_i64, 5, 1_i64, 5, 1_i64, 5, 1_i64);
        timer.restart_inactivity();
        timer.restart_eof();
        timer.restart_nak();
        timer.reset_progress_report();
        thread::sleep(Duration::from_secs_f32(1.5));
        assert!(timer.inactivity.timeout_occurred());
        assert!(timer.eof.timeout_occurred());
        assert!(timer.nak.timeout_occurred());
        assert!(timer.progress_report.timeout_occurred());
        assert_eq!(timer.inactivity.get_count(), 1);
        assert_eq!(timer.eof.get_count(), 1);
        assert_eq!(timer.nak.get_count(), 1);
        assert_eq!(timer.progress_report.get_count(), 1);
    }
}
