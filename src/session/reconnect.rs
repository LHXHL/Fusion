use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::app::config::RetryPolicy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectState {
    pub attempt: u32,
    pub next_delay: Duration,
    pub exhausted: bool,
}

impl ReconnectState {
    pub fn new(policy: &RetryPolicy) -> Self {
        Self {
            attempt: 0,
            next_delay: Duration::from_secs(policy.interval_secs),
            exhausted: false,
        }
    }

    pub fn fail_and_schedule(&mut self, policy: &RetryPolicy) {
        self.attempt += 1;
        if let Some(max_retries) = policy.max_retries {
            if self.attempt >= max_retries {
                self.exhausted = true;
            }
        }

        let doubled = self.next_delay.as_secs().saturating_mul(2);
        self.next_delay = Duration::from_secs(doubled.min(policy.max_interval_secs));
    }

    pub fn reset(&mut self, policy: &RetryPolicy) {
        self.attempt = 0;
        self.next_delay = Duration::from_secs(policy.interval_secs);
        self.exhausted = false;
    }
}

#[cfg(test)]
mod tests {
    use crate::{app::config::RetryPolicy, session::reconnect::ReconnectState};

    #[test]
    fn reconnect_state_grows_and_caps() {
        let policy = RetryPolicy {
            max_retries: Some(3),
            interval_secs: 2,
            max_interval_secs: 5,
        };
        let mut state = ReconnectState::new(&policy);
        assert_eq!(state.next_delay.as_secs(), 2);
        state.fail_and_schedule(&policy);
        assert_eq!(state.next_delay.as_secs(), 4);
        state.fail_and_schedule(&policy);
        assert_eq!(state.next_delay.as_secs(), 5);
        state.fail_and_schedule(&policy);
        assert!(state.exhausted);
    }
}
