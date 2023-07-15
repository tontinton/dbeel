/// Events that hold no data, they are only notifying that a specific flow in
/// the code has reached.
/// Useful for integration test to never sleep.
pub enum FlowEvent {
    StartTasks = 0,
    DeadNodeRemoved = 1,
}

impl From<FlowEvent> for u8 {
    fn from(value: FlowEvent) -> Self {
        value as u8
    }
}
