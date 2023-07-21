use enum_to_num::ToU8;

/// Events for notifying that a specific flow in the code has reached.
/// Useful for integration test to never sleep.
#[derive(ToU8)]
pub enum FlowEvent {
    StartTasks = 0,
    DeadNodeRemoved = 1,
    AliveNodeGossip = 2,
}
