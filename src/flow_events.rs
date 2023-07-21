use enum_to_num::EnumToNum;

/// Events for notifying that a specific flow in the code has reached.
/// Useful for integration test to never sleep.
#[derive(EnumToNum)]
pub enum FlowEvent {
    StartTasks,
    DeadNodeRemoved,
    AliveNodeGossip,
}
