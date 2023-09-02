use kinded::Kinded;

/// Events for notifying that a specific flow in the code has reached.
/// Useful for integration test to never sleep.
#[derive(Kinded)]
#[kinded(derive(Hash))]
pub enum FlowEvent {
    StartTasks,
    DeadNodeRemoved,
    AliveNodeGossip,
    CollectionCreated,
    DoneMigration,
    ItemSetFromShardMessage,
}
