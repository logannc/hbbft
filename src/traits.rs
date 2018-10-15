//! Common supertraits for distributed algorithms.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::once;

use failure::Fail;

use fault_log::{Fault, FaultLog};
use {Target, TargetedMessage};

/// A transaction, user message, etc.
pub trait Contribution: Eq + Debug + Hash + Send + Sync {}
impl<C> Contribution for C where C: Eq + Debug + Hash + Send + Sync {}

/// A peer node's unique identifier.
pub trait NodeIdT: Eq + Ord + Clone + Debug + Hash + Send + Sync {}
impl<N> NodeIdT for N where N: Eq + Ord + Clone + Debug + Hash + Send + Sync {}

/// Messages.
pub trait Message: Debug + Send + Sync {}
impl<M> Message for M where M: Debug + Send + Sync {}

/// Result of one step of the local state machine of a distributed algorithm. Such a result should
/// be used and never discarded by the client of the algorithm.
#[must_use = "The algorithm step result must be used."]
#[derive(Debug)]
pub struct Step<D>
where
    D: DistAlgorithm,
    <D as DistAlgorithm>::NodeId: NodeIdT,
{
    pub output: VecDeque<D::Output>,
    pub fault_log: FaultLog<D::NodeId>,
    pub messages: VecDeque<TargetedMessage<D::Message, D::NodeId>>,
}

impl<D> Default for Step<D>
where
    D: DistAlgorithm,
    <D as DistAlgorithm>::NodeId: NodeIdT,
{
    fn default() -> Step<D> {
        Step {
            output: VecDeque::default(),
            fault_log: FaultLog::default(),
            messages: VecDeque::default(),
        }
    }
}

impl<D: DistAlgorithm> Step<D>
where
    <D as DistAlgorithm>::NodeId: NodeIdT,
{
    /// Creates a new `Step` from the given collections.
    pub fn new(
        output: VecDeque<D::Output>,
        fault_log: FaultLog<D::NodeId>,
        messages: VecDeque<TargetedMessage<D::Message, D::NodeId>>,
    ) -> Self {
        Step {
            output,
            fault_log,
            messages,
        }
    }

    /// Returns the same step, with the given additional output.
    pub fn with_output(mut self, output: D::Output) -> Self {
        self.output.push_back(output);
        self
    }

    /// Converts `self` into a step of another type, given conversion methods for output and
    /// messages.
    pub fn map<D2, FO, FM>(self, f_out: FO, f_msg: FM) -> Step<D2>
    where
        D2: DistAlgorithm<NodeId = D::NodeId>,
        FO: Fn(D::Output) -> D2::Output,
        FM: Fn(D::Message) -> D2::Message,
    {
        Step {
            output: self.output.into_iter().map(f_out).collect(),
            fault_log: self.fault_log,
            messages: self.messages.into_iter().map(|tm| tm.map(&f_msg)).collect(),
        }
    }

    /// Extends `self` with `other`s messages and fault logs, and returns `other.output`.
    pub fn extend_with<D2, FM>(&mut self, other: Step<D2>, f_msg: FM) -> VecDeque<D2::Output>
    where
        D2: DistAlgorithm<NodeId = D::NodeId>,
        FM: Fn(D2::Message) -> D::Message,
    {
        self.fault_log.extend(other.fault_log);
        let msgs = other.messages.into_iter().map(|tm| tm.map(&f_msg));
        self.messages.extend(msgs);
        other.output
    }

    /// Adds the outputs, fault logs and messages of `other` to `self`.
    pub fn extend(&mut self, other: Self) {
        self.output.extend(other.output);
        self.fault_log.extend(other.fault_log);
        self.messages.extend(other.messages);
    }

    /// Converts this step into an equivalent step for a different `DistAlgorithm`.
    // This cannot be a `From` impl, because it would conflict with `impl From<T> for T`.
    pub fn convert<D2>(self) -> Step<D2>
    where
        D2: DistAlgorithm<NodeId = D::NodeId, Output = D::Output, Message = D::Message>,
    {
        Step {
            output: self.output,
            fault_log: self.fault_log,
            messages: self.messages,
        }
    }

    /// Returns `true` if there are now messages, faults or outputs.
    pub fn is_empty(&self) -> bool {
        self.output.is_empty() && self.fault_log.is_empty() && self.messages.is_empty()
    }
}

impl<D: DistAlgorithm> From<FaultLog<D::NodeId>> for Step<D> {
    fn from(fault_log: FaultLog<D::NodeId>) -> Self {
        Step {
            fault_log,
            ..Step::default()
        }
    }
}

impl<D: DistAlgorithm> From<Fault<D::NodeId>> for Step<D> {
    fn from(fault: Fault<D::NodeId>) -> Self {
        Step {
            fault_log: fault.into(),
            ..Step::default()
        }
    }
}

impl<D: DistAlgorithm> From<TargetedMessage<D::Message, D::NodeId>> for Step<D> {
    fn from(msg: TargetedMessage<D::Message, D::NodeId>) -> Self {
        Step {
            messages: once(msg).collect(),
            ..Step::default()
        }
    }
}

impl<D, I> From<I> for Step<D>
where
    D: DistAlgorithm,
    I: IntoIterator<Item = TargetedMessage<D::Message, D::NodeId>>,
{
    fn from(msgs: I) -> Self {
        Step {
            messages: msgs.into_iter().collect(),
            ..Step::default()
        }
    }
}

/// An interface to objects with epoch numbers. Different algorithms may have different internal
/// notion of _epoch_. This interface summarizes the properties that are essential for the message
/// sender queue.
pub trait Epoched {
    type Epoch: Clone + Copy + Eq + Ord;

    /// Returns the object's epoch number.
    fn epoch(&self) -> Self::Epoch;
}

impl<M: Epoched, N> Epoched for TargetedMessage<M, N> {
    type Epoch = <M as Epoched>::Epoch;

    fn epoch(&self) -> Self::Epoch {
        self.message.epoch()
    }
}

impl<'i, D> Step<D>
where
    D: DistAlgorithm,
    <D as DistAlgorithm>::NodeId: NodeIdT,
    <D as DistAlgorithm>::Message: 'i + Clone + Epoched,
{
    /// Removes and returns any messages that are not yet accepted by remote nodes according to the
    /// mapping `remote_epochs`. This way the returned messages are postponed until later, and the
    /// remaining messages can be sent to remote nodes without delay.
    pub fn defer_messages<I, F, G, H>(
        &mut self,
        remote_epochs: &'i BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
        remote_ids: I,
        is_accepting_epoch: F,
        is_later_epoch: G,
        is_passed_unchanged: H,
    ) -> impl Iterator<Item = (D::NodeId, D::Message)> + 'i
    where
        I: 'i + Iterator<Item = &'i D::NodeId>,
        <D as DistAlgorithm>::NodeId: 'i,
        F: Fn(&D::Message, <D::Message as Epoched>::Epoch) -> bool,
        G: Fn(&D::Message, <D::Message as Epoched>::Epoch) -> bool,
        H: FnMut(&TargetedMessage<D::Message, D::NodeId>) -> bool,
    {
        let messages = &mut self.messages;
        let (mut passed_msgs, failed_msgs): (Vec<_>, Vec<_>) =
            messages.drain(..).partition(is_passed_unchanged);
        // `Target::All` messages contained in the result of the partitioning are analyzed further
        // and each split into two sets of point messages: those which can be sent without delay and
        // those which should be postponed.
        let remote_nodes: BTreeSet<&D::NodeId> = remote_ids.collect();
        let mut deferred_msgs: Vec<(D::NodeId, D::Message)> = Vec::new();
        for msg in failed_msgs {
            let message = msg.message;
            match msg.target {
                Target::Node(id) => {
                    let mut defer = false;
                    {
                        let lagging = |&them| {
                            !(is_accepting_epoch(&message, them) || is_later_epoch(&message, them))
                        };
                        if remote_epochs.get(&id).map_or(true, lagging) {
                            defer = true;
                        }
                    }
                    if defer {
                        deferred_msgs.push((id, message));
                    }
                }
                Target::All => {
                    let isnt_earlier_epoch = |&them| {
                        is_accepting_epoch(&message, them) || is_later_epoch(&message, them)
                    };
                    let lagging = |them| !isnt_earlier_epoch(them);
                    let accepts = |&them| is_accepting_epoch(&message, them);
                    let accepting_nodes: BTreeSet<&D::NodeId> = remote_epochs
                        .iter()
                        .filter(|(_, them)| accepts(them))
                        .map(|(id, _)| id)
                        .collect();
                    let non_lagging_nodes: BTreeSet<&D::NodeId> = remote_epochs
                        .iter()
                        .filter(|(_, them)| isnt_earlier_epoch(them))
                        .map(|(id, _)| id)
                        .collect();
                    for &id in &accepting_nodes {
                        passed_msgs.push(Target::Node(id.clone()).message(message.clone()));
                    }
                    let lagging_nodes: BTreeSet<_> =
                        remote_nodes.difference(&non_lagging_nodes).collect();
                    for &id in lagging_nodes {
                        if remote_epochs.get(&id).map_or(true, lagging) {
                            deferred_msgs.push((id.clone(), message.clone()));
                        }
                    }
                }
            }
        }
        messages.extend(passed_msgs);
        deferred_msgs.into_iter()
    }
}

/// A distributed algorithm that defines a message flow.
pub trait DistAlgorithm: Send + Sync {
    /// Unique node identifier.
    type NodeId: NodeIdT;
    /// The input provided by the user.
    type Input;
    /// The output type. Some algorithms return an output exactly once, others return multiple
    /// times.
    type Output;
    /// The messages that need to be exchanged between the instances in the participating nodes.
    type Message: Message;
    /// The errors that can occur during execution.
    type Error: Fail;

    /// Handles an input provided by the user, and returns
    fn handle_input(&mut self, input: Self::Input) -> Result<Step<Self>, Self::Error>
    where
        Self: Sized;

    /// Handles a message received from node `sender_id`.
    fn handle_message(
        &mut self,
        sender_id: &Self::NodeId,
        message: Self::Message,
    ) -> Result<Step<Self>, Self::Error>
    where
        Self: Sized;

    /// Returns `true` if execution has completed and this instance can be dropped.
    fn terminated(&self) -> bool;

    /// Returns this node's own ID.
    fn our_id(&self) -> &Self::NodeId;
}
