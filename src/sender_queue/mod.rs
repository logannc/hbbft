mod message;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use {DistAlgorithm, Epoched, NetworkInfo, Target, TargetedMessage};

pub use self::message::{Message, MessageContent};

/// Generic sender queue functionality.
pub trait SenderQueueFunc<D>
where
    D: DistAlgorithm,
    <D as DistAlgorithm>::Message: Epoched + Serialize + for<'r> Deserialize<'r>,
{
    type AlgoStep;

    /// Converts a message of the managed algorithm into a sender queue message.
    fn convert_message(message: D::Message) -> Message<D>;

    /// Returns the maximum epoch out of a given epoch and the epoch of a given batch.
    fn max_epoch_with_batch(
        epoch: <D::Message as Epoched>::Epoch,
        batch: &D::Output,
    ) -> <D::Message as Epoched>::Epoch;

    /// Whether the epoch `them` accepts the message `us`.
    fn is_accepting_epoch(us: &D::Message, them: <D::Message as Epoched>::Epoch) -> bool;

    /// Whether the epoch `them` is ahead of the epoch of the message `us`.
    fn is_later_epoch(us: &D::Message, them: <D::Message as Epoched>::Epoch) -> bool;

    /// Whether the message should be sent immediately rather than postponed.
    fn is_passed_unchanged(
        msg: &TargetedMessage<D::Message, D::NodeId>,
        remote_epochs: &BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
    ) -> bool;

    /// A _spanning epoch_ of an epoch `e` is a unique epoch `e0` such that
    ///
    /// - `e` and `e0` are incomparable and
    ///
    /// - the duration of `e0` is at least that of `e`.
    ///
    /// If there exists a spanning epoch `e0` then the result of the function application is
    /// `Some(e0)`. Otherwise the result is `None`. In particular, any `DynamicHoneyBadger` epoch
    /// `Epoch((x, Some(y)))` has a spanning epoch `Epoch((x, None))`. In turn, no epoch `Epoch((x,
    /// None))` has a spanning epoch.
    fn spanning_epoch(
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Option<<D::Message as Epoched>::Epoch>;
}

pub type OutgoingQueue<D> = BTreeMap<
    (
        <D as DistAlgorithm>::NodeId,
        <<D as DistAlgorithm>::Message as Epoched>::Epoch,
    ),
    Vec<<D as DistAlgorithm>::Message>,
>;

/// An instance of `DistAlgorithm` wrapped with a queue of outgoing messages, that is, a sender
/// queue. This wrapping ensures that the messages sent to remote instances lead to progress of the
/// entire consensus network. In particular, messages to lagging remote nodes are queued and sent
/// only when those nodes' epochs match the queued messages' epochs. Thus all nodes can handle
/// incoming messages without queueing them and can ignore messages whose epochs are not currently
/// acccepted.
#[derive(Debug)]
pub struct SenderQueue<D>
where
    D: DistAlgorithm + SenderQueueFunc<D>,
    D::Message: Epoched + Serialize + for<'r> Deserialize<'r>,
{
    /// The managed `DistAlgorithm` instance.
    algo: D,
    /// `NetworkInfo` of the managed algorithm.
    netinfo: Arc<NetworkInfo<D::NodeId>>,
    /// Current epoch.
    epoch: <D::Message as Epoched>::Epoch,
    /// Messages that couldn't be handled yet by remote nodes.
    outgoing_queue: OutgoingQueue<D>,
    /// Known current epochs of remote nodes.
    remote_epochs: BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
    /// If used as part of `DynamicHoneyBadger`, this is the node which is being added using a
    /// `Change::Add` command. The command should be ongoing. The node receives any broadcast
    /// message but is not a validator.
    node_being_added: Option<D::NodeId>,
    /// Observer nodes.
    observers: BTreeSet<D::NodeId>,
}

pub type Step<D> = ::Step<SenderQueue<D>>;

pub type Result<T, D> = ::std::result::Result<T, <D as DistAlgorithm>::Error>;

impl<D> DistAlgorithm for SenderQueue<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
{
    type NodeId = D::NodeId;
    type Input = D::Input;
    type Output = D::Output;
    type Message = Message<D>;
    type Error = D::Error;

    fn handle_input(&mut self, input: Self::Input) -> Result<Step<D>, D> {
        let mut step = self.algo.handle_input(input)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step
            .extend(step.map(|output| output, <D as SenderQueueFunc<D>>::convert_message));
        Ok(sender_queue_step)
    }

    fn handle_message(
        &mut self,
        sender_id: &D::NodeId,
        message: Self::Message,
    ) -> Result<Step<D>, D> {
        match message.content {
            MessageContent::EpochStarted => Ok(self.handle_epoch_started(sender_id, message.epoch)),
            MessageContent::Algo(msg) => self.handle_message_content(sender_id, msg),
        }
    }

    fn terminated(&self) -> bool {
        false
    }

    fn our_id(&self) -> &D::NodeId {
        self.netinfo.our_id()
    }
}

impl<D> SenderQueue<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
{
    /// Returns a new `SenderQueueBuilder` configured to manage a given `DynamicHoneyBadger` instance.
    pub fn builder(algo: D) -> SenderQueueBuilder<D> {
        SenderQueueBuilder::new(algo)
    }

    /// Handles an epoch start announcement.
    fn handle_epoch_started(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Step<D> {
        self.remote_epochs
            .entry(sender_id.clone())
            .and_modify(|e| {
                if *e < epoch {
                    *e = epoch;
                }
            }).or_insert(epoch);
        self.remove_earlier_messages(sender_id, epoch);
        self.process_new_epoch(sender_id, epoch)
    }

    /// Removes all messages queued for the remote node from epochs upto `epoch`.
    fn remove_earlier_messages(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) {
        let earlier_keys: Vec<_> = self
            .outgoing_queue
            .keys()
            .cloned()
            .filter(|(id, this_epoch)| {
                id == sender_id
                    && PartialOrd::partial_cmp(this_epoch, &epoch) == Some(Ordering::Less)
            }).collect();
        for key in earlier_keys {
            self.outgoing_queue.remove(&key);
        }
    }

    /// Processes an announcement of a new epoch update received from a remote node.
    fn process_new_epoch(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Step<D> {
        // Send any HB messages for the HB epoch.
        let mut ready_messages = self
            .outgoing_queue
            .remove(&(sender_id.clone(), epoch))
            .unwrap_or_else(|| vec![]);
        if let Some(u) = <D as SenderQueueFunc<D>>::spanning_epoch(epoch) {
            // Send any DHB messages for the DHB era.
            ready_messages.extend(
                self.outgoing_queue
                    .remove(&(sender_id.clone(), u))
                    .unwrap_or_else(|| vec![]),
            );
        }
        Step::from(ready_messages.into_iter().map(|msg| {
            Target::Node(sender_id.clone()).message(Message {
                epoch: msg.epoch(),
                content: MessageContent::Algo(msg),
            })
        }))
    }

    /// Handles a Honey Badger algorithm message in a given epoch.
    fn handle_message_content(
        &mut self,
        sender_id: &D::NodeId,
        content: D::Message,
    ) -> Result<Step<D>, D> {
        let mut step = self.algo.handle_message(sender_id, content)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step
            .extend(step.map(|output| output, <D as SenderQueueFunc<D>>::convert_message));
        Ok(sender_queue_step)
    }

    /// Updates the current Honey Badger epoch.
    fn update_epoch(&mut self, step: &::Step<D>) -> Step<D> {
        let mut updated = false;
        // Look up `DynamicHoneyBadger` epoch updates.
        self.epoch = step.output.iter().fold(self.epoch, |epoch, batch| {
            let max_epoch = <D as SenderQueueFunc<D>>::max_epoch_with_batch(epoch, batch);
            if max_epoch != epoch {
                updated = true;
            }
            max_epoch
        });
        if updated {
            // Announce the new epoch.
            Target::All
                .message(MessageContent::EpochStarted.with_epoch(self.epoch))
                .into()
        } else {
            Step::default()
        }
    }

    /// Removes any messages to nodes at earlier epochs from the given `Step`. This may involve
    /// decomposing a `Target::All` message into `Target::Node` messages and sending some of the
    /// resulting messages while placing onto the queue those remaining messages whose recipient is
    /// currently at an earlier epoch.
    fn defer_messages(&mut self, step: &mut ::Step<D>) {
        let remote_epochs = &self.remote_epochs;
        let is_passed_unchanged = |msg: &TargetedMessage<_, _>| {
            <D as SenderQueueFunc<D>>::is_passed_unchanged(msg, remote_epochs)
        };
        let our_id = self.netinfo.our_id();
        let all_remote_nodes = self
            .netinfo
            .all_ids()
            .chain(self.node_being_added.iter())
            .chain(self.observers.iter())
            .filter(|&id| id != our_id)
            .into_iter();
        let deferred_msgs = step.defer_messages(
            &self.remote_epochs,
            all_remote_nodes,
            <D as SenderQueueFunc<D>>::is_accepting_epoch,
            <D as SenderQueueFunc<D>>::is_later_epoch,
            is_passed_unchanged,
        );
        // Append the deferred messages onto the queues.
        for (id, message) in deferred_msgs {
            let epoch = message.epoch();
            self.outgoing_queue
                .entry((id, epoch))
                .and_modify(|e| e.push(message.clone()))
                .or_insert_with(|| vec![message.clone()]);
        }
    }

    /// Returns a reference to the managed algorithm.
    pub fn algo(&self) -> &D {
        &self.algo
    }
}

/// A builder of a Honey Badger with a sender queue. It configures the parameters and creates new
/// instances of `SenderQueue`.
pub struct SenderQueueBuilder<D>
where
    D: DistAlgorithm,
{
    algo: D,
    observers: BTreeSet<D::NodeId>,
}

impl<D> SenderQueueBuilder<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
{
    pub fn new(algo: D) -> Self {
        SenderQueueBuilder {
            algo,
            observers: BTreeSet::new(),
        }
    }

    pub fn observers<I>(mut self, observers: I) -> Self
    where
        I: Iterator<Item = D::NodeId>,
    {
        self.observers = observers.collect();
        self
    }

    pub fn build(self, netinfo: Arc<NetworkInfo<D::NodeId>>) -> (SenderQueue<D>, Step<D>) {
        let epoch = <D::Message as Epoched>::Epoch::default();
        let sq = SenderQueue {
            algo: self.algo,
            netinfo,
            epoch,
            outgoing_queue: BTreeMap::new(),
            remote_epochs: BTreeMap::new(),
            node_being_added: None,
            observers: self.observers,
        };
        let step = Target::All
            .message(MessageContent::EpochStarted.with_epoch(epoch))
            .into();
        (sq, step)
    }
}
