use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use rand::Rand;
use serde::{Deserialize, Serialize};

use super::{
    Batch, Error, HoneyBadger, Message as AlgoMessage, MessageContent as AlgoMessageContent,
    Result, Step as AlgoStep,
};
use {Contribution, DistAlgorithm, Epoched, NodeIdT, Target, TargetedMessage};

#[derive(Clone, Debug, Deserialize, Rand, Serialize)]
pub enum MessageContent<N: Rand> {
    EpochStarted,
    Algo(AlgoMessageContent<N>),
}

impl<N: Rand> MessageContent<N> {
    pub fn with_epoch(self, epoch: u64) -> Message<N> {
        Message {
            epoch,
            content: self,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Rand, Serialize)]
pub struct Message<N: Rand> {
    pub(super) epoch: u64,
    pub(super) content: MessageContent<N>,
}

impl<N: Rand> Epoched for Message<N> {
    type Epoch = u64;

    fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl<N: Rand> From<AlgoMessage<N>> for Message<N> {
    fn from(message: AlgoMessage<N>) -> Self {
        Message {
            epoch: message.epoch,
            content: MessageContent::Algo(message.content),
        }
    }
}

/// An instance of `HoneyBadger` wrapped with a queue of outgoing messages (a.k.a. sender
/// queue). This wrapping ensures that the messages that are sent to remote instances lead to
/// progress of the entire consensus network. In particular, messages to lagging remote nodes are
/// queued and sent only when those nodes' epochs match the queued messages' epochs.
#[derive(Debug)]
pub struct SenderQueue<C, N: Rand> {
    /// The managed Honey Badger instance.
    algo: HoneyBadger<C, N>,
    /// Current Honey Badger epoch.
    epoch: u64,
    /// Messages that couldn't be handled yet by remote nodes.
    outgoing_queue: BTreeMap<(N, u64), Vec<AlgoMessage<N>>>,
    /// Known current epochs of remote nodes.
    remote_epochs: BTreeMap<N, u64>,
    /// Observer nodes.
    observers: BTreeSet<N>,
}

pub type Step<C, N> = ::Step<SenderQueue<C, N>>;

impl<C, N> DistAlgorithm for SenderQueue<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Rand,
{
    type NodeId = N;
    type Input = C;
    type Output = Batch<C, N>;
    type Message = Message<N>;
    type Error = Error;

    fn handle_input(&mut self, input: Self::Input) -> Result<Step<C, N>> {
        let mut step = self.algo.propose(&input)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    fn handle_message(&mut self, sender_id: &N, message: Self::Message) -> Result<Step<C, N>> {
        let epoch = message.epoch;
        match message.content {
            MessageContent::EpochStarted => Ok(self.handle_epoch_started(sender_id, epoch)),
            MessageContent::Algo(content) => self.handle_message_content(sender_id, epoch, content),
        }
    }

    fn terminated(&self) -> bool {
        false
    }

    fn our_id(&self) -> &N {
        self.algo.netinfo.our_id()
    }
}

impl<C, N> SenderQueue<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Rand,
{
    /// Returns a new `SenderQueueBuilder` configured to manage a given `HoneyBadger` instance.
    pub fn builder(algo: HoneyBadger<C, N>) -> SenderQueueBuilder<C, N> {
        SenderQueueBuilder::new(algo)
    }

    /// Handles an epoch start announcement.
    fn handle_epoch_started(&mut self, sender_id: &N, epoch: u64) -> Step<C, N> {
        self.remote_epochs
            .entry(sender_id.clone())
            .and_modify(|e| {
                if *e < epoch {
                    *e = epoch;
                }
            }).or_insert(epoch);
        // Remove all messages queued for the remote node from earlier epochs.
        let earlier_keys: Vec<_> = self
            .outgoing_queue
            .keys()
            .cloned()
            .filter(|(id, e)| id == sender_id && *e < epoch)
            .collect();
        for key in earlier_keys {
            self.outgoing_queue.remove(&key);
        }
        // If there are any messages to `sender_id` for `epoch`, send them now.
        if let Some(messages) = self.outgoing_queue.remove(&(sender_id.clone(), epoch)) {
            Step::from(messages.into_iter().map(|msg| {
                Target::Node(sender_id.clone()).message(Message {
                    epoch,
                    content: MessageContent::Algo(msg.content),
                })
            }))
        } else {
            Step::default()
        }
    }

    /// Handles a Honey Badger algorithm message in a given epoch.
    fn handle_message_content(
        &mut self,
        sender_id: &N,
        epoch: u64,
        content: AlgoMessageContent<N>,
    ) -> Result<Step<C, N>> {
        let mut step = self
            .algo
            .handle_message(sender_id, AlgoMessage { epoch, content })?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    /// Updates the current Honey Badger epoch.
    fn update_epoch(&mut self, step: &AlgoStep<C, N>) -> Step<C, N> {
        let mut updated = false;
        self.epoch = step
            .output
            .iter()
            .fold(self.epoch, |prev, batch: &Batch<C, N>| {
                if batch.epoch >= prev {
                    updated = true;
                    batch.epoch + 1
                } else {
                    prev
                }
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
    fn defer_messages(&mut self, step: &mut AlgoStep<C, N>) {
        let max_future_epochs = self.algo.max_future_epochs;
        let is_accepting_epoch = |us: &AlgoMessage<N>, them: u64| {
            let our_epoch = us.epoch();
            them <= our_epoch && our_epoch <= them + max_future_epochs
        };
        let is_later_epoch = |us: &AlgoMessage<N>, them: u64| us.epoch() < them;
        let remote_epochs = &self.remote_epochs;
        let is_passed_unchanged = |msg: &TargetedMessage<_, _>| {
            let pass = |&them| is_accepting_epoch(&msg.message, them);
            match &msg.target {
                Target::All => remote_epochs.values().all(pass),
                Target::Node(id) => remote_epochs.get(&id).map_or(false, pass),
            }
        };
        let our_id = self.algo.netinfo.our_id();
        let deferred_msgs = step.defer_messages(
            &self.remote_epochs,
            self.algo
                .netinfo
                .all_ids()
                .chain(self.observers.iter())
                .filter(|&id| id != our_id),
            is_accepting_epoch,
            is_later_epoch,
            is_passed_unchanged,
        );
        // Append the deferred messages onto the queue.
        for (id, message) in deferred_msgs {
            let epoch = message.epoch();
            self.outgoing_queue
                .entry((id, epoch))
                .and_modify(|e| e.push(message.clone()))
                .or_insert_with(|| vec![message.clone()]);
        }
    }

    /// Returns a reference to the managed algorithm.
    pub fn algo(&self) -> &HoneyBadger<C, N> {
        &self.algo
    }
}

/// A builder of a Honey Badger with a sender queue. It configures the parameters and creates new
/// instances of `SenderQueue`.
pub struct SenderQueueBuilder<C, N: Rand> {
    algo: HoneyBadger<C, N>,
    observers: BTreeSet<N>,
    _phantom: PhantomData<C>,
}

impl<C, N> SenderQueueBuilder<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Rand,
{
    pub fn new(algo: HoneyBadger<C, N>) -> Self {
        SenderQueueBuilder {
            algo,
            observers: BTreeSet::new(),
            _phantom: PhantomData,
        }
    }

    pub fn observers<I>(mut self, observers: I) -> Self
    where
        I: Iterator<Item = N>,
    {
        self.observers = observers.collect();
        self
    }

    pub fn build(self) -> (SenderQueue<C, N>, Step<C, N>) {
        let sq = SenderQueue {
            algo: self.algo,
            epoch: 0,
            outgoing_queue: BTreeMap::new(),
            remote_epochs: BTreeMap::new(),
            observers: self.observers,
        };
        let step = Target::All
            .message(MessageContent::EpochStarted.with_epoch(0))
            .into();
        (sq, step)
    }
}
