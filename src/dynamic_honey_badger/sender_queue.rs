use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use rand::Rand;
use serde::{Deserialize, Serialize};

use super::{
    Batch, Change, ChangeState, DynamicHoneyBadger, Epoch, Error, Input, Message as AlgoMessage,
    Result, Step as AlgoStep,
};
use {Contribution, DistAlgorithm, Epoched, NodeIdT, Target, TargetedMessage};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageContent<N: Rand> {
    EpochStarted,
    Algo(AlgoMessage<N>),
}

impl<N: Rand> MessageContent<N> {
    pub fn with_epoch(self, epoch: Epoch) -> Message<N> {
        Message {
            epoch,
            content: self,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message<N: Rand> {
    pub(super) epoch: Epoch,
    pub(super) content: MessageContent<N>,
}

impl<N: Rand> From<AlgoMessage<N>> for Message<N> {
    fn from(message: AlgoMessage<N>) -> Self {
        Message {
            epoch: message.epoch(),
            content: MessageContent::Algo(message),
        }
    }
}

/// An instance of `DynamicHoneyBadger` wrapped with a queue of outgoing messages (a.k.a. sender
/// queue). This wrapping ensures that the messages that are sent to remote instances lead to
/// progress of the entire consensus network. In particular, messages to lagging remote nodes are
/// queued and sent only when those nodes' epochs match the queued messages' epochs.
#[derive(Debug)]
pub struct SenderQueue<C, N: Rand> {
    /// The managed Dynamic Honey Badger instance.
    algo: DynamicHoneyBadger<C, N>,
    /// Current Honey Badger epoch.
    epoch: Epoch,
    /// Messages that couldn't be handled yet by remote nodes.
    outgoing_queue: BTreeMap<(N, Epoch), Vec<AlgoMessage<N>>>,
    /// Known current epochs of remote nodes.
    remote_epochs: BTreeMap<N, Epoch>,
    /// If used as part of `DynamicHoneyBadger`, this is the node which is being added using a
    /// `Change::Add` command. The command should be ongoing. The node receives any broadcast
    /// message but is not a validator.
    node_being_added: Option<N>,
    /// Observer nodes.
    observers: BTreeSet<N>,
}

pub type Step<C, N> = ::Step<SenderQueue<C, N>>;

impl<C, N> DistAlgorithm for SenderQueue<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Serialize + for<'r> Deserialize<'r> + Rand,
{
    type NodeId = N;
    type Input = Input<C, N>;
    type Output = Batch<C, N>;
    type Message = Message<N>;
    type Error = Error;

    fn handle_input(&mut self, input: Self::Input) -> Result<Step<C, N>> {
        let mut step = self.algo.handle_input(input)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    fn handle_message(&mut self, sender_id: &N, message: Self::Message) -> Result<Step<C, N>> {
        match message.content {
            MessageContent::EpochStarted => Ok(self.handle_epoch_started(sender_id, message.epoch)),
            MessageContent::Algo(msg) => self.handle_message_content(sender_id, msg),
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
    N: NodeIdT + Serialize + for<'r> Deserialize<'r> + Rand,
{
    /// Returns a new `SenderQueueBuilder` configured to manage a given `DynamicHoneyBadger` instance.
    pub fn builder(algo: DynamicHoneyBadger<C, N>) -> SenderQueueBuilder<C, N> {
        SenderQueueBuilder::new(algo)
    }

    /// Handles an epoch start announcement.
    fn handle_epoch_started(&mut self, sender_id: &N, epoch: Epoch) -> Step<C, N> {
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
    fn remove_earlier_messages(&mut self, sender_id: &N, Epoch((era, hb_epoch)): Epoch) {
        let earlier_keys: Vec<_> = self
            .outgoing_queue
            .keys()
            .cloned()
            .filter(|(id, Epoch((this_era, this_hb_epoch)))| {
                id == sender_id
                    && (*this_era < era
                        || (*this_era == era
                            && !this_hb_epoch.is_none()
                            && *this_hb_epoch < hb_epoch))
            }).collect();
        for key in earlier_keys {
            debug!("Removing messages for {:?}", key);
            self.outgoing_queue.remove(&key);
        }
    }

    /// Processes an announcement of a new epoch update received from a remote node.
    fn process_new_epoch(&mut self, sender_id: &N, Epoch((era, hb_epoch)): Epoch) -> Step<C, N> {
        // Send any DHB messages for `era`.
        let mut ready_messages = self
            .outgoing_queue
            .remove(&(sender_id.clone(), Epoch((era, None))))
            .unwrap_or_else(|| vec![]);
        if hb_epoch.is_some() {
            // Send any HB messages for `hb_epoch`.
            ready_messages.extend(
                self.outgoing_queue
                    .remove(&(sender_id.clone(), Epoch((era, hb_epoch))))
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
        sender_id: &N,
        content: AlgoMessage<N>,
    ) -> Result<Step<C, N>> {
        let mut step = self.algo.handle_message(sender_id, content)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    /// Updates the current Honey Badger epoch.
    fn update_epoch(&mut self, step: &AlgoStep<C, N>) -> Step<C, N> {
        let mut updated = false;
        // Look up `DynamicHoneyBadger` epoch updates.
        self.epoch = step
            .output
            .iter()
            .fold(self.epoch, |epoch, batch: &Batch<C, N>| {
                if batch.next_epoch > epoch {
                    debug!("batch.next_epoch = {:?}", batch.next_epoch);
                    match batch.change {
                        ChangeState::Complete(Change::Remove(ref id)) => {
                            self.observers.insert(id.clone());
                        }
                        ChangeState::InProgress(Change::Add(ref id, ..)) => {
                            self.observers.remove(id);
                            self.node_being_added = Some(id.clone());
                        }
                        ChangeState::Complete(Change::Add(..)) => {
                            self.node_being_added = None;
                        }
                        _ => {}
                    }
                    updated = true;
                    batch.next_epoch
                } else {
                    epoch
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
        let is_accepting_epoch = |us: &AlgoMessage<N>, Epoch((them_era, them_hb_epoch)): Epoch| {
            let Epoch((era, hb_epoch)) = us.epoch();
            era == them_era
                && (them_hb_epoch <= hb_epoch
                    && hb_epoch <= them_hb_epoch.map(|e| e + max_future_epochs as u64))
        };
        let is_later_epoch = |us: &AlgoMessage<N>, Epoch((them_era, them_hb_epoch)): Epoch| {
            let Epoch((era, hb_epoch)) = us.epoch();
            era < them_era || (era == them_era && hb_epoch.is_some() && hb_epoch < them_hb_epoch)
        };
        let remote_epochs = &self.remote_epochs;
        let is_passed_unchanged = |msg: &TargetedMessage<_, _>| {
            let pass = |&them: &Epoch| is_accepting_epoch(&msg.message, them);
            match &msg.target {
                Target::All => remote_epochs.values().all(pass),
                Target::Node(id) => remote_epochs.get(&id).map_or(false, pass),
            }
        };
        let our_id = self.algo.netinfo.our_id();
        let all_remote_nodes = self
            .algo
            .netinfo
            .all_ids()
            .chain(self.node_being_added.iter())
            .chain(self.observers.iter())
            .filter(|&id| id != our_id)
            .into_iter();
        let deferred_msgs = step.defer_messages(
            &self.remote_epochs,
            all_remote_nodes,
            is_accepting_epoch,
            is_later_epoch,
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
    pub fn algo(&self) -> &DynamicHoneyBadger<C, N> {
        &self.algo
    }
}

/// A builder of a Honey Badger with a sender queue. It configures the parameters and creates new
/// instances of `SenderQueue`.
pub struct SenderQueueBuilder<C, N: Rand> {
    algo: DynamicHoneyBadger<C, N>,
    observers: BTreeSet<N>,
    _phantom: PhantomData<C>,
}

impl<C, N> SenderQueueBuilder<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Serialize + for<'r> Deserialize<'r> + Rand,
{
    pub fn new(algo: DynamicHoneyBadger<C, N>) -> Self {
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
            epoch: Epoch((0, Some(0))),
            outgoing_queue: BTreeMap::new(),
            remote_epochs: BTreeMap::new(),
            node_being_added: None,
            observers: self.observers,
        };
        let step = Target::All
            .message(MessageContent::EpochStarted.with_epoch(Epoch((0, Some(0)))))
            .into();
        (sq, step)
    }
}
