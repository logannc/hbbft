use serde::{Deserialize, Serialize};

use {DistAlgorithm, Epoched};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageContent<D>
where
    D: DistAlgorithm,
{
    EpochStarted,
    Algo(D::Message),
}

impl<D> MessageContent<D>
where
    D: DistAlgorithm,
    D::Message: Epoched + Serialize + for<'r> Deserialize<'r>,
{
    pub fn with_epoch(self, epoch: <D::Message as Epoched>::Epoch) -> Message<D> {
        Message {
            epoch,
            content: self,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message<D>
where
    D: DistAlgorithm,
    D::Message: Epoched + Serialize + for<'r> Deserialize<'r>,
{
    pub(super) epoch: <D::Message as Epoched>::Epoch,
    pub(super) content: MessageContent<D>,
}
