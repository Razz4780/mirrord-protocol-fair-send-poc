#![deny(unused_crate_dependencies)]

use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    error::Error,
    io,
    marker::PhantomData,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use bincode::{Decode, Encode, error::DecodeError};
use bytes::{Buf, BytesMut};
use futures::{FutureExt, StreamExt, future::Shared, stream::BoxStream};
use rand::distr::{Distribution, Uniform};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Notify, futures::OwnedNotified, oneshot},
};
use tokio_util::codec::{Decoder, FramedRead};

pub trait MessageOut: Encode {}

impl<T: Encode> MessageOut for T {}

pub trait MessageIn: 'static + Decode<()> {}

impl<T: 'static + Decode<()>> MessageIn for T {}

/// Splits the given IO stream into independent read and write handles.
///
/// # Read
///
/// The IO stream is transformed into a [`Stream`](futures::Stream) of decoded messages.
///
/// # Write
///
/// Sending the messages is handled in a dedicated [`tokio::task`].
/// The returned [`SendQueue`] can be used to send messages from multiple [`tokio::task`]s,
/// preserving fairness and limiting the amount of memory consumed by each task's messages.
///
/// # Params
///
/// * `send_queue_capacity` - soft limit for the size of the returned [`SendQueue`],
///   counted in bytes of encoded messages
pub fn wrap_connection<IO, MIn, MOut>(
    io: IO,
    send_queue_capacity: NonZeroUsize,
) -> (BoxStream<'static, io::Result<MIn>>, SendQueue<MOut>)
where
    IO: 'static + AsyncRead + AsyncWrite + Send,
    MIn: MessageIn,
    MOut: MessageOut,
{
    let mut state = SendQueuesState::default();
    let queue_id = state.add_queue(send_queue_capacity);
    let state = Arc::new(Mutex::new(state));
    let shared_state = state.clone();
    let (error_tx, error_rx) = oneshot::channel();
    // This is suboptimal for some streams.
    // E.g. for `TcpStream`, we should rather use `TcpStream::into_split`.
    let (read, mut write) = tokio::io::split(io);

    tokio::spawn(async move {
        loop {
            let result = state
                .lock()
                .expect("send queues operations should not panic")
                .next_message();

            match result {
                SendQueuesPoll::Message(message) => {
                    if let Err(error) = write.write_all(&message).await {
                        let _ = error_tx.send(Arc::new(error));
                        break;
                    }
                    if let Err(error) = write.flush().await {
                        let _ = error_tx.send(Arc::new(error));
                        break;
                    }
                }
                SendQueuesPoll::NotReady(notified) => notified.await,
                SendQueuesPoll::Terminated => break,
            }
        }
    });

    (
        FramedRead::new(read, BincodeDecoder::<MIn>(PhantomData)).boxed(),
        SendQueue {
            queue_id,
            shared_state,
            error_rx: error_rx.shared(),
            _phantom: PhantomData,
        },
    )
}

/// Queue for sending messages.
///
/// Has a soft size limit for buffered messages, counted in bytes of encoded representation.
///
/// All [`SendQueue`]s for the same connection are handled fairly,
/// meaning that the next message to be sent is always picked from a random queue.
///
/// # Clone
///
/// Due to a custom [`Drop`] implementation, this struct should not implement [`Clone`].
pub struct SendQueue<M: MessageOut> {
    queue_id: QueueId,
    shared_state: SharedState,
    error_rx: Shared<oneshot::Receiver<Arc<io::Error>>>,
    _phantom: PhantomData<fn() -> M>,
}

impl<M: MessageOut> SendQueue<M> {
    /// Waits for the buffer to have some free capacity,
    /// and puts the encoded message in it.
    ///
    /// Messages put in the buffer will remain there after the queue is dropped.
    ///
    /// Returns an error if the connection has failed.
    pub async fn send(&self, message: M) -> Result<(), Box<dyn Error>> {
        let mut message = bincode::encode_to_vec(message, bincode::config::standard())?;

        loop {
            let result = self
                .shared_state
                .lock()
                .expect("send queues operations should not panic")
                .push_message(self.queue_id, message);

            match result {
                Ok(()) => break Ok(()),
                Err((rejected, notified)) => {
                    message = rejected;

                    tokio::select! {
                        _ = notified => {},
                        error = self.error_rx.clone() => match error {
                            Ok(error) => break Err(error.into()),
                            Err(..) => break Err("task responsible for transfering the messages has panicked".into()),
                        }
                    }
                }
            }
        }
    }

    /// Makes a new [`SendQueue`] for the same connection.
    pub fn make_new(&self, send_buffer_capacity: NonZeroUsize) -> Self {
        let queue_id = self
            .shared_state
            .lock()
            .unwrap()
            .add_queue(send_buffer_capacity);

        Self {
            queue_id,
            shared_state: self.shared_state.clone(),
            error_rx: self.error_rx.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<M: MessageOut> Drop for SendQueue<M> {
    fn drop(&mut self) {
        let Ok(mut guard) = self.shared_state.lock() else {
            return;
        };
        guard.remove_queue(self.queue_id);
    }
}

type SharedState = Arc<Mutex<SendQueuesState>>;

#[derive(Default)]
struct SendQueuesState {
    ready_queue_ids: Vec<QueueId>,
    queues: HashMap<QueueId, QueueState>,
    total_size: usize,
    notify_non_empty: Arc<Notify>,
}

impl SendQueuesState {
    fn add_queue(&mut self, capacity: NonZeroUsize) -> QueueId {
        let notify = Arc::new(Notify::new());
        let queue_id = QueueId(Arc::as_ptr(&notify));
        let state = QueueState {
            messages: Default::default(),
            capacity,
            size: 0,
            notify_free_capacity: notify,
            can_remove_when_empty: false,
        };
        self.queues.insert(queue_id, state);
        queue_id
    }

    fn remove_queue(&mut self, id: QueueId) {
        let Entry::Occupied(mut e) = self.queues.entry(id) else {
            unreachable!();
        };
        if e.get().messages.is_empty() {
            e.remove();
        } else {
            e.get_mut().can_remove_when_empty = true;
        }
    }

    fn push_message(
        &mut self,
        queue_id: QueueId,
        message: Vec<u8>,
    ) -> Result<(), (Vec<u8>, OwnedNotified)> {
        let state = self
            .queues
            .get_mut(&queue_id)
            .expect("send queues state out of sync");

        if state.size >= state.capacity.get() {
            return Err((message, state.notify_free_capacity.clone().notified_owned()));
        }

        if state.messages.is_empty() {
            self.ready_queue_ids.push(queue_id);
        }

        if self.total_size == 0 {
            self.notify_non_empty.notify_waiters();
        }

        state.size += message.len();
        self.total_size += message.len();
        state.messages.push_back(message);

        Ok(())
    }

    fn next_message(&mut self) -> SendQueuesPoll {
        if self.queues.is_empty() {
            return SendQueuesPoll::Terminated;
        }

        let id_idx = Uniform::new(0, self.ready_queue_ids.len())
            .map(|uniform| uniform.sample(&mut rand::rng()));
        let Ok(id_idx) = id_idx else {
            return SendQueuesPoll::NotReady(self.notify_non_empty.clone().notified_owned());
        };
        let id = self.ready_queue_ids[id_idx];
        let Entry::Occupied(mut e) = self.queues.entry(id) else {
            unreachable!()
        };

        let message = e
            .get_mut()
            .messages
            .pop_front()
            .expect("send queues state out of sync");

        let was_full = e.get().size >= e.get().capacity.get();
        e.get_mut().size -= message.len();

        if was_full && e.get().capacity.get() > e.get().size {
            e.get().notify_free_capacity.notify_waiters();
        }

        if e.get().messages.is_empty() {
            self.ready_queue_ids.swap_remove(id_idx);
            if e.get().can_remove_when_empty {
                e.remove();
            }
        }

        self.total_size -= message.len();

        SendQueuesPoll::Message(message)
    }
}

struct QueueState {
    messages: VecDeque<Vec<u8>>,
    capacity: NonZeroUsize,
    size: usize,
    notify_free_capacity: Arc<Notify>,
    can_remove_when_empty: bool,
}

enum SendQueuesPoll {
    NotReady(OwnedNotified),
    Message(Vec<u8>),
    Terminated,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
struct QueueId(*const Notify);

unsafe impl Send for QueueId {}

unsafe impl Sync for QueueId {}

struct BincodeDecoder<M>(PhantomData<fn() -> M>);

impl<M: bincode::Decode<()>> Decoder for BincodeDecoder<M> {
    type Item = M;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match bincode::decode_from_slice(&src[..], bincode::config::standard()) {
            Ok((message, read)) => {
                src.advance(read);
                Ok(Some(message))
            }
            Err(DecodeError::UnexpectedEnd { .. }) => Ok(None),
            Err(err) => Err(io::Error::other(err.to_string())),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, num::NonZeroUsize};

    use bincode::{Decode, Encode};
    use futures::StreamExt;
    use tokio::{
        net::{TcpListener, TcpStream},
        runtime::Handle,
    };

    #[derive(Encode, Decode)]
    struct TestMessage {
        sender_id: usize,
        payload: usize,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn preserves_order() {
        const MESSAGES_PER_WORKER: usize = 100;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let (stream_1, stream_2) =
            tokio::join!(async { listener.accept().await.unwrap().0 }, async {
                TcpStream::connect(address).await.unwrap()
            },);

        let (_stream_1, sink_1) = super::wrap_connection::<_, TestMessage, TestMessage>(
            stream_1,
            NonZeroUsize::new(32).unwrap(),
        );
        let (mut stream_2, _sink_2) = super::wrap_connection::<_, TestMessage, TestMessage>(
            stream_2,
            NonZeroUsize::new(32).unwrap(),
        );

        let workers = Handle::current().metrics().num_workers();
        for i in 0..workers {
            let queue = sink_1.make_new(NonZeroUsize::new(32).unwrap());
            tokio::spawn(async move {
                for payload in 0..MESSAGES_PER_WORKER {
                    queue
                        .send(TestMessage {
                            sender_id: i,
                            payload,
                        })
                        .await
                        .unwrap();
                }
            });
        }

        let mut total = 0;
        let mut received: HashMap<usize, Vec<usize>> = Default::default();
        while total < workers * MESSAGES_PER_WORKER {
            let msg = stream_2.next().await.unwrap().unwrap();
            received.entry(msg.sender_id).or_default().push(msg.payload);
            total += 1;
        }
        for i in 0..workers {
            let received = received.remove(&i).unwrap();
            assert_eq!(received.len(), MESSAGES_PER_WORKER);
            received
                .iter()
                .enumerate()
                .for_each(|(i, payload)| assert_eq!(i, *payload));
        }
    }
}
