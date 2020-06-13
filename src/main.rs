use core::convert::Infallible as Void;
use core::fmt::{self, Display, Formatter};
use core_error::Error;
use futures::{
    channel::mpsc::unbounded,
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    executor::{block_on, ThreadPool},
    future::pending,
    stream::Map,
    task::Spawn,
    task::SpawnExt,
    StreamExt,
};
use looking_glass::{Erase, ProtocolAny, Reflect, StaticTyped, Typed};
use protocol_mve_transport::{Coalesce, ProtocolMveTransport, Unravel};

fn main() {
    let a = block_on(async move {
        let a = "test".to_owned();
        let a: ProtocolAny<ProtocolMveTransport<_>> = a.erase().await.unwrap();
        a
    });

    let (a_sender, a_receiver) = unbounded();
    let (b_sender, b_receiver) = unbounded();

    let pool = ThreadPool::new().unwrap();

    let spawner = pool.clone();

    pool.spawn(async move {
        Unravel::<
            Void,
            ThreadPool,
            Map<UnboundedReceiver<Vec<u8>>, fn(Vec<u8>) -> Result<Vec<u8>, Void>>,
            UnboundedSender<Vec<u8>>,
            ProtocolAny<ProtocolMveTransport<Box<dyn Error + Send>>>,
        >::new(a_receiver.map(Ok::<Vec<u8>, Void>), b_sender, spawner, a)
        .await
        .unwrap();
    })
    .unwrap();

    let spawner = pool.clone();

    block_on(async move {
        let data = Coalesce::<
            Void,
            ThreadPool,
            Map<UnboundedReceiver<Vec<u8>>, fn(Vec<u8>) -> Result<Vec<u8>, Void>>,
            UnboundedSender<Vec<u8>>,
            ProtocolAny<ProtocolMveTransport<Box<dyn Error + Send>>>,
        >::new(
            b_receiver.map(Ok::<Vec<u8>, Void>),
            a_sender,
            spawner.clone(),
        );
        let data = data.await.unwrap();
        let data: String = data.downcast(spawner).await.unwrap();
        println!("{}", data);
    });
}
