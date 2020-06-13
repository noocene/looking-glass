use core_error::Error;
use downcast_rs::{impl_downcast, Downcast};
use erasure_traits::{FramedTransportCoalesce, FramedTransportUnravel};
use futures::{
    channel::mpsc::{channel, unbounded, UnboundedReceiver, UnboundedSender},
    ready,
    stream::FuturesUnordered,
    stream::Map,
    task::{Spawn, SpawnExt},
    FutureExt, Sink, SinkExt, Stream, StreamExt, TryFuture, TryFutureExt, TryStreamExt,
};
use protocol::{
    allocated::ProtocolError, protocol, CloneContext, Coalesce, ContextReference, Contextualize,
    FinalizeImmediate, Future, Read, ReferenceContext, Unravel, Write,
};
use serde::{Deserialize, Serialize};
use std::{
    any::TypeId,
    borrow::BorrowMut,
    fmt::Debug,
    marker::PhantomData,
    mem::replace,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;

#[protocol]
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Id([u8; 32]);

#[protocol]
#[derive(Debug, Error)]
pub enum ErasedError {
    #[error("protocol error: {0}")]
    Protocol(
        #[source]
        #[from]
        ProtocolError,
    ),
    #[error("underlying error: {0}")]
    Erased(
        #[source]
        #[from]
        Box<dyn Error + Send>,
    ),
}

pub trait Typed {
    type Type: TryFuture<Ok = Id>;

    fn ty() -> Self::Type;
}

trait ProtocolCast: Downcast {
    fn extract(
        self: Box<Self>,
        spawner: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, Box<dyn Error + Send>>> + Send>>;
}

impl_downcast!(ProtocolCast);

struct Extraction {
    stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Box<dyn Error + Send>>> + Send>>,
    sink: Pin<Box<dyn Sink<Vec<u8>, Error = Box<dyn Error + Send>> + Send>>,
}

pub struct ProtocolAny<T> {
    inner: Box<dyn ProtocolCast + Send>,
    marker: PhantomData<T>,
    ty: Id,
}

impl<T> Debug for ProtocolAny<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ProtocolAny")
    }
}

#[derive(Debug, Error)]
#[bounds(where T: Error + 'static, U: Error + 'static)]
pub enum DowncastError<T, U> {
    #[error("incorrect type")]
    TypeMismatch,
    #[error("failed to acquire type parameter id: {0}")]
    StaticTyped(#[source] T),
    #[error("failed to extract erased transport: {0}")]
    Extract(#[source] Box<dyn Error + Send>),
    #[error("coalesce error: {0}")]
    Coalesce(#[source] U),
}

impl<T, V, U> From<DowncastError<T, V>> for (DowncastError<T, V>, Option<U>) {
    fn from(error: DowncastError<T, V>) -> Self {
        (error, None)
    }
}

impl<T> ProtocolAny<T> {
    pub async fn downcast<U: Typed + 'static, S: Clone + Spawn + Send + 'static>(
        self,
        spawner: S,
    ) -> Result<
        U,
        (
            DowncastError<<U::Type as TryFuture>::Error, <T::Coalesce as TryFuture>::Error>,
            Option<Self>,
        ),
    >
    where
        T: FramedTransportCoalesce<
                U,
                Pin<Box<dyn Stream<Item = Result<Vec<u8>, Box<dyn Error + Send>>>>>,
                Pin<Box<dyn Sink<Vec<u8>, Error = Box<dyn Error + Send>>>>,
                S,
            > + 'static,
    {
        if self.inner.as_any().type_id() == TypeId::of::<LocalWrapper<U, T>>() {
            return Ok(self
                .inner
                .into_any()
                .downcast::<LocalWrapper<U, T>>()
                .unwrap()
                .data);
        } else {
            match U::ty().into_future().await {
                Ok(ty) => {
                    if ty != self.ty {
                        Err((DowncastError::TypeMismatch, Some(self)))
                    } else {
                        let Extraction { stream, sink } = self
                            .inner
                            .extract(Box::new(CloneSpawnWrapper {
                                inner: spawner.clone(),
                            }))
                            .into_future()
                            .await
                            .map_err(DowncastError::Extract)?;
                        Ok(T::coalesce(stream, sink, spawner)
                            .into_future()
                            .await
                            .map_err(DowncastError::Coalesce)?)
                    }
                }
                Err(e) => Err((DowncastError::StaticTyped(e), Some(self))),
            }
        }
    }

    pub fn ty(&self) -> Id {
        self.ty
    }
}

struct LocalWrapper<T, U> {
    data: T,
    marker: PhantomData<U>,
}

mod clone_spawn {
    use super::Spawn;

    pub trait CloneSpawn: Spawn + Send {
        fn box_clone(&self) -> Box<dyn CloneSpawn>;
    }
}

pub use clone_spawn::CloneSpawn;

#[derive(Clone)]
struct CloneSpawnWrapper<T: Clone + Spawn> {
    inner: T,
}

impl<T: Clone + Spawn> Spawn for CloneSpawnWrapper<T> {
    fn spawn_obj(
        &self,
        future: futures::future::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        self.inner.spawn_obj(future)
    }
}

impl<T: Clone + Spawn + Send + 'static> CloneSpawn for CloneSpawnWrapper<T> {
    fn box_clone(&self) -> Box<dyn CloneSpawn> {
        Box::new(CloneSpawnWrapper {
            inner: self.inner.clone(),
        })
    }
}

impl Clone for Box<dyn CloneSpawn> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl<T: 'static + Typed + Send, U: 'static> ProtocolCast for LocalWrapper<T, U>
where
    <T::Type as TryFuture>::Error: Error + Send,
    T::Type: Send,
    U: Send
        + FramedTransportUnravel<
            T,
            Map<
                futures::channel::mpsc::Receiver<Vec<u8>>,
                fn(Vec<u8>) -> Result<Vec<u8>, Box<dyn Error + Send>>,
            >,
            futures::channel::mpsc::Sender<Vec<u8>>,
            Box<dyn CloneSpawn>,
        >,
    U::Unravel: Send,
{
    fn extract(
        self: Box<Self>,
        spawner: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, Box<dyn Error + Send>>> + Send>>
    {
        let (a_sender, b_receiver) = channel(0);
        let (b_sender, a_receiver) = channel(0);

        // TODO find a way to handle errors properly
        let e = spawner.spawn(
            U::unravel(self.data, a_receiver.map(Ok), a_sender, spawner.clone()).map(|_| ()),
        );

        Box::pin(async move {
            e.map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;

            Ok(Extraction {
                stream: Box::pin(b_receiver.map(Ok)),
                sink: Box::pin(b_sender.sink_map_err(|e| Box::new(e) as Box<dyn Error + Send>)),
            })
        })
    }
}

pub trait Erase<T>: Typed {
    type Erase: TryFuture<Ok = ProtocolAny<T>>;

    fn erase(self) -> Self::Erase;
}

impl<
        T: Typed + Send + 'static,
        U: FramedTransportUnravel<
                T,
                Map<
                    futures::channel::mpsc::Receiver<Vec<u8>>,
                    fn(Vec<u8>) -> Result<Vec<u8>, Box<dyn Error + Send>>,
                >,
                futures::channel::mpsc::Sender<Vec<u8>>,
                Box<dyn CloneSpawn>,
            > + Send
            + 'static,
    > Erase<U> for T
where
    <T::Type as TryFuture>::Error: Error + Send,
    T::Type: Send,
    U::Unravel: Send,
{
    type Erase = Pin<
        Box<
            dyn futures::Future<Output = Result<ProtocolAny<U>, <T::Type as TryFuture>::Error>>
                + Send,
        >,
    >;

    fn erase(self) -> Self::Erase {
        let id = Self::ty().into_future();

        Box::pin(async move {
            Ok(ProtocolAny {
                ty: id.await?,
                marker: PhantomData,
                inner: Box::new(LocalWrapper::<_, U> {
                    data: self,
                    marker: PhantomData,
                }),
            })
        })
    }
}

mod join_protocol_any {
    use super::{CloneContext, Contextualize, Deserialize, Id, PhantomData, Read, Serialize};

    pub enum JoinProtocolAny<T, C: ?Sized + Read<(Id, <C as Contextualize>::Handle)> + CloneContext> {
        Read(PhantomData<T>),
        Context(C::JoinOutput, Id),
        Done,
    }

    use super::Error;

    #[derive(Debug, Error)]
    #[bounds(
        where
            T: Error + 'static,
            U: Error + 'static,
    )]
    pub enum JoinProtocolAnyError<T, U> {
        #[error("failed to join owned context: {0}")]
        Contextualize(#[source] T),
        #[error("failed to read handle for owned context: {0}")]
        Read(#[source] U),
    }

    #[derive(Serialize, Deserialize, PartialEq)]
    pub enum ProtocolAnyArgument {
        Extract,
        Drop,
    }

    pub enum RemoteWrapperFinalize {
        Write,
        Flush,
        Done,
    }
}

use join_protocol_any::{
    JoinProtocolAny, JoinProtocolAnyError, ProtocolAnyArgument, RemoteWrapperFinalize,
};

impl<T, C: ?Sized + Read<(Id, <C as Contextualize>::Handle)> + CloneContext> Unpin
    for JoinProtocolAny<T, C>
{
}

impl<T, C: ?Sized + Read<(Id, <C as Contextualize>::Handle)> + CloneContext> Future<C>
    for JoinProtocolAny<T, C>
where
    C: Unpin,
    C::JoinOutput: Unpin,
    C::Context: FinalizeImmediate<RemoteWrapperFinalize>
        + Write<ProtocolAnyArgument>
        + Write<Vec<u8>>
        + Read<Vec<u8>>
        + Send
        + 'static
        + Unpin,
    <C::Context as Write<ProtocolAnyArgument>>::Error: Send + Error + 'static,
    <C::Context as Write<Vec<u8>>>::Error: Send + Error + 'static,
    <C::Context as Read<Vec<u8>>>::Error: Send + Error + 'static,
    RemoteWrapperFinalize: Future<<C::Context as FinalizeImmediate<RemoteWrapperFinalize>>::Target>,
{
    type Ok = ProtocolAny<T>;
    type Error = JoinProtocolAnyError<<C::JoinOutput as Future<C>>::Error, C::Error>;

    fn poll<R: BorrowMut<C>>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut ctx: R,
    ) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = &mut *self;

        let ctx = ctx.borrow_mut();

        loop {
            match &mut *this {
                JoinProtocolAny::Read(_) => {
                    let (id, handle) =
                        ready!(Pin::new(&mut *ctx).read(cx)).map_err(JoinProtocolAnyError::Read)?;
                    *this = JoinProtocolAny::Context(ctx.join_owned(handle), id);
                }
                JoinProtocolAny::Context(future, id) => {
                    let context = ready!(Pin::new(future).poll(cx, ctx.borrow_mut()))
                        .map_err(JoinProtocolAnyError::Contextualize)?;
                    let ty = *id;
                    *this = JoinProtocolAny::Done;
                    return Poll::Ready(Ok(ProtocolAny {
                        ty,
                        marker: PhantomData,
                        inner: Box::new(RemoteWrapper {
                            context: Some(context),
                        }),
                    }));
                }
                JoinProtocolAny::Done => panic!("JoinProtocolAny polled after completion"),
            }
        }
    }
}

impl<T, C: ?Sized + Read<(Id, <C as Contextualize>::Handle)> + CloneContext> JoinProtocolAny<T, C> {
    pub fn new() -> Self {
        JoinProtocolAny::Read(PhantomData)
    }
}

enum RemoteWrapperExtractState {
    Write,
    Flush,
}

struct RemoteWrapperExtract<C> {
    context: Option<C>,
    state: RemoteWrapperExtractState,
}

struct RemoteExtraction<C> {
    context: C,
}

impl<C: Read<Vec<u8>> + Unpin> Stream for RemoteExtraction<C> {
    type Item = Result<Vec<u8>, C::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.context).read(cx).map(Some)
    }
}

impl<C: Write<Vec<u8>> + Unpin> Sink<Vec<u8>> for RemoteExtraction<C> {
    type Error = C::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.context).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        Pin::new(&mut self.context).write(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.context).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.context).poll_flush(cx)
    }
}

impl<C: Write<ProtocolAnyArgument> + Unpin> Future<C> for RemoteWrapperFinalize {
    type Ok = ();
    type Error = <C as Write<ProtocolAnyArgument>>::Error;

    fn poll<R: BorrowMut<C>>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut ctx: R,
    ) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = &mut *self;
        let mut ctx = Pin::new(ctx.borrow_mut());

        loop {
            match this {
                RemoteWrapperFinalize::Write => {
                    ready!(ctx.as_mut().poll_ready(cx))?;
                    ctx.as_mut().write(ProtocolAnyArgument::Drop)?;
                    *this = RemoteWrapperFinalize::Flush;
                }
                RemoteWrapperFinalize::Flush => {
                    ready!(ctx.as_mut().poll_flush(cx))?;
                    *this = RemoteWrapperFinalize::Done;
                    return Poll::Ready(Ok(()));
                }
                RemoteWrapperFinalize::Done => {
                    panic!("RemoteWrapperFinalize polled after completion")
                }
            }
        }
    }
}

struct RemoteWrapper<C: FinalizeImmediate<RemoteWrapperFinalize>>
where
    RemoteWrapperFinalize: Future<<C as FinalizeImmediate<RemoteWrapperFinalize>>::Target>,
{
    context: Option<C>,
}

impl<C: FinalizeImmediate<RemoteWrapperFinalize>> Drop for RemoteWrapper<C>
where
    RemoteWrapperFinalize: Future<<C as FinalizeImmediate<RemoteWrapperFinalize>>::Target>,
{
    fn drop(&mut self) {
        if let Some(mut context) = self.context.take() {
            let _ = context.finalize_immediate(RemoteWrapperFinalize::Write);
        }
    }
}

impl<C: Send + Unpin + Write<ProtocolAnyArgument> + Write<Vec<u8>> + Read<Vec<u8>> + 'static>
    futures::Future for RemoteWrapperExtract<C>
where
    <C as Write<ProtocolAnyArgument>>::Error: Send + Error + 'static,
    <C as Write<Vec<u8>>>::Error: Send + Error + 'static,
    <C as Read<Vec<u8>>>::Error: Send + Error + 'static,
{
    type Output = Result<Extraction, Box<dyn Error + Send>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            match &mut this.state {
                RemoteWrapperExtractState::Write => {
                    let mut context = Pin::new(this.context.as_mut().unwrap());
                    ready!(Write::<ProtocolAnyArgument>::poll_ready(
                        context.as_mut(),
                        cx
                    ))
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;
                    context
                        .write(ProtocolAnyArgument::Extract)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;
                    this.state = RemoteWrapperExtractState::Flush;
                }
                RemoteWrapperExtractState::Flush => {
                    ready!(Write::<ProtocolAnyArgument>::poll_flush(
                        Pin::new(this.context.as_mut().unwrap(),),
                        cx
                    ))
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?;
                    let (sink, stream) = RemoteExtraction {
                        context: this.context.take().unwrap(),
                    }
                    .split();
                    return Poll::Ready(Ok(Extraction {
                        stream: Box::pin(stream.map_err(|e| Box::new(e) as Box<dyn Error + Send>)),
                        sink: Box::pin(sink.sink_map_err(|e| Box::new(e) as Box<dyn Error + Send>)),
                    }));
                }
            }
        }
    }
}

impl<
        C: Write<ProtocolAnyArgument>
            + Write<Vec<u8>>
            + Read<Vec<u8>>
            + Unpin
            + Send
            + 'static
            + FinalizeImmediate<RemoteWrapperFinalize>,
    > ProtocolCast for RemoteWrapper<C>
where
    <C as Write<ProtocolAnyArgument>>::Error: Send + Error + 'static,
    <C as Write<Vec<u8>>>::Error: Send + Error + 'static,
    <C as Read<Vec<u8>>>::Error: Send + Error + 'static,
    RemoteWrapperFinalize: Future<<C as FinalizeImmediate<RemoteWrapperFinalize>>::Target>,
{
    fn extract(
        mut self: Box<Self>,
        spawner: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, Box<dyn Error + Send>>> + Send>>
    {
        Box::pin(RemoteWrapperExtract {
            context: self.context.take(),
            state: RemoteWrapperExtractState::Write,
        })
    }
}

impl<T, C: ?Sized + Read<(Id, <C as Contextualize>::Handle)> + CloneContext + Unpin> Coalesce<C>
    for ProtocolAny<T>
where
    C::JoinOutput: Unpin,
    C::Context: Write<Vec<u8>>
        + Read<Vec<u8>>
        + Write<ProtocolAnyArgument>
        + Send
        + FinalizeImmediate<RemoteWrapperFinalize>
        + Unpin
        + 'static,
    <C::Context as Write<ProtocolAnyArgument>>::Error: Send + Error + 'static,
    <C::Context as Write<Vec<u8>>>::Error: Send + Error + 'static,
    <C::Context as Read<Vec<u8>>>::Error: Send + Error + 'static,
    RemoteWrapperFinalize: Future<<C::Context as FinalizeImmediate<RemoteWrapperFinalize>>::Target>,
{
    type Future = JoinProtocolAny<T, C>;

    fn coalesce() -> Self::Future {
        JoinProtocolAny::new()
    }
}

enum ForkProtocolAnyState<C: ?Sized + ReferenceContext> {
    Init,
    Contextualize(C::ForkOutput),
    Write(C::Handle, C::Context),
    Flush(C::Context),
    Done,
}

enum Transfer {
    Clear,
    Write(Vec<u8>),
    Flush,
    Done,
}

enum FinalizeProtocolAnyState<C: ?Sized> {
    Read,
    Extract(
        Pin<Box<dyn futures::Future<Output = Result<Extraction, Box<dyn Error + Send>>> + Send>>,
        Option<UnboundedReceiver<futures::future::FutureObj<'static, ()>>>,
    ),
    ExtractTransfer(
        Pin<Box<dyn Stream<Item = Result<Vec<u8>, Box<dyn Error + Send>>> + Send>>,
        Transfer,
        Pin<Box<dyn Sink<Vec<u8>, Error = Box<dyn Error + Send>> + Send>>,
        Transfer,
        Option<UnboundedReceiver<futures::future::FutureObj<'static, ()>>>,
    ),
    Done(PhantomData<C>),
}

mod fork_protocol_any {
    use super::{
        FinalizeProtocolAnyState, ForkProtocolAnyState, FuturesUnordered, ProtocolAny,
        ReferenceContext,
    };

    pub struct ForkProtocolAny<C: ?Sized + ReferenceContext, T> {
        pub(crate) data: Option<ProtocolAny<T>>,
        pub(crate) state: ForkProtocolAnyState<C>,
    }

    impl<C: ?Sized + ReferenceContext, T> Unpin for ForkProtocolAny<C, T> {}

    pub struct FinalizeProtocolAny<C: ?Sized + ReferenceContext, T> {
        pub(crate) data: Option<ProtocolAny<T>>,
        pub(crate) context: C::Context,
        pub(crate) futures: FuturesUnordered<futures::future::FutureObj<'static, ()>>,
        pub(crate) state: FinalizeProtocolAnyState<C>,
    }

    impl<T, C: ?Sized + ReferenceContext> Unpin for FinalizeProtocolAny<C, T> {}
}

#[derive(Clone)]
struct Spawner {
    sender: UnboundedSender<futures::future::FutureObj<'static, ()>>,
}

impl Spawn for Spawner {
    fn spawn_obj(
        &self,
        future: futures::future::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        let _ = self.sender.unbounded_send(future);
        Ok(())
    }
}

use fork_protocol_any::{FinalizeProtocolAny, ForkProtocolAny};

#[derive(Debug, Error)]
#[bounds(where
    T: Error + 'static,
    U: Error + 'static,
    V: Error + 'static,
    W: Error + 'static,
    X: Error + 'static
)]
pub enum ProtocolAnyUnravelError<T, U, V, W, X> {
    #[error("contextualize failed: {0}")]
    Contextualize(#[source] T),
    #[error("write failed: {0}")]
    Write(#[source] U),
    #[error("read failed: {0}")]
    Read(#[source] V),
    #[error("extract failed: {0}")]
    Extract(#[source] Box<dyn Error + Send>),
    #[error("extract data write failed: {0}")]
    WriteData(#[source] W),
    #[error("extract data read failed: {0}")]
    ReadData(#[source] X),
}

type ProtocolAnyError<C> = ProtocolAnyUnravelError<
    <<C as ReferenceContext>::ForkOutput as Future<C>>::Error,
    <C as Write<(Id, <C as Contextualize>::Handle)>>::Error,
    <<<C as ReferenceContext>::Context as ContextReference<C>>::Target as Read<
        ProtocolAnyArgument,
    >>::Error,
    <<<C as ReferenceContext>::Context as ContextReference<C>>::Target as Write<Vec<u8>>>::Error,
    <<<C as ReferenceContext>::Context as ContextReference<C>>::Target as Read<Vec<u8>>>::Error,
>;

impl<T, C: ?Sized + Write<(Id, <C as Contextualize>::Handle)> + ReferenceContext + Unpin> Future<C>
    for FinalizeProtocolAny<C, T>
where
    <C::Context as ContextReference<C>>::Target:
        Write<Vec<u8>> + Read<Vec<u8>> + Read<ProtocolAnyArgument> + Send + Unpin + 'static,
{
    type Ok = ();
    type Error = ProtocolAnyError<C>;

    fn poll<R: BorrowMut<C>>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut ctx: R,
    ) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = &mut *self;
        let ctx = this.context.with(ctx.borrow_mut());

        loop {
            match &mut this.state {
                FinalizeProtocolAnyState::Read => {
                    match ready!(Read::<ProtocolAnyArgument>::read(Pin::new(&mut *ctx), cx))
                        .map_err(ProtocolAnyUnravelError::Read)?
                    {
                        ProtocolAnyArgument::Extract => {
                            let (sender, receiver) = unbounded();

                            this.state = FinalizeProtocolAnyState::Extract(
                                this.data.take().unwrap().inner.extract(Box::new(
                                    CloneSpawnWrapper {
                                        inner: Spawner { sender },
                                    },
                                )),
                                Some(receiver),
                            );
                        }
                        ProtocolAnyArgument::Drop => {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
                FinalizeProtocolAnyState::Extract(fut, receiver) => {
                    if let Some(recv) = receiver {
                        match Pin::new(recv).poll_next(cx) {
                            Poll::Ready(Some(item)) => {
                                this.futures.push(item);
                            }
                            Poll::Ready(None) => {
                                receiver.take();
                            }
                            _ => {}
                        }
                    }
                    let _ = Pin::new(&mut this.futures).poll_next(cx);
                    let extraction =
                        ready!(fut.as_mut().poll(cx)).map_err(ProtocolAnyUnravelError::Extract)?;
                    if let FinalizeProtocolAnyState::Extract(_, receiver) =
                        replace(&mut this.state, FinalizeProtocolAnyState::Done(PhantomData))
                    {
                        this.state = FinalizeProtocolAnyState::ExtractTransfer(
                            extraction.stream,
                            Transfer::Clear,
                            extraction.sink,
                            Transfer::Clear,
                            receiver,
                        );
                    } else {
                        panic!("invalid state")
                    }
                }
                FinalizeProtocolAnyState::ExtractTransfer(
                    stream,
                    inbound,
                    sink,
                    outbound,
                    receiver,
                ) => {
                    if let Some(recv) = receiver {
                        match Pin::new(recv).poll_next(cx) {
                            Poll::Ready(Some(item)) => {
                                this.futures.push(item);
                            }
                            Poll::Ready(None) => {
                                receiver.take();
                            }
                            _ => {}
                        }
                    }
                    let _ = Pin::new(&mut this.futures).poll_next(cx);

                    let mut outbound_done = false;

                    loop {
                        match outbound {
                            Transfer::Write(data) => {
                                let mut ctx = Pin::new(&mut *ctx);
                                if let Poll::Pending =
                                    Write::<Vec<u8>>::poll_ready(ctx.as_mut(), cx)
                                        .map_err(ProtocolAnyUnravelError::WriteData)?
                                {
                                    break;
                                }

                                ctx.write(replace(data, Vec::new()))
                                    .map_err(ProtocolAnyUnravelError::WriteData)?;

                                *outbound = Transfer::Flush;
                            }
                            Transfer::Flush => {
                                if let Poll::Pending =
                                    Write::<Vec<u8>>::poll_flush(Pin::new(&mut *ctx), cx)
                                {
                                    break;
                                }

                                *outbound = Transfer::Clear;
                            }
                            Transfer::Clear => match Pin::new(stream).poll_next(cx) {
                                Poll::Ready(None) => {
                                    *outbound = Transfer::Done;
                                    outbound_done = true;
                                }
                                Poll::Ready(Some(Err(e))) => {
                                    return Poll::Ready(Err(ProtocolAnyUnravelError::Extract(e)))
                                }
                                Poll::Ready(Some(Ok(data))) => {
                                    *outbound = Transfer::Write(data);
                                }
                                _ => {}
                            },
                            Transfer::Done => {
                                outbound_done = true;
                            }
                        }
                        break;
                    }

                    loop {
                        match inbound {
                            Transfer::Write(data) => {
                                if let Poll::Pending = Pin::new(&mut *sink)
                                    .poll_ready(cx)
                                    .map_err(ProtocolAnyUnravelError::Extract)?
                                {
                                    break;
                                }

                                Pin::new(&mut *sink)
                                    .start_send(replace(data, Vec::new()))
                                    .map_err(ProtocolAnyUnravelError::Extract)?;

                                *inbound = Transfer::Flush;
                            }
                            Transfer::Flush => {
                                if let Poll::Pending = Pin::new(&mut *sink)
                                    .poll_flush(cx)
                                    .map_err(ProtocolAnyUnravelError::Extract)?
                                {
                                    break;
                                }

                                *inbound = Transfer::Clear;
                            }
                            Transfer::Clear => {
                                if outbound_done && this.futures.len() == 0 {
                                    this.state = FinalizeProtocolAnyState::Done(PhantomData);
                                    return Poll::Ready(Ok(()));
                                }

                                match Pin::new(&mut *ctx).read(cx) {
                                    Poll::Ready(Err(e)) => {
                                        return Poll::Ready(Err(ProtocolAnyUnravelError::ReadData(
                                            e,
                                        )))
                                    }
                                    Poll::Ready(Ok(data)) => {
                                        *inbound = Transfer::Write(data);
                                    }
                                    _ => {}
                                }
                            }
                            Transfer::Done => {}
                        }
                        break;
                    }
                }
                FinalizeProtocolAnyState::Done(_) => {
                    panic!("FinalizeProtocolAny polled after completion")
                }
            }
        }
    }
}

impl<T, C: ?Sized + Write<(Id, <C as Contextualize>::Handle)> + ReferenceContext + Unpin> Future<C>
    for ForkProtocolAny<C, T>
where
    C::ForkOutput: Unpin,
    <C::Context as ContextReference<C>>::Target:
        Write<Vec<u8>> + Read<Vec<u8>> + Read<ProtocolAnyArgument> + Send + Unpin + 'static,
{
    type Ok = FinalizeProtocolAny<C, T>;
    type Error = ProtocolAnyError<C>;

    fn poll<R: BorrowMut<C>>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut ctx: R,
    ) -> Poll<Result<Self::Ok, Self::Error>> {
        let this = &mut *self;
        let ctx = ctx.borrow_mut();

        loop {
            match &mut this.state {
                ForkProtocolAnyState::Init => {
                    this.state = ForkProtocolAnyState::Contextualize(ctx.fork_ref());
                }
                ForkProtocolAnyState::Contextualize(fut) => {
                    let (context, handle) = ready!(Pin::new(fut).poll(cx, &mut *ctx))
                        .map_err(ProtocolAnyUnravelError::Contextualize)?;
                    this.state = ForkProtocolAnyState::Write(handle, context);
                }
                ForkProtocolAnyState::Write(handle, context) => {
                    let mut ctx = Pin::new(&mut *ctx);
                    ready!(ctx.as_mut().poll_ready(cx)).map_err(ProtocolAnyUnravelError::Write)?;
                    if let ForkProtocolAnyState::Write(handle, context) =
                        replace(&mut this.state, ForkProtocolAnyState::Done)
                    {
                        ctx.write((this.data.as_ref().unwrap().ty, handle))
                            .map_err(ProtocolAnyUnravelError::Write)?;
                        this.state = ForkProtocolAnyState::Flush(context);
                    } else {
                        panic!("invalid state")
                    }
                }
                ForkProtocolAnyState::Flush(context) => {
                    ready!(Pin::new(&mut *ctx).poll_ready(cx))
                        .map_err(ProtocolAnyUnravelError::Write)?;
                    if let ForkProtocolAnyState::Flush(context) =
                        replace(&mut this.state, ForkProtocolAnyState::Done)
                    {
                        return Poll::Ready(Ok(FinalizeProtocolAny {
                            context,
                            data: this.data.take(),
                            state: FinalizeProtocolAnyState::Read,
                            futures: FuturesUnordered::new(),
                        }));
                    } else {
                        panic!("invalid state")
                    }
                }
                ForkProtocolAnyState::Done => panic!("ForkProtocolAny polled after completion"),
            }
        }
    }
}

impl<T, C: ?Sized + Write<(Id, <C as Contextualize>::Handle)> + ReferenceContext + Unpin> Unravel<C>
    for ProtocolAny<T>
where
    <C::Context as ContextReference<C>>::Target:
        Write<Vec<u8>> + Read<Vec<u8>> + Read<ProtocolAnyArgument> + Send + Unpin + 'static,
    C::ForkOutput: Unpin,
{
    type Target = ForkProtocolAny<C, T>;
    type Finalize = FinalizeProtocolAny<C, T>;

    fn unravel(self) -> Self::Target {
        ForkProtocolAny {
            data: Some(self),
            state: ForkProtocolAnyState::Init,
        }
    }
}
