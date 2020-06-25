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

extern crate self as looking_glass;

pub use derive::typed;
#[doc(hidden)]
pub use highway;

#[protocol]
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct Id([u64; 4]);

impl Id {
    #[doc(hidden)]
    pub fn hidden_new_do_not_call_manually(item: [u64; 4]) -> Self {
        Id(item)
    }
    #[doc(hidden)]
    pub fn hidden_extract_do_not_call_manually(self) -> [u64; 4] {
        self.0
    }
}

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
    fn ty() -> Id;
}

trait ProtocolCast: Downcast {
    fn extract(
        self: Box<Self>,
        spawner: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, ErasedError>> + Send>>;
}

impl_downcast!(ProtocolCast);

struct Extraction {
    stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, ErasedError>> + Send>>,
    sink: Pin<Box<dyn Sink<Vec<u8>, Error = ErasedError> + Send>>,
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
#[bounds(where T: Error + 'static)]
pub enum DowncastError<T> {
    #[error("incorrect type")]
    TypeMismatch,
    #[error("failed to extract erased transport: {0}")]
    Extract(#[source] ErasedError),
    #[error("coalesce error: {0}")]
    Coalesce(#[source] T),
}

impl<T, U> From<DowncastError<T>> for (DowncastError<T>, Option<U>) {
    fn from(error: DowncastError<T>) -> Self {
        (error, None)
    }
}

pub trait Eraser<U, S>:
    FramedTransportCoalesce<
    U,
    Pin<Box<dyn Stream<Item = Result<Vec<u8>, ErasedError>> + Send>>,
    Pin<Box<dyn Sink<Vec<u8>, Error = ErasedError> + Send>>,
    S,
>
{
}

impl<
        T: FramedTransportCoalesce<
            U,
            Pin<Box<dyn Stream<Item = Result<Vec<u8>, ErasedError>> + Send>>,
            Pin<Box<dyn Sink<Vec<u8>, Error = ErasedError> + Send>>,
            S,
        >,
        U,
        S,
    > Eraser<U, S> for T
{
}

impl<T> ProtocolAny<T> {
    pub async fn downcast<U: Typed + 'static, S: Clone + Spawn + Send + 'static>(
        self,
        spawner: S,
    ) -> Result<
        U,
        (
            DowncastError<<T::Coalesce as TryFuture>::Error>,
            Option<Self>,
        ),
    >
    where
        T: FramedTransportCoalesce<
                U,
                Pin<Box<dyn Stream<Item = Result<Vec<u8>, ErasedError>> + Send>>,
                Pin<Box<dyn Sink<Vec<u8>, Error = ErasedError> + Send>>,
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
            let ty = U::ty();
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

use clone_spawn::CloneSpawn;

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
    U: Send
        + FramedTransportUnravel<
            T,
            Map<
                futures::channel::mpsc::Receiver<Vec<u8>>,
                fn(Vec<u8>) -> Result<Vec<u8>, ErasedError>,
            >,
            futures::channel::mpsc::Sender<Vec<u8>>,
            Box<dyn CloneSpawn>,
        >,
    U::Unravel: Send,
{
    fn extract(
        self: Box<Self>,
        spawner: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, ErasedError>> + Send>> {
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
                sink: Box::pin(b_sender.sink_map_err(|e| ErasedError::Erased(Box::new(e)))),
            })
        })
    }
}

pub trait Erase<T>: Typed {
    fn erase(self) -> ProtocolAny<T>;
}

impl<
        T: Typed + Send + 'static,
        U: FramedTransportUnravel<
                T,
                Map<
                    futures::channel::mpsc::Receiver<Vec<u8>>,
                    fn(Vec<u8>) -> Result<Vec<u8>, ErasedError>,
                >,
                futures::channel::mpsc::Sender<Vec<u8>>,
                Box<dyn CloneSpawn>,
            > + Send
            + 'static,
    > Erase<U> for T
where
    U::Unravel: Send,
{
    fn erase(self) -> ProtocolAny<U> {
        let id = Self::ty();

        ProtocolAny {
            ty: id,
            marker: PhantomData,
            inner: Box::new(LocalWrapper::<_, U> {
                data: self,
                marker: PhantomData,
            }),
        }
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
    type Output = Result<Extraction, ErasedError>;

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
                        stream: Box::pin(stream.map_err(|e| ErasedError::Erased(Box::new(e)))),
                        sink: Box::pin(sink.sink_map_err(|e| ErasedError::Erased(Box::new(e)))),
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
        _: Box<dyn CloneSpawn>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Extraction, ErasedError>> + Send>> {
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
        Pin<Box<dyn futures::Future<Output = Result<Extraction, ErasedError>> + Send>>,
        Option<UnboundedReceiver<futures::future::FutureObj<'static, ()>>>,
    ),
    ExtractTransfer(
        Pin<Box<dyn Stream<Item = Result<Vec<u8>, ErasedError>> + Send>>,
        Transfer,
        Pin<Box<dyn Sink<Vec<u8>, Error = ErasedError> + Send>>,
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
    Extract(#[source] ErasedError),
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
                ForkProtocolAnyState::Write(_, _) => {
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
                ForkProtocolAnyState::Flush(_) => {
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

derive::type_primitives! {
    T | { [T; 0000] }, T | { [T; 0001] }, T | { [T; 0002] },
    T | { [T; 0003] }, T | { [T; 0004] }, T | { [T; 0005] },
    T | { [T; 0006] }, T | { [T; 0007] }, T | { [T; 0008] },
    T | { [T; 0009] }, T | { [T; 0010] }, T | { [T; 0011] },
    T | { [T; 0012] }, T | { [T; 0013] }, T | { [T; 0014] },
    T | { [T; 0015] }, T | { [T; 0016] }, T | { [T; 0017] },
    T | { [T; 0018] }, T | { [T; 0019] }, T | { [T; 0020] },
    T | { [T; 0021] }, T | { [T; 0022] }, T | { [T; 0023] },
    T | { [T; 0024] }, T | { [T; 0025] }, T | { [T; 0026] },
    T | { [T; 0027] }, T | { [T; 0028] }, T | { [T; 0029] },
    T | { [T; 0030] }, T | { [T; 0031] }, T | { [T; 0032] },
    T | { [T; 0064] }, T | { [T; 0128] }, T | { [T; 0256] },
    T | { [T; 0512] }, T | { [T; 1024] }, T | { [T; 2048] },
    T | { [T; 4096] }, T | { [T; 8192] },
    T | { Option<T> }, T, E | { Result<T, E> },
    T | { PhantomData<T> },
    T | { Vec<T> },
    T | { Pin<Box<dyn futures::Future<Output = T>>> },
    T | { Pin<Box<dyn futures::Future<Output = T> + Sync>> },
    T | { Pin<Box<dyn futures::Future<Output = T> + Send>> },
    T | { Pin<Box<dyn futures::Future<Output = T> + Sync + Send>> },
    T | { Pin<Box<dyn Stream<Item = T>>> },
    T | { Pin<Box<dyn Stream<Item = T> + Sync>> },
    T | { Pin<Box<dyn Stream<Item = T> + Send>> },
    T | { Pin<Box<dyn Stream<Item = T> + Sync + Send>> },
    T, E | { Pin<Box<dyn Sink<T, Error = E>>> },
    T, E | { Pin<Box<dyn Sink<T, Error = E> + Sync>> },
    T, E | { Pin<Box<dyn Sink<T, Error = E> + Send>> },
    T, E | { Pin<Box<dyn Sink<T, Error = E> + Sync + Send>> },
    | { Box<dyn Error> }, | { Box<dyn Error + Sync> },
    | { Box<dyn Error + Send> }, | { Box<dyn Error + Sync + Send> },
    | { u8 }, | { u16 }, | { u32 }, | { u64 }, | { u128 }, | { usize },
    | { i8 }, | { i16 }, | { i32 }, | { i64 }, | { i128 }, | { isize },
    | { bool }, | { char }, | { f32 }, | { f64 }, | { () },
    | { String },
}

macro_rules! fn_impls {
    (
        $(($($marker:ident)*) => (
            $($name:ident)*
        ))+
    ) => {
        derive::type_primitives! {
            $(
                Ret $(,$name)* | { Box<dyn Fn($($name,)*) -> Ret $(+ $marker)*> },
                Ret $(,$name)* | { Box<dyn FnMut($($name,)*) -> Ret $(+ $marker)*> },
                Ret $(,$name)* | { Box<dyn FnOnce($($name,)*) -> Ret $(+ $marker)*> },
            )+
        }
    };
}

macro_rules! tuple_impls {
    (
        $(
            ($($name:ident)*)
        )+
    ) => {
        derive::type_primitives! {
            $(
                $($name),* | { ($($name,)*) },
            )+
        }
    };
}

tuple_impls! {
    (T0)
    (T0 T1)
    (T0 T1 T2 T3)
    (T0 T1 T2 T3 T4)
    (T0 T1 T2 T3 T4 T5)
    (T0 T1 T2 T3 T4 T5 T6)
    (T0 T1 T2 T3 T4 T5 T6 T7)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14)
    (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15)
}

macro_rules! marker_variants {
    ($(
        $($marker:ident)*
    ),+) => {
        $(
            fn_impls! {
                ($($marker)*) => ()
                ($($marker)*) => (T0)
                ($($marker)*) => (T0 T1)
                ($($marker)*) => (T0 T1 T2)
                ($($marker)*) => (T0 T1 T2 T3)
                ($($marker)*) => (T0 T1 T2 T3 T4)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14)
                ($($marker)*) => (T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15)
            }
        )+
    };
}

marker_variants! {
    ,
    Sync,
    Send, Sync Send
}
