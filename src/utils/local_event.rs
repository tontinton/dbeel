use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use pin_project_lite::pin_project;

type SharedListeners = Rc<RefCell<BTreeMap<u64, ListenerState>>>;

struct ListenerState {
    waker: Option<Waker>,
    done: bool,
}

pin_project! {
    pub struct LocalEventListener {
        #[pin]
        id: u64,

        #[pin]
        listeners: SharedListeners,
    }
}

impl Future for LocalEventListener {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut listeners = this.listeners.borrow_mut();

        if let Some(state) = listeners.get(&this.id) {
            if state.done {
                listeners.remove(&this.id);
                return Poll::Ready(());
            }
        }

        listeners.insert(
            *this.id,
            ListenerState {
                waker: Some(cx.waker().clone()),
                done: false,
            },
        );

        Poll::Pending
    }
}

/// An async event that is optimized for single thread use.
pub struct LocalEvent {
    listeners: SharedListeners,
    last_id: Cell<u64>,
}

impl Default for LocalEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalEvent {
    pub fn new() -> Self {
        Self {
            listeners: Rc::new(RefCell::new(BTreeMap::new())),
            last_id: Cell::new(0),
        }
    }

    pub fn listen(&self) -> LocalEventListener {
        let mut listeners = self.listeners.borrow_mut();
        let id = self.last_id.get();

        // Potential bug when an event has a listener with an id that already
        // exists. Because I don't expect there to be so many listeners on a
        // single event that exist for so long, I won't handle it.
        self.last_id.set(id.wrapping_add(1));

        listeners.insert(
            id,
            ListenerState {
                waker: None,
                done: false,
            },
        );

        LocalEventListener {
            id,
            listeners: self.listeners.clone(),
        }
    }

    pub fn notify(&self) {
        let mut listeners = self.listeners.borrow_mut();
        for listener in listeners.values_mut() {
            listener.done = true;
            if let Some(waker) = listener.waker.take() {
                waker.wake();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use futures_lite::future::yield_now;
    use glommio::{spawn_local, LocalExecutorBuilder, Placement};

    use super::*;

    fn run_with_glommio<G, F, T>(fut_gen: G)
    where
        G: FnOnce() -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let builder = LocalExecutorBuilder::new(Placement::Unbound);
        let handle = builder.name("test").spawn(fut_gen).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn sticky_event() {
        run_with_glommio(|| async {
            let event = LocalEvent::new();
            let listener = event.listen();
            event.notify();
            listener.await;
        })
    }

    #[test]
    fn sanity_event() {
        run_with_glommio(|| async {
            let set = Rc::new(Cell::new(false));
            let event = LocalEvent::new();
            let listener = event.listen();

            let cloned_set = set.clone();
            spawn_local(async move {
                yield_now().await;
                cloned_set.set(true);
                event.notify();
            })
            .detach();

            listener.await;
            assert!(set.get());
        })
    }

    #[test]
    fn reuse_event() {
        run_with_glommio(|| async {
            let event = LocalEvent::new();

            let listener1 = event.listen();
            let listener2 = event.listen();

            event.notify();
            listener1.await;

            let listener3 = event.listen();

            event.notify();
            listener3.await;
            listener2.await;
        })
    }
}
