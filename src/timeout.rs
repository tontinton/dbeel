use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use glommio::timer::Timer;
use pin_project_lite::pin_project;

use crate::error::{Error, Result};

pin_project! {
    struct Timeout<F, T>
    where
        F: Future<Output = Result<T>>,
    {
        #[pin]
        future: F,
        #[pin]
        timeout: Timer,
    }
}

impl<F, T> Timeout<F, T>
where
    F: Future<Output = Result<T>>,
{
    fn new(future: F, duration: Duration) -> Self {
        Self {
            future,
            timeout: Timer::new(duration),
        }
    }
}

impl<F, T> Future for Timeout<F, T>
where
    F: Future<Output = Result<T>>,
{
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Pending => {}
            other => return other,
        }

        if this.timeout.poll(cx).is_ready() {
            let err = Err(Error::Timeout);
            Poll::Ready(err)
        } else {
            Poll::Pending
        }
    }
}

pub async fn timeout<F, T>(duration: Duration, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    Timeout::new(future, duration).await
}
