use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use crate::{
    cluster_async::OperationTarget, cluster_client::RetryParams, cluster_routing::Redirect,
    types::RetryMethod, Cmd, RedisResult,
};

use futures_util::{future::BoxFuture, ready};
use log::trace;
use pin_project_lite::pin_project;
use tokio::sync::oneshot;

use super::{
    routing::{InternalRoutingInfo, InternalSingleNodeRouting},
    OperationResult, PollFlushAction, Response,
};

#[derive(Clone)]
pub(super) enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        routing: InternalRoutingInfo<C>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        route: InternalSingleNodeRouting<C>,
    },
}

pub(super) enum Retry<C> {
    Immediately {
        request: PendingRequest<C>,
    },
    MoveToPending {
        request: PendingRequest<C>,
    },
    AfterSleep {
        request: PendingRequest<C>,
        sleep_duration: Duration,
    },
}

impl<C> CmdArg<C> {
    fn set_redirect(&mut self, redirect: Option<Redirect>) {
        if let Some(redirect) = redirect {
            match self {
                CmdArg::Cmd { routing, .. } => match routing {
                    InternalRoutingInfo::SingleNode(route) => {
                        let redirect = InternalSingleNodeRouting::Redirect {
                            redirect,
                            previous_routing: Box::new(std::mem::take(route)),
                        }
                        .into();
                        *routing = redirect;
                    }
                    InternalRoutingInfo::MultiNode(_) => {
                        panic!("Cannot redirect multinode requests")
                    }
                },
                CmdArg::Pipeline { route, .. } => {
                    let redirect = InternalSingleNodeRouting::Redirect {
                        redirect,
                        previous_routing: Box::new(std::mem::take(route)),
                    };
                    *route = redirect;
                }
            }
        }
    }

    fn reset_routing(&mut self) {
        let fix_route = |route: &mut InternalSingleNodeRouting<C>| {
            match route {
                InternalSingleNodeRouting::Redirect {
                    previous_routing, ..
                } => {
                    let previous_routing = std::mem::take(previous_routing.as_mut());
                    *route = previous_routing;
                }
                // If a specific connection is specified, then reconnecting without resetting the routing
                // will mean that the request is still routed to the old connection.
                InternalSingleNodeRouting::Connection { identifier, .. } => {
                    *route = InternalSingleNodeRouting::ByAddress(std::mem::take(identifier));
                }
                _ => {}
            }
        };
        match self {
            CmdArg::Cmd { routing, .. } => {
                if let InternalRoutingInfo::SingleNode(route) = routing {
                    fix_route(route);
                }
            }
            CmdArg::Pipeline { route, .. } => {
                fix_route(route);
            }
        }
    }
}

pin_project! {
    #[project = RequestStateProj]
    pub(super) enum RequestState<F> {
        Future {
            #[pin]
            future: F,
        },
        Sleep {
            #[pin]
            sleep: BoxFuture<'static, ()>,
        },
    }
}

// A wrapper that defines the expectation regarding the result of a request.
pub(super) enum ResultExpectation {
    // request was received from the user, and so must continue as long as the sender is alive
    External(oneshot::Sender<RedisResult<Response>>),
    // request was created internally, and must continue regardless of the response.
    Internal,
}

impl ResultExpectation {
    pub(super) fn send(self, result: RedisResult<Response>) {
        let _ = match self {
            ResultExpectation::External(sender) => sender.send(result),
            ResultExpectation::Internal => Ok(()),
        };
    }

    pub(super) fn is_closed(&self) -> bool {
        match self {
            ResultExpectation::External(sender) => sender.is_closed(),
            ResultExpectation::Internal => false,
        }
    }
}

pub(super) struct PendingRequest<C> {
    pub(super) retry: u32,
    pub(super) sender: ResultExpectation,
    pub(super) cmd: CmdArg<C>,
}

pin_project! {
    pub(super) struct Request<C> {
        pub(super) retry_params: RetryParams,
        pub(super) request: Option<PendingRequest<C>>,
        #[pin]
        pub(super) future: RequestState<BoxFuture<'static, OperationResult>>,
    }
}

fn choose_response<C>(
    result: OperationResult,
    mut request: PendingRequest<C>,
    retry_params: &RetryParams,
) -> (Option<Retry<C>>, PollFlushAction) {
    let (target, err) = match result {
        Ok(item) => {
            trace!("Ok");
            request.sender.send(Ok(item));
            return (None, PollFlushAction::None);
        }
        Err((target, err)) => (target, err),
    };

    let has_retries_remaining = request.retry < retry_params.number_of_retries;

    macro_rules! retry_or_send {
        ($retry_func: expr) => {
            if has_retries_remaining {
                Some($retry_func(request))
            } else {
                let _ = request.sender.send(Err(err));
                None
            }
        };
    }

    request.retry = request.retry.saturating_add(1);

    let sleep_duration = retry_params.wait_time_for_retry(request.retry);

    match (target, err.retry_method()) {
        (_, RetryMethod::ReconnectFromInitialConnections) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.reset_routing();
                Retry::MoveToPending { request }
            });
            (retry, PollFlushAction::ReconnectFromInitialConnections)
        }

        (OperationTarget::Node { address }, RetryMethod::Reconnect) => (
            retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.reset_routing();
                Retry::MoveToPending { request }
            }),
            PollFlushAction::Reconnect(vec![address]),
        ),

        (OperationTarget::FanOut, _) => {
            // Fanout operation are retried per internal request, and don't need additional retries.
            request.sender.send(Err(err));
            (None, PollFlushAction::None)
        }
        (OperationTarget::NotFound, _) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.reset_routing();
                Retry::AfterSleep {
                    request,
                    sleep_duration,
                }
            });
            (retry, PollFlushAction::RebuildSlots)
        }

        (_, RetryMethod::AskRedirect) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.set_redirect(
                    err.redirect_node()
                        .map(|(node, _slot)| Redirect::Ask(node.to_string())),
                );
                Retry::Immediately { request }
            });
            (retry, PollFlushAction::None)
        }

        (_, RetryMethod::MovedRedirect) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.set_redirect(
                    err.redirect_node()
                        .map(|(node, _slot)| Redirect::Moved(node.to_string())),
                );
                Retry::Immediately { request }
            });
            (retry, PollFlushAction::RebuildSlots)
        }

        (_, RetryMethod::WaitAndRetry) => (
            retry_or_send!(|request: PendingRequest<C>| {
                Retry::AfterSleep {
                    sleep_duration,
                    request,
                }
            }),
            PollFlushAction::None,
        ),

        (_, RetryMethod::NoRetry) => {
            request.sender.send(Err(err));
            (None, PollFlushAction::None)
        }

        (_, RetryMethod::RetryImmediately) => (
            retry_or_send!(|request: PendingRequest<C>| { Retry::MoveToPending { request } }),
            PollFlushAction::None,
        ),
    }
}

impl<C> Future for Request<C> {
    type Output = (Option<Retry<C>>, PollFlushAction);

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() || this.request.as_ref().unwrap().sender.is_closed() {
            return Poll::Ready((None, PollFlushAction::None));
        };

        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                ready!(sleep.poll(cx));
                return (
                    Some(Retry::Immediately {
                        // can unwrap, because we tested for `is_none`` earlier in the function
                        request: this.request.take().unwrap(),
                    }),
                    PollFlushAction::None,
                )
                    .into();
            }
        };
        let result = ready!(future.poll(cx));

        // can unwrap, because we tested for `is_none`` earlier in the function
        let request = this.request.take().unwrap();
        Poll::Ready(choose_response(result, request, this.retry_params))
    }
}

impl<C> Request<C> {
    pub(super) fn respond(self: Pin<&mut Self>, msg: RedisResult<Response>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        self.project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::oneshot;

    use crate::{
        cluster_async::{routing, PollFlushAction},
        cluster_client::RetryParams,
        RedisError, RedisResult,
    };

    use super::*;

    fn get_redirect<C>(request: &PendingRequest<C>) -> Option<Redirect> {
        match &request.cmd {
            CmdArg::Cmd { routing, .. } => match routing {
                InternalRoutingInfo::SingleNode(InternalSingleNodeRouting::Redirect {
                    redirect,
                    ..
                }) => Some(redirect.clone()),
                _ => None,
            },
            CmdArg::Pipeline { route, .. } => match route {
                InternalSingleNodeRouting::Redirect { redirect, .. } => Some(redirect.clone()),
                _ => None,
            },
        }
    }

    fn to_err(error: &str) -> RedisError {
        crate::parse_redis_value(error.as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    fn request_and_receiver(
        retry: u32,
    ) -> (
        PendingRequest<usize>,
        oneshot::Receiver<RedisResult<Response>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        (
            PendingRequest::<usize> {
                retry,
                sender: ResultExpectation::External(sender),
                cmd: super::CmdArg::Cmd {
                    cmd: Arc::new(crate::cmd("foo")),
                    routing: routing::InternalSingleNodeRouting::Random.into(),
                },
            },
            receiver,
        )
    }

    const ADDRESS: &str = "foo:1234";

    #[test]
    fn should_redirect_and_retry_on_ask_error_if_retries_remain() {
        let (request, mut receiver) = request_and_receiver(0);
        let err = || to_err(&format!("-ASK 123 {ADDRESS}\r\n"));
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert!(receiver.try_recv().is_err());
        if let Some(super::Retry::Immediately { request, .. }) = retry {
            assert_eq!(
                get_redirect(&request),
                Some(Redirect::Ask(ADDRESS.to_string()))
            );
        } else {
            panic!("Expected retry");
        };
        assert_eq!(next, PollFlushAction::None);

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(err())));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn should_retry_and_refresh_slots_on_move_error_if_retries_remain() {
        let err = || to_err(&format!("-MOVED 123 {ADDRESS}\r\n"));
        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        if let Some(super::Retry::Immediately { request, .. }) = retry {
            assert_eq!(
                get_redirect(&request),
                Some(Redirect::Moved(ADDRESS.to_string()))
            );
        } else {
            panic!("Expected retry");
        };
        assert!(receiver.try_recv().is_err());
        assert_eq!(next, PollFlushAction::RebuildSlots);

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(err())));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::RebuildSlots);
    }

    #[test]
    fn never_retry_on_fanout_operation_target() {
        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((
            OperationTarget::FanOut,
            to_err(&format!("-MOVED 123 {ADDRESS}\r\n")),
        ));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        let expected = to_err(&format!("-MOVED 123 {ADDRESS}\r\n"));
        assert_eq!(receiver.try_recv(), Ok(Err(expected)));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn should_sleep_and_retry_on_not_found_operation_target() {
        let err = || to_err(&format!("-ASK 123 {ADDRESS}\r\n"));

        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((OperationTarget::NotFound, err()));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert!(receiver.try_recv().is_err());
        if let Some(super::Retry::AfterSleep { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        };
        assert_eq!(next, PollFlushAction::RebuildSlots);

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(err())));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn complete_disconnect_should_reconnect_from_initial_nodes_regardless_of_target() {
        let err = || RedisError::from((crate::ErrorKind::ClusterConnectionNotFound, ""));

        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((OperationTarget::NotFound, err()));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert!(receiver.try_recv().is_err());
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        };
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);

        // try the same, with a different target
        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((
            OperationTarget::Node {
                address: ADDRESS.to_string(),
            },
            err(),
        ));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert!(receiver.try_recv().is_err());
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        };
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);

        // and another target
        let (request, mut receiver) = request_and_receiver(0);
        let result = Err((OperationTarget::FanOut, err()));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert!(receiver.try_recv().is_err());
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        };
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);
    }
}
