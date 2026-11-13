use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use crate::errors::RetryMethod;
use crate::{
    Cmd, ErrorKind, RedisError, RedisResult, Value, cluster_async::OperationTarget,
    cluster_handling::NodeAddress, cluster_handling::client::RetryParams,
    cluster_handling::redirect_address, cluster_routing::Redirect,
};

use futures_util::{
    FutureExt,
    future::{self, BoxFuture},
    ready,
};
use log::{trace, warn};
use pin_project_lite::pin_project;
use tokio::sync::oneshot;

use super::{
    OperationResult, PollFlushAction, Response,
    routing::{InternalRoutingInfo, InternalSingleNodeRouting},
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
    Reconnect(NodeAddress),
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
    /// Some of the commands of the pipeline `request` were rejected, and are sent again on their
    /// own by `retries`. `reply` is the pipeline's reply once they're done.
    Join {
        request: PendingRequest<C>,
        retries: Vec<Self>,
        reply: BoxFuture<'static, RedisResult<Response>>,
    },
}

impl<C> CmdArg<C> {
    fn set_redirect(&mut self, redirect: Option<Redirect>) {
        if let Some(redirect) = redirect {
            match self {
                Self::Reconnect(_) => {}
                Self::Cmd { routing, .. } => match routing {
                    InternalRoutingInfo::SingleNode(route) => {
                        let redirect = InternalSingleNodeRouting::Redirect {
                            redirect,
                            previous_routing: Box::new(std::mem::take(route)),
                        }
                        .into();
                        *routing = redirect;
                    }
                    InternalRoutingInfo::MultiNode(_) => {
                        // Cannot redirect a multi-node request, so just ignore the redirect.
                        warn!("Received a redirect for a multi-node request, ignoring it");
                    }
                },
                Self::Pipeline { route, .. } => {
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
            while let InternalSingleNodeRouting::Redirect {
                previous_routing, ..
            } = route
            {
                *route = std::mem::take(previous_routing.as_mut());
            }

            if let InternalSingleNodeRouting::Connection { identifier, .. } = route {
                *route = InternalSingleNodeRouting::ByAddress(std::mem::take(identifier));
            }
        };
        match self {
            Self::Reconnect(_) => {}
            Self::Cmd { routing, .. } => {
                if let InternalRoutingInfo::SingleNode(route) = routing {
                    fix_route(route);
                }
            }
            Self::Pipeline { route, .. } => {
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
        Join {
            #[pin]
            reply: BoxFuture<'static, RedisResult<Response>>,
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
            Self::External(sender) => sender.send(result),
            Self::Internal => Ok(()),
        };
    }

    pub(super) fn is_closed(&self) -> bool {
        match self {
            Self::External(sender) => sender.is_closed(),
            Self::Internal => false,
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

pub(crate) fn choose_response<C: Clone>(
    result: OperationResult,
    mut request: PendingRequest<C>,
    retry_params: &RetryParams,
) -> (Option<Retry<C>>, PollFlushAction) {
    // Reconnect requests are internal signals - just trigger reconnect, no retry.
    if let CmdArg::Reconnect(addr) = &request.cmd {
        let addr = addr.clone();
        request.sender.send(Ok(Response::Single(crate::Value::Nil)));
        return (
            None,
            PollFlushAction::Reconnect(std::collections::HashSet::from([addr])),
        );
    }

    let (target, result) = result;
    if let (OperationTarget::Node { address }, Ok(Response::Multiple(values))) = (&target, &result)
        && let CmdArg::Pipeline { pipeline, .. } = &request.cmd
        && !pipeline.is_transaction()
        && values.iter().any(Value::is_error_that_requires_action)
        && !values.iter().all(Value::is_error_that_requires_action)
    {
        let Ok(Response::Multiple(values)) = result else {
            unreachable!()
        };
        return retry_rejected_commands(address, values, request, retry_params);
    }

    let err = match result {
        Ok(item) => {
            if let Some(error) = match &item {
                Response::Single(value) if value.is_error_that_requires_action() => {
                    Some(value.clone().extract_error().unwrap_err())
                }
                Response::Multiple(values) => values
                    .iter()
                    .position(|value| value.is_error_that_requires_action())
                    .map(|position| values[position].clone().extract_error().unwrap_err()),
                _ => None,
            } {
                error
            } else {
                trace!("Ok");
                request.sender.send(Ok(item));
                return (None, PollFlushAction::None);
            }
        }
        Err(err) => err,
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
            PollFlushAction::Reconnect(HashSet::from([address])),
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

        (OperationTarget::Node { address }, RetryMethod::AskRedirect) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request.cmd.set_redirect(
                    err.redirect_node().and_then(|(node, _slot)| {
                        redirect_address(node, &address).map(Redirect::Ask)
                    }),
                );
                Retry::Immediately { request }
            });
            (retry, PollFlushAction::None)
        }

        (OperationTarget::Node { address }, RetryMethod::MovedRedirect) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                request
                    .cmd
                    .set_redirect(err.redirect_node().and_then(|(node, _slot)| {
                        redirect_address(node, &address).map(Redirect::Moved)
                    }));
                Retry::Immediately { request }
            });
            (retry, PollFlushAction::RebuildSlots)
        }

        (_, RetryMethod::RefreshSlotsAndRetry) => {
            let retry = retry_or_send!(|mut request: PendingRequest<C>| {
                // No redirect address is available (e.g. READONLY), so re-route by slot
                // against the refreshed topology.
                request.cmd.reset_routing();
                Retry::MoveToPending { request }
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

/// Some of a pipeline's commands were rejected with an error that requires an action, such as
/// ASK while their slot is being migrated, and the others have run, so the pipeline can't be sent
/// again. Instead, each rejected command is handled like a command that was sent on its own and
/// rejected with the same error, with the retries that the pipeline has left, and the pipeline's
/// reply is completed once they're all done.
fn retry_rejected_commands<C: Clone>(
    address: &NodeAddress,
    mut values: Vec<Value>,
    request: PendingRequest<C>,
    retry_params: &RetryParams,
) -> (Option<Retry<C>>, PollFlushAction) {
    let CmdArg::Pipeline {
        pipeline,
        offset,
        route,
        ..
    } = &request.cmd
    else {
        unreachable!("only a pipeline's commands are retried on their own")
    };

    let mut retries = Vec::new();
    let mut receivers = Vec::new();
    let mut poll_flush_action = PollFlushAction::None;
    for (index, cmd) in pipeline.cmd_iter().skip(*offset).enumerate() {
        let Some(value) = values.get_mut(index) else {
            break;
        };
        if !value.is_error_that_requires_action() {
            continue;
        }
        let (sender, receiver) = oneshot::channel();
        let command = PendingRequest {
            retry: request.retry,
            sender: ResultExpectation::External(sender),
            cmd: CmdArg::Cmd {
                cmd: Arc::new(cmd.to_cmd()),
                routing: InternalRoutingInfo::SingleNode(route.clone()),
            },
        };
        let rejection = (
            OperationTarget::Node {
                address: address.clone(),
            },
            Ok(Response::Single(std::mem::replace(value, Value::Nil))),
        );
        let (retry, next) = choose_response(rejection, command, retry_params);
        retries.extend(retry);
        poll_flush_action = poll_flush_action.change_state(next);
        receivers.push((index, receiver));
    }

    let reply = async move {
        // Wait for every command, so that a failed one doesn't drop the others while they wait to
        // be retried, and fail with the first error in the pipeline's order.
        let results = future::join_all(receivers.into_iter().map(|(index, receiver)| {
            receiver.map(move |result| {
                let result = result.unwrap_or_else(|_| {
                    Err(RedisError::from((
                        ErrorKind::Client,
                        "request wasn't handled due to internal failure",
                    )))
                });
                (index, result)
            })
        }))
        .await;
        for (index, result) in results {
            match result? {
                Response::Single(value) => values[index] = value,
                Response::Multiple(_) => unreachable!("a command has a single reply"),
            }
        }
        Ok(Response::Multiple(values))
    }
    .boxed();

    (
        Some(Retry::Join {
            request,
            retries,
            reply,
        }),
        poll_flush_action,
    )
}

impl<C: Clone> Future for Request<C> {
    type Output = (Option<Retry<C>>, PollFlushAction);

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() || this.request.as_ref().unwrap().sender.is_closed() {
            return Poll::Ready((None, PollFlushAction::None));
        }

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
            RequestStateProj::Join { reply } => {
                // The rejected commands were retried on their own, so the reply is final.
                let reply = ready!(reply.poll(cx));
                // can unwrap, because we tested for `is_none`` earlier in the function
                this.request.take().unwrap().sender.send(reply);
                return Poll::Ready((None, PollFlushAction::None));
            }
        };
        let result = ready!(future.poll(cx));

        // can unwrap, because we tested for `is_none`` earlier in the function
        let request = this.request.take().unwrap();
        Poll::Ready(choose_response(result, request, this.retry_params))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::Arc;

    use tokio::sync::oneshot;

    use crate::{
        RedisError, RedisResult,
        cluster_async::{PollFlushAction, routing},
        cluster_handling::NodeAddress,
        cluster_handling::client::RetryParams,
        parse_redis_value,
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
            CmdArg::Reconnect(_) => None,
        }
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

    const ADDRESS: NodeAddress = NodeAddress::from_parts(arcstr::literal!("foo"), 1234);

    fn single_result(val: &str) -> RedisResult<Response> {
        parse_redis_value(val.as_bytes()).map(Response::Single)
    }

    fn to_err(error: &str) -> RedisError {
        crate::parse_redis_value(error.as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }

    #[test]
    fn should_redirect_and_retry_on_ask_error_if_retries_remain() {
        let (request, mut receiver) = request_and_receiver(0);
        let err_string = format!("-ASK 123 {ADDRESS}\r\n");
        let err = || single_result(&err_string);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_matches!(receiver.try_recv(), Err(_));
        if let Some(super::Retry::Immediately { request, .. }) = retry {
            assert_eq!(get_redirect(&request), Some(Redirect::Ask(ADDRESS)));
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::None);

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(to_err(&err_string))));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn should_retry_and_refresh_slots_on_move_error_if_retries_remain() {
        let err_string = format!("-MOVED 123 {ADDRESS}\r\n");
        let err = || single_result(&err_string);
        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        if let Some(super::Retry::Immediately { request, .. }) = retry {
            assert_eq!(get_redirect(&request), Some(Redirect::Moved(ADDRESS)));
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::RebuildSlots);
        assert_matches!(receiver.try_recv(), Err(_));

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(to_err(&err_string))));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::RebuildSlots);
    }

    fn pipeline_request_and_receiver(
        retry: u32,
        pipeline: crate::Pipeline,
    ) -> (
        PendingRequest<usize>,
        oneshot::Receiver<RedisResult<Response>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        let count = pipeline.len();
        (
            PendingRequest::<usize> {
                retry,
                sender: ResultExpectation::External(sender),
                cmd: CmdArg::Pipeline {
                    pipeline: Arc::new(pipeline),
                    offset: 0,
                    count,
                    route: InternalSingleNodeRouting::ByAddress(ADDRESS),
                },
            },
            receiver,
        )
    }

    fn packed_command<C>(request: &PendingRequest<C>) -> Vec<u8> {
        match &request.cmd {
            CmdArg::Cmd { cmd, .. } => cmd.get_packed_command(),
            _ => panic!("Expected a command"),
        }
    }

    const TARGET: NodeAddress = NodeAddress::from_parts(arcstr::literal!("target"), 6380);

    #[test]
    fn should_retry_only_the_rejected_commands_of_a_pipeline() {
        let mut pipeline = crate::pipe();
        pipeline.incr("a", 1).get("b").get("c").get("d");
        let (request, mut receiver) = pipeline_request_and_receiver(0, pipeline);
        let values = vec![
            crate::Value::Int(1),
            parse_redis_value(format!("-ASK 123 {TARGET}\r\n").as_bytes()).unwrap(),
            parse_redis_value(format!("-MOVED 123 {TARGET}\r\n").as_bytes()).unwrap(),
            parse_redis_value(b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n")
                .unwrap(),
        ];
        let result = (
            OperationTarget::Node { address: ADDRESS },
            Ok(Response::Multiple(values)),
        );
        let (retry, next) = choose_response(result, request, &RetryParams::default());

        // Each rejected command gets the action that its error requires.
        assert_eq!(next, PollFlushAction::RebuildSlots);
        let Some(Retry::Join {
            retries, mut reply, ..
        }) = retry
        else {
            panic!("Expected the rejected commands to be retried on their own");
        };
        let mut retries = retries.into_iter();
        let Some(Retry::Immediately { request: ask }) = retries.next() else {
            panic!("Expected the ASK command to be retried immediately");
        };
        let Some(Retry::Immediately { request: moved }) = retries.next() else {
            panic!("Expected the MOVED command to be retried immediately");
        };
        let Some(Retry::AfterSleep {
            request: try_again, ..
        }) = retries.next()
        else {
            panic!("Expected the TRYAGAIN command to be retried after a sleep");
        };
        assert!(retries.next().is_none());
        assert_eq!(
            packed_command(&ask),
            crate::cmd("GET").arg("b").get_packed_command()
        );
        assert_eq!(get_redirect(&ask), Some(Redirect::Ask(TARGET)));
        assert_eq!(
            packed_command(&moved),
            crate::cmd("GET").arg("c").get_packed_command()
        );
        assert_eq!(get_redirect(&moved), Some(Redirect::Moved(TARGET)));
        assert_eq!(
            packed_command(&try_again),
            crate::cmd("GET").arg("d").get_packed_command()
        );
        assert_eq!(get_redirect(&try_again), None);
        assert_eq!((ask.retry, moved.retry, try_again.retry), (1, 1, 1));

        // The pipeline's reply waits for every retried command.
        ask.sender
            .send(Ok(Response::Single(crate::Value::SimpleString("b".into()))));
        try_again
            .sender
            .send(Ok(Response::Single(crate::Value::SimpleString("d".into()))));
        assert!((&mut reply).now_or_never().is_none());
        moved
            .sender
            .send(Ok(Response::Single(crate::Value::SimpleString("c".into()))));
        assert_eq!(
            reply.now_or_never().unwrap(),
            Ok(Response::Multiple(vec![
                crate::Value::Int(1),
                crate::Value::SimpleString("b".into()),
                crate::Value::SimpleString("c".into()),
                crate::Value::SimpleString("d".into()),
            ]))
        );
        assert_matches!(receiver.try_recv(), Err(_));
    }

    #[test]
    fn should_retry_the_rejected_commands_of_a_pipeline_with_its_remaining_retries() {
        let retry_params = RetryParams::default();
        let mut pipeline = crate::pipe();
        pipeline.incr("a", 1).get("b");
        let (request, _receiver) =
            pipeline_request_and_receiver(retry_params.number_of_retries, pipeline);
        let values = vec![
            crate::Value::Int(1),
            parse_redis_value(format!("-ASK 123 {TARGET}\r\n").as_bytes()).unwrap(),
        ];
        let result = (
            OperationTarget::Node { address: ADDRESS },
            Ok(Response::Multiple(values)),
        );
        let (retry, _) = choose_response(result, request, &retry_params);

        // The pipeline has no retries left, so the rejected command fails with its error.
        let Some(Retry::Join { retries, reply, .. }) = retry else {
            panic!("Expected the rejected command to be handled on its own");
        };
        assert!(retries.is_empty());
        assert_eq!(
            reply.now_or_never().unwrap().unwrap_err().code(),
            Some("ASK")
        );
    }

    #[test]
    fn should_retry_a_pipeline_whose_commands_were_all_redirected() {
        let mut pipeline = crate::pipe();
        pipeline.get("a").get("b");
        let (request, mut receiver) = pipeline_request_and_receiver(0, pipeline);
        let moved = parse_redis_value(format!("-MOVED 123 {TARGET}\r\n").as_bytes()).unwrap();
        let result = (
            OperationTarget::Node { address: ADDRESS },
            Ok(Response::Multiple(vec![moved.clone(), moved])),
        );
        let (retry, next) = choose_response(result, request, &RetryParams::default());

        // No command has run, so the whole pipeline is sent again.
        let Some(Retry::Immediately { request }) = retry else {
            panic!("Expected the pipeline to be retried");
        };
        assert!(matches!(request.cmd, CmdArg::Pipeline { .. }));
        assert_eq!(get_redirect(&request), Some(Redirect::Moved(TARGET)));
        assert_eq!(next, PollFlushAction::RebuildSlots);
        assert_matches!(receiver.try_recv(), Err(_));
    }

    #[test]
    fn should_retry_a_transaction_as_a_whole() {
        let mut pipeline = crate::pipe();
        pipeline.atomic().incr("a", 1).get("b");
        let (request, _receiver) = pipeline_request_and_receiver(0, pipeline);
        let values = vec![
            crate::Value::Int(1),
            parse_redis_value(format!("-MOVED 123 {TARGET}\r\n").as_bytes()).unwrap(),
        ];
        let result = (
            OperationTarget::Node { address: ADDRESS },
            Ok(Response::Multiple(values)),
        );
        let (retry, _) = choose_response(result, request, &RetryParams::default());

        let Some(Retry::Immediately { request }) = retry else {
            panic!("Expected the transaction to be retried");
        };
        assert!(matches!(request.cmd, CmdArg::Pipeline { .. }));
    }

    #[test]
    fn should_refresh_slots_and_retry_on_readonly_error_if_retries_remain() {
        let err_string = "-READONLY You can't write against a read only replica.\r\n";
        let err = || single_result(err_string);
        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        // READONLY has no redirect address, so the request keeps its (slot-based) routing
        // and is retried after a sleep once the slot map has been rebuilt.
        if let Some(super::Retry::MoveToPending { request }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected a move-to-pending");
        }
        assert_eq!(next, PollFlushAction::RebuildSlots);
        assert_matches!(receiver.try_recv(), Err(_));

        // Once retries are exhausted, the error is surfaced (still rebuilding slots).
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(to_err(err_string))));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::RebuildSlots);
    }

    #[test]
    fn never_retry_on_fanout_operation_target() {
        let (request, mut receiver) = request_and_receiver(0);
        let err_string = format!("-MOVED 123 {ADDRESS}\r\n");
        let result = (OperationTarget::FanOut, single_result(&err_string));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(to_err(&err_string))));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn should_sleep_and_retry_on_not_found_operation_target() {
        let err_string = format!("-ASK 123 {ADDRESS}\r\n");
        let err = || single_result(&err_string);

        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::NotFound, err());
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_matches!(receiver.try_recv(), Err(_));
        if let Some(super::Retry::AfterSleep { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::RebuildSlots);

        // try the same, without remaining retries
        let (request, mut receiver) = request_and_receiver(retry_params.number_of_retries);
        let result = (OperationTarget::Node { address: ADDRESS }, err());
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_eq!(receiver.try_recv(), Ok(Err(to_err(&err_string))));
        assert!(retry.is_none());
        assert_eq!(next, PollFlushAction::None);
    }

    #[test]
    fn complete_disconnect_should_reconnect_from_initial_nodes_regardless_of_target() {
        let err = || RedisError::from((crate::ErrorKind::ClusterConnectionNotFound, ""));

        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::NotFound, Err(err()));
        let retry_params = RetryParams::default();
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_matches!(receiver.try_recv(), Err(_));
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);

        // try the same, with a different target
        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::Node { address: ADDRESS }, Err(err()));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_matches!(receiver.try_recv(), Err(_));
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);

        // and another target
        let (request, mut receiver) = request_and_receiver(0);
        let result = (OperationTarget::FanOut, Err(err()));
        let (retry, next) = choose_response(result, request, &retry_params);

        assert_matches!(receiver.try_recv(), Err(_));
        if let Some(super::Retry::MoveToPending { request, .. }) = retry {
            assert!(get_redirect(&request).is_none());
        } else {
            panic!("Expected retry");
        }
        assert_eq!(next, PollFlushAction::ReconnectFromInitialConnections);
    }
}
