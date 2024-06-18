use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use futures::{future::BoxFuture, ready, Future};
use log::trace;
use pin_project_lite::pin_project;
use tokio::sync::oneshot;

use crate::{
    cluster_async::{boxed_sleep, OperationTarget},
    cluster_client::RetryParams,
    cluster_routing::Redirect,
    Cmd, ErrorKind, RedisResult,
};

use super::{
    routing::{InternalRoutingInfo, InternalSingleNodeRouting},
    Next, OperationResult, Response,
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
        None,
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

pub(super) struct PendingRequest<C> {
    pub(super) retry: u32,
    pub(super) sender: oneshot::Sender<RedisResult<Response>>,
    pub(super) cmd: CmdArg<C>,
}

pin_project! {
    pub(super)  struct Request<C> {
         pub(super)retry_params: RetryParams,
         pub(super)request: Option<PendingRequest<C>>,
        #[pin]
         pub(super)future: RequestState<BoxFuture<'static, OperationResult>>,
    }
}

impl<C> Future for Request<C> {
    type Output = Next<C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() {
            return Poll::Ready(Next::Done);
        }
        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                ready!(sleep.poll(cx));
                return Next::Retry {
                    request: self.project().request.take().unwrap(),
                }
                .into();
            }
            _ => panic!("Request future must be Some"),
        };
        match ready!(future.poll(cx)) {
            Ok(item) => {
                trace!("Ok");
                self.respond(Ok(item));
                Next::Done.into()
            }
            Err((target, err)) => {
                trace!("Request error {}", err);

                let request = this.request.as_mut().unwrap();
                if request.retry >= this.retry_params.number_of_retries {
                    self.respond(Err(err));
                    return Next::Done.into();
                }
                request.retry = request.retry.saturating_add(1);

                if err.kind() == ErrorKind::ClusterConnectionNotFound {
                    return Next::ReconnectToInitialNodes {
                        request: this.request.take().unwrap(),
                    }
                    .into();
                }

                let sleep_duration = this.retry_params.wait_time_for_retry(request.retry);

                let address = match target {
                    OperationTarget::Node { address } => address,
                    OperationTarget::FanOut => {
                        // Fanout operation are retried per internal request, and don't need additional retries.
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                    OperationTarget::NotFound => {
                        // TODO - this is essentially a repeat of the retriable error. probably can remove duplication.
                        let mut request = this.request.take().unwrap();
                        request.cmd.reset_routing();
                        return Next::RefreshSlots {
                            request,
                            sleep_duration: Some(sleep_duration),
                        }
                        .into();
                    }
                };

                match err.retry_method() {
                    crate::types::RetryMethod::AskRedirect => {
                        let mut request = this.request.take().unwrap();
                        request.cmd.set_redirect(
                            err.redirect_node()
                                .map(|(node, _slot)| Redirect::Ask(node.to_string())),
                        );
                        Next::Retry { request }.into()
                    }
                    crate::types::RetryMethod::MovedRedirect => {
                        let mut request = this.request.take().unwrap();
                        request.cmd.set_redirect(
                            err.redirect_node()
                                .map(|(node, _slot)| Redirect::Moved(node.to_string())),
                        );
                        Next::RefreshSlots {
                            request,
                            sleep_duration: None,
                        }
                        .into()
                    }
                    crate::types::RetryMethod::WaitAndRetry => {
                        // Sleep and retry.
                        this.future.set(RequestState::Sleep {
                            sleep: boxed_sleep(sleep_duration),
                        });
                        self.poll(cx)
                    }
                    crate::types::RetryMethod::Reconnect => {
                        let mut request = this.request.take().unwrap();
                        // TODO should we reset the redirect here?
                        request.cmd.reset_routing();
                        Next::Reconnect {
                            request,
                            target: address,
                        }
                    }
                    .into(),
                    crate::types::RetryMethod::RetryImmediately => Next::Retry {
                        request: this.request.take().unwrap(),
                    }
                    .into(),
                    crate::types::RetryMethod::NoRetry => {
                        self.respond(Err(err));
                        Next::Done.into()
                    }
                }
            }
        }
    }
}

impl<C> Request<C> {
    pub(super) fn respond(self: Pin<&mut Self>, msg: RedisResult<Response>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}
