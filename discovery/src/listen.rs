use crate::{FULLNAME_SUFFIX, SERVICE_TYPE};
use futures_util::{Stream, StreamExt};
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent};

pub type InstanceName = Box<str>;

// fullname: <instance>.<service>.<domain>
pub fn parse_instance_from_fullname(fullname: &str) -> Option<&str> { fullname.strip_suffix(FULLNAME_SUFFIX) }

pub enum ServiceOpt {
    Register(Box<ResolvedService>),
    Unregister(Box<str>),
}
pub fn listen() -> impl Stream<Item = ServiceOpt> {
    use ServiceEvent::*;
    use ServiceOpt::*;
    let daemon = ServiceDaemon::new().unwrap();
    // daemon.set_multicast_loop_v4(false).expect("failed to disable multicast loop v4");
    // daemon.set_multicast_loop_v6(false).expect("failed to disable multicast v6");
    daemon.browse(SERVICE_TYPE).expect("browse server info failed").into_stream().filter_map(async |srv| match srv {
        ServiceResolved(srv) => Some(Register(srv)),
        ServiceRemoved(_, fullname) => Some(Unregister(parse_instance_from_fullname(&fullname).unwrap().into())),
        _ => None,
    })
}
