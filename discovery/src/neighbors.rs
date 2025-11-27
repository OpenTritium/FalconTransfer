use crate::listen::InstanceName;
use dashmap::DashMap;
use mdns_sd::ScopedIp;
use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

type ScopeId = u32;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ScopedAddr {
    V4(Ipv4Addr),
    V6 { scope_id: ScopeId, addr: Ipv6Addr },
}

impl ScopedAddr {
    #[inline]
    pub fn ip(&self) -> IpAddr {
        use ScopedAddr::*;
        match *self {
            V4(addr) => IpAddr::V4(addr),
            V6 { addr, .. } => IpAddr::V6(addr),
        }
    }

    #[inline]
    pub fn scope_id(&self) -> Option<ScopeId> {
        use ScopedAddr::*;
        match *self {
            V6 { scope_id, .. } => Some(scope_id),
            _ => None,
        }
    }

    pub fn is_ipv4(&self) -> bool { matches!(*self, ScopedAddr::V4(_)) }

    pub fn is_ipv6(&self) -> bool { matches!(*self, ScopedAddr::V6 { .. }) }
}

impl From<Ipv4Addr> for ScopedAddr {
    #[inline]
    fn from(addr: Ipv4Addr) -> Self { ScopedAddr::V4(addr) }
}

impl From<(Ipv6Addr, ScopeId)> for ScopedAddr {
    #[inline]
    fn from((addr, scope_id): (Ipv6Addr, ScopeId)) -> Self { ScopedAddr::V6 { scope_id, addr } }
}

impl From<ScopedIp> for ScopedAddr {
    #[inline]
    fn from(src: ScopedIp) -> Self {
        match src {
            ScopedIp::V4(addr) => ScopedAddr::from(*addr.addr()),
            ScopedIp::V6(addr) => ScopedAddr::from((*addr.addr(), addr.scope_id().index)),
            _ => panic!("unsupported scoped ip"),
        }
    }
}

impl fmt::Display for ScopedAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ScopedAddr::*;
        match self {
            V4(addr) => write!(f, "{}", addr),
            V6 { addr, scope_id } => {
                if *scope_id == 0 {
                    write!(f, "{}", addr)
                } else {
                    write!(f, "{}%{}", addr, scope_id)
                }
            }
        }
    }
}

pub type NeighborTable = DashMap<InstanceName, Vec<ScopedAddr>>;
