use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
use std::{net::IpAddr, ops::Not};

pub(crate) fn collect_non_loopback() -> network_interface::Result<Vec<IpAddr>> {
    let iface_chunk = NetworkInterface::show()?;
    Ok(iface_chunk
        .into_iter()
        .flat_map(|x| x.addr)
        .filter_map(|addr| match addr {
            Addr::V4(addr) => {
                let ip = addr.ip;
                ip.is_loopback().not().then_some(ip.into())
            }
            Addr::V6(addr) => {
                let ip = addr.ip;
                ip.is_loopback().not().then_some(ip.into())
            }
        })
        .collect::<Vec<IpAddr>>())
}
