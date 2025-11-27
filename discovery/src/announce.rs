use crate::{ALPN_ID, FULLNAME_SUFFIX, SERVICE_TYPE, iface::collect_non_loopback};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::time::{SystemTime, UNIX_EPOCH};

/// 局域网组播发布服务
pub struct AnnounceDaemon {
    instance_name: Box<str>,
    daemon: ServiceDaemon,
}

/// 不要相信你的主机名
/// 将主机名净化成实例名称
fn sanitize_hostname_to_instancename(name: &str) -> Box<str> {
    let mut buf = String::with_capacity(name.len());
    for c in name.chars() {
        match c {
            '.' => buf.push_str(r"\."),
            '\\' => buf.push_str(r"\\"),
            _ if c.is_control() => {}
            _ => buf.push(c),
        }
    }
    const MAX_INSTANCE_NAME_LEN: usize = 63;
    if buf.len() > MAX_INSTANCE_NAME_LEN {
        let boundary = buf
            .char_indices()
            .find(|(idx, c)| idx + c.len_utf8() > MAX_INSTANCE_NAME_LEN)
            .map(|(idx, _)| idx)
            .unwrap_or(MAX_INSTANCE_NAME_LEN);
        buf.truncate(boundary);
    }
    buf.into_boxed_str()
}

/// 产生一个伪随机字符串
#[inline]
fn timestamp_based_str() -> Box<str> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_nanos();
    format!("{:X}", nanos).into_boxed_str()
}

impl AnnounceDaemon {
    /// 创建守护进程
    pub fn new() -> Self {
        let hostname = hostname::get()
            .ok()
            .and_then(|os_str| os_str.into_string().map(|s| s.into_boxed_str()).ok())
            .unwrap_or(timestamp_based_str());
        let instance_name = sanitize_hostname_to_instancename(&hostname);
        let daemon = ServiceDaemon::new().expect("failed to create daemon");
        // daemon.set_multicast_loop_v4(false).expect("failed to disable multicast loop v4");
        // daemon.set_multicast_loop_v6(false).expect("failed to disable multicast v6");
        Self { instance_name, daemon }
    }

    /// 注册服务
    pub fn register(&self) -> mdns_sd::Result<()> {
        let addrs = collect_non_loopback().expect("local addr is empty");
        let dns_hostname = format!("{}.local.", self.instance_name);
        let porp = [("alpn", ALPN_ID)];
        let srv_info = ServiceInfo::new(
            SERVICE_TYPE,
            &self.instance_name,
            dns_hostname.as_ref(),
            addrs.as_slice(),
            0,
            porp.as_slice(),
        )?;
        self.daemon.register(srv_info)?;
        Ok(())
    }
}

impl Drop for AnnounceDaemon {
    fn drop(&mut self) {
        let mut fullname = self.instance_name.to_string();
        fullname.push_str(FULLNAME_SUFFIX);
        self.daemon.unregister(&fullname).expect("failed to unregister");
        self.daemon.shutdown().expect("failed to shutdown");
    }
}
