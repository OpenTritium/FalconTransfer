use crate::{FULLNAME_SUFFIX, SERVICE_TYPE};
use futures_util::StreamExt;
use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent};
use std::{
    collections::{HashMap, hash_map::Entry},
    time::Duration,
};
use tokio::{select, spawn, sync::mpsc, task::AbortHandle};
use tokio_util::time::{DelayQueue, delay_queue};

type InstanceName = Box<str>;

// todo 禁用本地回环

// fullname: <instance>.<service>.<domain>

fn parse_instance_from_fullname(fullname: &str) -> Option<&str> { fullname.strip_suffix(FULLNAME_SUFFIX) }

struct ResolvedCache {
    entries: HashMap<InstanceName, delay_queue::Key>,
    expirations: DelayQueue<ResolvedService>,
}

impl ResolvedCache {
    const TIMEOUT: Duration = Duration::from_secs(3);

    fn new() -> Self { Self { entries: HashMap::new(), expirations: DelayQueue::new() } }

    fn update(&mut self, instance: InstanceName, srv: ResolvedService) -> delay_queue::Key {
        match self.entries.entry(instance) {
            Entry::Occupied(mut occ) => {
                let exp_key = occ.get_mut();
                self.expirations.remove(exp_key);
                let new_key = self.expirations.insert(srv, Self::TIMEOUT);
                *exp_key = new_key;
                new_key
            }
            Entry::Vacant(vac) => {
                let new_key = self.expirations.insert(srv, Self::TIMEOUT);
                vac.insert(new_key);
                new_key
            }
        }
    }

    // 用于收到取消注册服务时移除缓存
    fn remove(&mut self, instance: InstanceName) -> Option<ResolvedService> {
        self.entries.remove(&instance).map(|key| self.expirations.remove(&key).into_inner())
    }

    async fn next(&mut self) -> Option<ResolvedService> {
        self.expirations.next().await.map(|srv| {
            let exp_key = srv.key();
            let instance = self.entries.iter().find(|&(_, key)| key == &exp_key).unwrap().0.clone();
            self.entries.remove(&instance);
            srv.into_inner()
        })
    }
}

pub struct DebouncedClient(AbortHandle);

impl DebouncedClient {
    pub(crate) fn run() -> (Self, mpsc::Receiver<ResolvedService>) {
        let (tx, rx) = mpsc::channel(8);
        let abort = spawn(async move {
            let daemon = ServiceDaemon::new().unwrap();
            daemon.set_multicast_loop_v4(false).expect("failed to disable multicast loop v4");
            daemon.set_multicast_loop_v6(false).expect("failed to disable multicast v6");
            let srv_rx = daemon.browse(SERVICE_TYPE).unwrap();
            let mut cache = ResolvedCache::new();
            loop {
                select! {
                    Some(srv) = cache.next() => {
                        tx.send(srv).await.expect("failed to send debounced local link");
                    }
                    Ok(ServiceEvent::ServiceResolved(srv)) = srv_rx.recv_async() => {
                        if let Some(instance) = parse_instance_from_fullname(srv.get_fullname()) {
                            cache.update(instance.into(), *srv);
                        }
                    }
                    Ok(ServiceEvent::ServiceRemoved(_,fullname)) = srv_rx.recv_async() => {
                         parse_instance_from_fullname(&fullname).and_then(|instance| cache.remove(instance.into()));
                    }
                    else => { }
                }
            }
        })
        .abort_handle();
        (Self(abort), rx)
    }
}

impl Drop for DebouncedClient {
    fn drop(&mut self) { self.0.abort(); }
}
