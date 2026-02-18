#[cfg(test)]
pub fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

#[cfg(test)]
pub fn drop_runtime(rt: tokio::runtime::Runtime) {
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
