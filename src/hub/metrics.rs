use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};

lazy_static! {
    pub static ref TOTAL_CHANNELS: Gauge =
        register_gauge!("total_hub_channels", "Total number of channels.",)
            .expect("cannot create gauge metric for total number of channels");
}

lazy_static! {
    pub static ref TOTAL_CLIENTS: Gauge =
        register_gauge!("total_clients", "Total number of connected clients.",)
            .expect("cannot create gauge metric for total number of connected clients");
}
