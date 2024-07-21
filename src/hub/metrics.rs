use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};

lazy_static! {
    pub(crate) static ref TOTAL_CHANNELS: IntGauge =
        register_int_gauge!("total_channels", "Total number of hub channels.").unwrap();
}

lazy_static! {
    pub(crate) static ref TOTAL_CLIENTS: IntGauge =
        register_int_gauge!("total_clients", "Total number of connected clients.").unwrap();
}
