use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};

lazy_static! {
    pub(crate) static ref TOTAL_CHANNELS: IntGauge =
        register_int_gauge!("total_channels", "Total number of hub channels.").unwrap();
}

lazy_static! {
    pub(crate) static ref WS_TOTAL_CLIENTS: IntGauge =
        register_int_gauge!("ws_total_clients", "Total number of connected websocket clients.").unwrap();
}

lazy_static! {
    pub(crate) static ref SSE_TOTAL_CLIENTS: IntGauge =
        register_int_gauge!("sse_total_clients", "Total number of connected sse clients.").unwrap();
}
