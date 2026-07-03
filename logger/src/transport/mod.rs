pub mod buffer;
pub mod inbound;
pub mod outbound;

pub use buffer::BufferTask;
use std::sync::Arc;

pub async fn init(sources: Vec<String>, listen: String) -> (Arc<BufferTask>, Arc<BufferTask>) {
    let input_buf = Arc::new(BufferTask::new());
    let output_buf = Arc::new(BufferTask::new());

    input_buf.reset();
    output_buf.reset();

    let b_in = input_buf.clone();
    tokio::spawn(async move {
        inbound::start_inbound(sources, b_in).await;
    });

    let b_out = output_buf.clone();
    tokio::spawn(async move {
        outbound::run_outbound_worker(listen, b_out).await;
    });

    (input_buf, output_buf)
}
