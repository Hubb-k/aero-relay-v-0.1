use std::sync::Arc;
use tokio::time::{sleep, Duration};
use crate::transport::BufferTask;

pub async fn run(input: Arc<BufferTask>, output: Arc<BufferTask>) {
    let mut rp = input.get_current_head();

    loop {
        let (data, next_rp): (Vec<u8>, usize) = input.pull_from(rp);

        if !data.is_empty() {
            print!("{}", String::from_utf8_lossy(&data));
            output.push(&data);
            rp = next_rp;
        } else {
            sleep(Duration::from_micros(500)).await;
        }
    }
}
