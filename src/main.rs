mod checkpoint;
mod client;
mod error;

#[tokio::main]
async fn main() {
    env_logger::init();
    println!("Hello, world!");
}
