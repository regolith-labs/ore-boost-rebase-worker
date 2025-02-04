mod checkpoint;
mod client;
mod error;
mod lookup_tables;

#[tokio::main]
async fn main() {
    env_logger::init();
    println!("Hello, world!");
}
