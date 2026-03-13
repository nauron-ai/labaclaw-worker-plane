use anyhow::{Context, Result};
use clap::Parser;
use labaclaw_worker_plane::MESSAGE_TYPE_HEADER;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "topic-dump")]
#[command(about = "Debug utility to dump worker-plane Kafka records from a topic")]
struct Cli {
    #[arg(long)]
    brokers: String,
    #[arg(long)]
    topic: String,
    #[arg(long, default_value = "debug-topic-dump")]
    group_id: String,
    #[arg(long, default_value_t = 10)]
    num: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cli.brokers)
        .set("group.id", &cli.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to build Kafka consumer")?;
    consumer
        .subscribe(&[&cli.topic])
        .context("Failed to subscribe to topic")?;

    for _ in 0..cli.num {
        let message = consumer.recv().await.context("Kafka consume failed")?;
        let payload = message
            .payload_view::<str>()
            .transpose()
            .context("Message payload is not valid UTF-8")?
            .unwrap_or("");
        let key = message
            .key_view::<str>()
            .transpose()
            .context("Message key is not valid UTF-8")?
            .unwrap_or("");
        let message_type = message
            .headers()
            .and_then(|headers| {
                for index in 0..headers.count() {
                    let header = headers.get(index);
                    if header.key == MESSAGE_TYPE_HEADER {
                        if let Some(value) = header.value {
                            if let Ok(value) = std::str::from_utf8(value) {
                                return Some(value.to_string());
                            }
                        }
                    }
                }
                None
            })
            .unwrap_or_else(|| "unknown".into());
        println!("{key}\t{message_type}\t{payload}");
    }

    Ok(())
}
