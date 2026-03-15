use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use labaclaw_worker_plane::{
    build_dedicated_agent_spec, build_deployment_manifest, render_deployment_yaml,
    spawn_failed_event, spawned_event, AgentFactoryValues, SpawnAgentRequested,
    COMMAND_TYPE_SPAWN_AGENT_REQUESTED, EVENT_TYPE_AGENT_SPAWNED, EVENT_TYPE_AGENT_SPAWN_FAILED,
    MESSAGE_TYPE_HEADER,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::fs;
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "agent-factory")]
#[command(
    about = "Consume worker-plane spawn commands and materialize dedicated agent Deployments"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value_t = false)]
        once: bool,
    },
    RenderDeployment {
        #[arg(long)]
        spawn_request: String,
    },
}

#[derive(Debug, Clone)]
struct FactoryRuntimeConfig {
    values: AgentFactoryValues,
    consumer_group: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve { once } => serve(once).await,
        Command::RenderDeployment { spawn_request } => {
            let bytes = fs::read(&spawn_request)
                .with_context(|| format!("Failed to read {spawn_request}"))?;
            let request: SpawnAgentRequested =
                serde_json::from_slice(&bytes).context("Failed to parse spawn request JSON")?;
            let cfg = runtime_config_from_env()?;
            let spec = build_dedicated_agent_spec(&request, &cfg.values);
            print!(
                "{}",
                render_deployment_yaml(
                    &spec,
                    &cfg.values.service_account,
                    &cfg.values.runtime_secret_name
                )?
            );
            Ok(())
        }
    }
}

async fn serve(once: bool) -> Result<()> {
    let cfg = runtime_config_from_env()?;
    let consumer = build_consumer(&cfg)?;
    let producer = build_producer(&cfg)?;
    let kube = Client::try_default()
        .await
        .context("Failed to create Kubernetes client for worker-plane factory")?;
    let deployments: Api<Deployment> = Api::namespaced(kube, &cfg.values.namespace);
    consumer
        .subscribe(&[&cfg.values.command_topic])
        .context("Failed to subscribe to worker-plane command topic")?;

    info!(
        brokers = %cfg.values.redpanda_brokers.join(","),
        topic = %cfg.values.command_topic,
        namespace = %cfg.values.namespace,
        "agent-factory consuming spawn commands"
    );

    loop {
        let message = consumer.recv().await.context("Kafka consume failed")?;
        let commit = handle_message(&cfg, &producer, &deployments, &message).await;
        match commit {
            Ok(processed) => {
                consumer
                    .commit_message(&message, CommitMode::Async)
                    .context("Failed to commit Kafka message")?;
                if once && processed {
                    break;
                }
            }
            Err(error) => {
                error!(error = %error, "agent-factory failed to process message");
                consumer
                    .commit_message(&message, CommitMode::Async)
                    .context("Failed to commit failed Kafka message")?;
                if once {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn handle_message(
    cfg: &FactoryRuntimeConfig,
    producer: &FutureProducer,
    deployments: &Api<Deployment>,
    message: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<bool> {
    let Some(message_type) = header_value(message, MESSAGE_TYPE_HEADER) else {
        warn!("Ignoring Kafka message without labaclaw-message-type header");
        return Ok(false);
    };
    if message_type != COMMAND_TYPE_SPAWN_AGENT_REQUESTED {
        return Ok(false);
    }

    let payload = message
        .payload_view::<str>()
        .transpose()
        .context("Spawn command payload is not valid UTF-8")?
        .ok_or_else(|| anyhow::anyhow!("Spawn command payload is empty"))?;
    let request: SpawnAgentRequested =
        serde_json::from_str(payload).context("Failed to deserialize SpawnAgentRequested")?;

    let spec = build_dedicated_agent_spec(&request, &cfg.values);
    if let Some(expected_worker_image) = request
        .expected_worker_image
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if expected_worker_image != spec.image {
            let event = spawn_failed_event(
                &request.agent_id,
                None,
                format!(
                    "Worker image mismatch for agent {}: orchestrator expected {}, factory resolved {}",
                    request.agent_id, expected_worker_image, spec.image
                ),
            );
            publish_json(
                producer,
                &cfg.values.event_topic,
                EVENT_TYPE_AGENT_SPAWN_FAILED,
                &request.agent_id,
                &event,
            )
            .await?;
            anyhow::bail!(
                "Worker image mismatch for agent {}: expected {}, got {}",
                request.agent_id,
                expected_worker_image,
                spec.image
            );
        }
    }
    let manifest = build_deployment_manifest(
        &spec,
        &cfg.values.service_account,
        &cfg.values.runtime_secret_name,
    );
    let patch_params = PatchParams::apply("labaclaw-worker-plane").force();
    match deployments
        .patch(&spec.metadata.name, &patch_params, &Patch::Apply(&manifest))
        .await
    {
        Ok(_) => {
            info!(
                agent_id = %request.agent_id,
                deployment = %spec.metadata.name,
                "Dedicated agent Deployment applied"
            );
            let event = spawned_event(
                &request.agent_id,
                &cfg.values.namespace,
                &spec.metadata.name,
            );
            publish_json(
                producer,
                &cfg.values.event_topic,
                EVENT_TYPE_AGENT_SPAWNED,
                &request.agent_id,
                &event,
            )
            .await?;
        }
        Err(error) => {
            let event = spawn_failed_event(
                &request.agent_id,
                None,
                format!("Failed to apply dedicated agent Deployment: {error}"),
            );
            publish_json(
                producer,
                &cfg.values.event_topic,
                EVENT_TYPE_AGENT_SPAWN_FAILED,
                &request.agent_id,
                &event,
            )
            .await?;
            return Err(error).context("Kubernetes apply failed for spawned agent");
        }
    }

    Ok(true)
}

fn runtime_config_from_env() -> Result<FactoryRuntimeConfig> {
    let values = AgentFactoryValues {
        worker_image_repository: env_var("LABACLAW_WORKER_IMAGE_REPOSITORY")?,
        worker_image_tag: env_var_with_default("LABACLAW_WORKER_IMAGE_TAG", "latest"),
        namespace: env_var_with_default("LABACLAW_WORKER_PLANE_NAMESPACE", "labaclaw-workers"),
        service_account: env_var_with_default(
            "LABACLAW_WORKER_PLANE_SERVICE_ACCOUNT",
            "labaclaw-worker-plane",
        ),
        runtime_secret_name: env_var_with_default(
            "LABACLAW_WORKER_PLANE_RUNTIME_SECRET_NAME",
            "labaclaw-worker-plane-runtime",
        ),
        redpanda_brokers: env_var("LABACLAW_WORKER_PLANE_REDPANDA_BROKERS")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        command_topic: env_var_with_default(
            "LABACLAW_WORKER_PLANE_COMMAND_TOPIC",
            "agent.command.v1",
        ),
        event_topic: env_var_with_default("LABACLAW_WORKER_PLANE_EVENT_TOPIC", "agent.event.v1"),
        heartbeat_topic: env_var_with_default(
            "LABACLAW_WORKER_PLANE_HEARTBEAT_TOPIC",
            "agent.heartbeat.v1",
        ),
        rustfs_endpoint: env_var("LABACLAW_RUSTFS_ENDPOINT")?,
        rustfs_bucket: env_var_with_default("LABACLAW_RUSTFS_BUCKET", "laba-artifacts"),
        rustfs_prefix: env_var_with_default("LABACLAW_RUSTFS_PREFIX", "labaclaw"),
        rustfs_region: env_var_with_default("LABACLAW_RUSTFS_REGION", "us-east-1"),
    };
    Ok(FactoryRuntimeConfig {
        values,
        consumer_group: env_var_with_default(
            "LABACLAW_WORKER_PLANE_FACTORY_GROUP",
            "labaclaw-worker-plane-factory",
        ),
    })
}

fn build_consumer(cfg: &FactoryRuntimeConfig) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", cfg.values.redpanda_brokers.join(","))
        .set("group.id", &cfg.consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create worker-plane Kafka consumer")
}

fn build_producer(cfg: &FactoryRuntimeConfig) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", cfg.values.redpanda_brokers.join(","))
        .set("message.timeout.ms", "10000")
        .create()
        .context("Failed to create worker-plane Kafka producer")
}

async fn publish_json<T: serde::Serialize>(
    producer: &FutureProducer,
    topic: &str,
    message_type: &str,
    agent_id: &str,
    payload: &T,
) -> Result<()> {
    let serialized =
        serde_json::to_string(payload).context("Failed to serialize worker-plane event payload")?;
    let headers = OwnedHeaders::new().insert(Header {
        key: MESSAGE_TYPE_HEADER,
        value: Some(message_type),
    });
    producer
        .send(
            FutureRecord::to(topic)
                .key(agent_id)
                .payload(&serialized)
                .headers(headers),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(error, _message)| error)
        .with_context(|| format!("Failed to publish {message_type} for agent {agent_id}"))?;
    Ok(())
}

fn header_value(message: &rdkafka::message::BorrowedMessage<'_>, key: &str) -> Option<String> {
    let headers = message.headers()?;
    for index in 0..headers.count() {
        let header = headers.get(index);
        if header.key == key {
            if let Some(value) = header.value {
                if let Ok(value) = std::str::from_utf8(value) {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

fn env_var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("Missing required environment variable {name}"))
}

fn env_var_with_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}
