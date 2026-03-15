use anyhow::{Context, Result};
use clap::Parser;
use labaclaw_worker_plane::{
    completed_event, execute_bootstrap_request, heartbeat_event, parse_s3_ref, progress_event,
    question_event, spawn_failed_event, suspended_event, terminated_event, AgentCompleted,
    ResumeAgentRequested, SpawnedBootstrapRequest, SuspendAgentRequested, TaskAssigned,
    TerminateAgentRequested, WorkerArtifactReference, WorkerResultSidecar,
    COMMAND_TYPE_RESUME_AGENT_REQUESTED, COMMAND_TYPE_SPAWN_AGENT_REQUESTED,
    COMMAND_TYPE_SUSPEND_AGENT_REQUESTED, COMMAND_TYPE_TASK_ASSIGNED,
    COMMAND_TYPE_TERMINATE_AGENT_REQUESTED, EVENT_TYPE_AGENT_COMPLETED, EVENT_TYPE_AGENT_HEARTBEAT,
    EVENT_TYPE_AGENT_PROGRESS_REPORTED, EVENT_TYPE_AGENT_QUESTION_RAISED,
    EVENT_TYPE_AGENT_SPAWN_FAILED, EVENT_TYPE_AGENT_SUSPENDED, EVENT_TYPE_AGENT_TERMINATED,
    MESSAGE_TYPE_HEADER,
};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "agent-runner")]
#[command(about = "Per-agent worker runtime for dedicated LabaClaw Deployments")]
struct Cli {
    #[arg(long, default_value_t = false)]
    once: bool,
}

#[derive(Debug, Clone)]
struct RunnerConfig {
    agent_id: String,
    spec_ref: String,
    bootstrap_ref: String,
    redpanda_brokers: Vec<String>,
    command_topic: String,
    event_topic: String,
    heartbeat_topic: String,
    rustfs_endpoint: String,
    rustfs_bucket: String,
    rustfs_region: String,
    rustfs_access_key: Option<String>,
    rustfs_secret_key: Option<String>,
    workload_name: String,
    consumer_group: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let cfg = runner_config_from_env()?;
    let producer = build_producer(&cfg)?;
    let store = build_artifact_store(&cfg)?;

    publish_json(
        &producer,
        &cfg.heartbeat_topic,
        EVENT_TYPE_AGENT_HEARTBEAT,
        &cfg.agent_id,
        &heartbeat_event(&cfg.agent_id, &cfg.workload_name, "starting"),
    )
    .await?;

    if let Err(error) = run_initial_bootstrap(&cfg, &producer, store.as_ref()).await {
        error!(error = %error, "Bootstrap mission failed");
        let event = spawn_failed_event(&cfg.agent_id, None, error.to_string());
        publish_json(
            &producer,
            &cfg.event_topic,
            EVENT_TYPE_AGENT_SPAWN_FAILED,
            &cfg.agent_id,
            &event,
        )
        .await?;
        return Err(error);
    }

    publish_json(
        &producer,
        &cfg.heartbeat_topic,
        EVENT_TYPE_AGENT_HEARTBEAT,
        &cfg.agent_id,
        &heartbeat_event(&cfg.agent_id, &cfg.workload_name, "idle"),
    )
    .await?;

    if cli.once {
        return Ok(());
    }

    run_service_loop(&cfg, &producer, store.as_ref()).await
}

async fn run_initial_bootstrap(
    cfg: &RunnerConfig,
    producer: &FutureProducer,
    store: &dyn ObjectStore,
) -> Result<()> {
    let _spec_bytes = download_bytes(store, &cfg.spec_ref).await?;
    publish_json(
        producer,
        &cfg.event_topic,
        EVENT_TYPE_AGENT_PROGRESS_REPORTED,
        &cfg.agent_id,
        &progress_event(
            &cfg.agent_id,
            None,
            "bootstrap_spec_loaded",
            format!("Fetched AgentSpec from {}", cfg.spec_ref),
        ),
    )
    .await?;

    let bootstrap_bytes = download_bytes(store, &cfg.bootstrap_ref).await?;
    let request: SpawnedBootstrapRequest = serde_json::from_slice(&bootstrap_bytes)
        .context("Failed to deserialize bootstrap request")?;
    publish_json(
        producer,
        &cfg.event_topic,
        EVENT_TYPE_AGENT_PROGRESS_REPORTED,
        &cfg.agent_id,
        &progress_event(
            &cfg.agent_id,
            Some(request.request_id.clone()),
            "bootstrap_running",
            "Executing bootstrap mission",
        ),
    )
    .await?;

    execute_request(cfg, producer, store, &request).await
}

async fn run_service_loop(
    cfg: &RunnerConfig,
    producer: &FutureProducer,
    store: &dyn ObjectStore,
) -> Result<()> {
    let consumer = build_consumer(cfg)?;
    consumer
        .subscribe(&[&cfg.command_topic])
        .context("Failed to subscribe agent-runner to command topic")?;

    let mut suspended = false;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(15)) => {
                let service_state = if suspended { "suspended" } else { "idle" };
                publish_json(
                    producer,
                    &cfg.heartbeat_topic,
                    EVENT_TYPE_AGENT_HEARTBEAT,
                    &cfg.agent_id,
                    &heartbeat_event(&cfg.agent_id, &cfg.workload_name, service_state),
                ).await?;
            }
            message = consumer.recv() => {
                let message = message.context("Kafka consume failed in agent-runner")?;
                let should_exit = handle_command_message(cfg, producer, store, &message, &mut suspended).await?;
                consumer.commit_message(&message, CommitMode::Async)
                    .context("Failed to commit agent-runner command message")?;
                if should_exit {
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn handle_command_message(
    cfg: &RunnerConfig,
    producer: &FutureProducer,
    store: &dyn ObjectStore,
    message: &rdkafka::message::BorrowedMessage<'_>,
    suspended: &mut bool,
) -> Result<bool> {
    let Some(message_type) = header_value(message, MESSAGE_TYPE_HEADER) else {
        return Ok(false);
    };
    let payload = message
        .payload_view::<str>()
        .transpose()
        .context("Worker command payload is not valid UTF-8")?
        .ok_or_else(|| anyhow::anyhow!("Worker command payload is empty"))?;

    match message_type.as_str() {
        COMMAND_TYPE_SPAWN_AGENT_REQUESTED => Ok(false),
        COMMAND_TYPE_TASK_ASSIGNED => {
            if *suspended {
                warn!(agent_id = %cfg.agent_id, "Ignoring TaskAssigned while runner is suspended");
                return Ok(false);
            }
            let request: TaskAssigned =
                serde_json::from_str(payload).context("Failed to deserialize TaskAssigned")?;
            if request.agent_id != cfg.agent_id {
                return Ok(false);
            }
            let task_bytes = download_bytes(store, &request.task_ref).await?;
            let bootstrap: SpawnedBootstrapRequest = serde_json::from_slice(&task_bytes)
                .context("Failed to deserialize task payload as SpawnedBootstrapRequest")?;
            execute_request(cfg, producer, store, &bootstrap).await?;
            Ok(false)
        }
        COMMAND_TYPE_SUSPEND_AGENT_REQUESTED => {
            let request: SuspendAgentRequested = serde_json::from_str(payload)
                .context("Failed to deserialize SuspendAgentRequested")?;
            if request.agent_id != cfg.agent_id {
                return Ok(false);
            }
            *suspended = true;
            publish_json(
                producer,
                &cfg.event_topic,
                EVENT_TYPE_AGENT_SUSPENDED,
                &cfg.agent_id,
                &suspended_event(&cfg.agent_id, request.reason.clone()),
            )
            .await?;
            Ok(false)
        }
        COMMAND_TYPE_RESUME_AGENT_REQUESTED => {
            let request: ResumeAgentRequested = serde_json::from_str(payload)
                .context("Failed to deserialize ResumeAgentRequested")?;
            if request.agent_id != cfg.agent_id {
                return Ok(false);
            }
            *suspended = false;
            publish_json(
                producer,
                &cfg.event_topic,
                EVENT_TYPE_AGENT_PROGRESS_REPORTED,
                &cfg.agent_id,
                &progress_event(
                    &cfg.agent_id,
                    None,
                    "resumed",
                    "Dedicated agent resumed by orchestrator",
                ),
            )
            .await?;
            Ok(false)
        }
        COMMAND_TYPE_TERMINATE_AGENT_REQUESTED => {
            let request: TerminateAgentRequested = serde_json::from_str(payload)
                .context("Failed to deserialize TerminateAgentRequested")?;
            if request.agent_id != cfg.agent_id {
                return Ok(false);
            }
            publish_json(
                producer,
                &cfg.event_topic,
                EVENT_TYPE_AGENT_TERMINATED,
                &cfg.agent_id,
                &terminated_event(&cfg.agent_id, request.reason),
            )
            .await?;
            Ok(true)
        }
        other => {
            warn!(message_type = %other, "Ignoring unsupported worker command type");
            Ok(false)
        }
    }
}

async fn execute_request(
    cfg: &RunnerConfig,
    producer: &FutureProducer,
    store: &dyn ObjectStore,
    request: &SpawnedBootstrapRequest,
) -> Result<()> {
    let outcome = execute_bootstrap_request(request);
    if let Some(error) = outcome.fatal_error.clone() {
        let event = spawn_failed_event(&cfg.agent_id, Some(request.request_id.clone()), error);
        publish_json(
            producer,
            &cfg.event_topic,
            EVENT_TYPE_AGENT_SPAWN_FAILED,
            &cfg.agent_id,
            &event,
        )
        .await?;
        return Ok(());
    }

    if let Some(question_summary) = outcome.question_summary.clone() {
        let question_ref = if let Some(question_markdown) = outcome.question_markdown.as_deref() {
            let question_ref =
                derive_question_ref(&cfg.bootstrap_ref, &cfg.agent_id, &request.request_id)?;
            upload_bytes(store, &question_ref, question_markdown.as_bytes()).await?;
            Some(question_ref)
        } else {
            None
        };
        publish_json(
            producer,
            &cfg.event_topic,
            EVENT_TYPE_AGENT_QUESTION_RAISED,
            &cfg.agent_id,
            &question_event(
                &cfg.agent_id,
                &request.request_id,
                question_ref,
                Some(question_summary),
            ),
        )
        .await?;
    }

    if let Some(mut result_markdown) = outcome.result_markdown {
        let artifact_refs = upload_generated_artifacts(
            store,
            &cfg.bootstrap_ref,
            &cfg.agent_id,
            &request.request_id,
            &outcome.artifacts,
        )
        .await?;
        if !artifact_refs.is_empty() {
            result_markdown.push_str("\n\nArtifacts:\n");
            for (name, reference) in &artifact_refs {
                result_markdown.push_str(&format!("- {name}: {reference}\n"));
            }
        }
        let result_ref = derive_result_ref(&cfg.bootstrap_ref, &cfg.agent_id, &request.request_id)?;
        upload_bytes(store, &result_ref, result_markdown.as_bytes()).await?;
        let result_json_ref =
            derive_result_json_ref(&cfg.bootstrap_ref, &cfg.agent_id, &request.request_id)?;
        let sidecar = WorkerResultSidecar {
            status: outcome.status.clone(),
            summary: outcome
                .summary
                .clone()
                .unwrap_or_else(|| "Dedicated worker completed the request".into()),
            questions: outcome.questions.clone(),
            confidence: outcome.confidence,
            validation_status: outcome.validation_status.clone(),
            artifacts: artifact_refs
                .iter()
                .map(|(name, reference)| WorkerArtifactReference {
                    name: name.clone(),
                    reference: reference.clone(),
                })
                .collect(),
            direct_write_recommendation: outcome.direct_write_recommendation,
        };
        let sidecar_bytes = serde_json::to_vec_pretty(&sidecar)
            .context("Failed to serialize worker result sidecar")?;
        upload_bytes(store, &result_json_ref, &sidecar_bytes).await?;
        let completion: AgentCompleted = completed_event(
            &cfg.agent_id,
            &request.request_id,
            result_ref,
            Some(result_json_ref),
            outcome
                .summary
                .unwrap_or_else(|| "Dedicated worker completed the request".into()),
        );
        publish_json(
            producer,
            &cfg.event_topic,
            EVENT_TYPE_AGENT_COMPLETED,
            &cfg.agent_id,
            &completion,
        )
        .await?;
    }

    Ok(())
}

fn runner_config_from_env() -> Result<RunnerConfig> {
    let redpanda_brokers = env_var("LABACLAW_WORKER_PLANE_REDPANDA_BROKERS")
        .or_else(|_| env_var("LABACLAW_REDPANDA_BROKERS"))?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    Ok(RunnerConfig {
        agent_id: env_var("LABACLAW_AGENT_ID")?,
        spec_ref: env_var("LABACLAW_AGENT_SPEC_REF")?,
        bootstrap_ref: env_var("LABACLAW_BOOTSTRAP_REF")?,
        redpanda_brokers,
        command_topic: env_var_with_default("LABACLAW_COMMAND_TOPIC", "agent.command.v1"),
        event_topic: env_var_with_default("LABACLAW_EVENT_TOPIC", "agent.event.v1"),
        heartbeat_topic: env_var_with_default("LABACLAW_HEARTBEAT_TOPIC", "agent.heartbeat.v1"),
        rustfs_endpoint: env_var("LABACLAW_RUSTFS_ENDPOINT")?,
        rustfs_bucket: env_var_with_default("LABACLAW_RUSTFS_BUCKET", "laba-artifacts"),
        rustfs_region: env_var_with_default("LABACLAW_RUSTFS_REGION", "us-east-1"),
        rustfs_access_key: std::env::var("LABACLAW_RUSTFS_ACCESS_KEY")
            .ok()
            .or_else(|| std::env::var("RUSTFS_ACCESS_KEY").ok()),
        rustfs_secret_key: std::env::var("LABACLAW_RUSTFS_SECRET_KEY")
            .ok()
            .or_else(|| std::env::var("RUSTFS_SECRET_KEY").ok()),
        workload_name: env_var_with_default("HOSTNAME", "agent-runner"),
        consumer_group: format!(
            "labaclaw-agent-{}",
            env_var_with_default("LABACLAW_AGENT_ID", "agent-runner")
        ),
    })
}

fn build_consumer(cfg: &RunnerConfig) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", cfg.redpanda_brokers.join(","))
        .set("group.id", &cfg.consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("Failed to create agent-runner Kafka consumer")
}

fn build_producer(cfg: &RunnerConfig) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", cfg.redpanda_brokers.join(","))
        .set("message.timeout.ms", "10000")
        .create()
        .context("Failed to create agent-runner Kafka producer")
}

fn build_artifact_store(cfg: &RunnerConfig) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&cfg.rustfs_bucket)
        .with_region(&cfg.rustfs_region)
        .with_endpoint(&cfg.rustfs_endpoint)
        .with_allow_http(cfg.rustfs_endpoint.starts_with("http://"))
        .with_virtual_hosted_style_request(false);
    if let Some(access_key) = cfg.rustfs_access_key.as_deref() {
        builder = builder.with_access_key_id(access_key);
    }
    if let Some(secret_key) = cfg.rustfs_secret_key.as_deref() {
        builder = builder.with_secret_access_key(secret_key);
    }
    let store = builder
        .build()
        .context("Failed to create RustFS/S3 client in agent-runner")?;
    Ok(Arc::new(store))
}

async fn download_bytes(store: &dyn ObjectStore, reference: &str) -> Result<Vec<u8>> {
    let (_, key) = parse_s3_ref(reference)?;
    let bytes = store
        .get(&ObjectStorePath::from(key))
        .await
        .with_context(|| format!("Failed to fetch {reference}"))?
        .bytes()
        .await
        .with_context(|| format!("Failed to read bytes from {reference}"))?;
    Ok(bytes.to_vec())
}

async fn upload_bytes(store: &dyn ObjectStore, reference: &str, bytes: &[u8]) -> Result<()> {
    let (_, key) = parse_s3_ref(reference)?;
    store
        .put(&ObjectStorePath::from(key), bytes.to_vec().into())
        .await
        .with_context(|| format!("Failed to upload {reference}"))?;
    Ok(())
}

async fn publish_json<T: serde::Serialize>(
    producer: &FutureProducer,
    topic: &str,
    message_type: &str,
    agent_id: &str,
    payload: &T,
) -> Result<()> {
    let serialized =
        serde_json::to_string(payload).context("Failed to serialize worker event payload")?;
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

fn derive_result_ref(bootstrap_ref: &str, agent_id: &str, request_id: &str) -> Result<String> {
    let (bucket, key) = parse_s3_ref(bootstrap_ref)?;
    let prefix = key
        .split("/bootstrap/")
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');
    let key = if prefix.is_empty() {
        format!("results/{agent_id}/{request_id}/result.md")
    } else {
        format!("{prefix}/results/{agent_id}/{request_id}/result.md")
    };
    Ok(format!("s3://{bucket}/{key}"))
}

fn derive_result_json_ref(bootstrap_ref: &str, agent_id: &str, request_id: &str) -> Result<String> {
    let (bucket, key) = parse_s3_ref(bootstrap_ref)?;
    let prefix = key
        .split("/bootstrap/")
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');
    let key = if prefix.is_empty() {
        format!("results/{agent_id}/{request_id}/result.json")
    } else {
        format!("{prefix}/results/{agent_id}/{request_id}/result.json")
    };
    Ok(format!("s3://{bucket}/{key}"))
}

fn derive_question_ref(bootstrap_ref: &str, agent_id: &str, request_id: &str) -> Result<String> {
    let (bucket, key) = parse_s3_ref(bootstrap_ref)?;
    let prefix = key
        .split("/bootstrap/")
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');
    let key = if prefix.is_empty() {
        format!("questions/{agent_id}/{request_id}/question.md")
    } else {
        format!("{prefix}/questions/{agent_id}/{request_id}/question.md")
    };
    Ok(format!("s3://{bucket}/{key}"))
}

async fn upload_generated_artifacts(
    store: &dyn ObjectStore,
    bootstrap_ref: &str,
    agent_id: &str,
    request_id: &str,
    artifacts: &[labaclaw_worker_plane::GeneratedArtifact],
) -> Result<Vec<(String, String)>> {
    let mut uploaded = Vec::new();
    for artifact in artifacts {
        let artifact_ref = derive_named_artifact_ref(
            bootstrap_ref,
            agent_id,
            request_id,
            &artifact.relative_path,
        )?;
        upload_bytes(store, &artifact_ref, &artifact.bytes).await?;
        uploaded.push((artifact.relative_path.clone(), artifact_ref));
    }
    Ok(uploaded)
}

fn derive_named_artifact_ref(
    bootstrap_ref: &str,
    agent_id: &str,
    request_id: &str,
    relative_path: &str,
) -> Result<String> {
    let (bucket, key) = parse_s3_ref(bootstrap_ref)?;
    let prefix = key
        .split("/bootstrap/")
        .next()
        .unwrap_or_default()
        .trim_end_matches('/');
    let normalized_path = relative_path
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .map(|segment| {
            segment
                .chars()
                .map(|ch| {
                    if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                        ch
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/");
    let key = if prefix.is_empty() {
        format!("artifacts/{agent_id}/{request_id}/{normalized_path}")
    } else {
        format!("{prefix}/artifacts/{agent_id}/{request_id}/{normalized_path}")
    };
    Ok(format!("s3://{bucket}/{key}"))
}

fn env_var(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("Missing required environment variable {name}"))
}

fn env_var_with_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}
