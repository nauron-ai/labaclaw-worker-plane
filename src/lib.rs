use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::Command;

pub const MESSAGE_TYPE_HEADER: &str = "labaclaw-message-type";
pub const COMMAND_TYPE_SPAWN_AGENT_REQUESTED: &str = "SpawnAgentRequested";
pub const COMMAND_TYPE_TASK_ASSIGNED: &str = "TaskAssigned";
pub const COMMAND_TYPE_SUSPEND_AGENT_REQUESTED: &str = "SuspendAgentRequested";
pub const COMMAND_TYPE_RESUME_AGENT_REQUESTED: &str = "ResumeAgentRequested";
pub const COMMAND_TYPE_TERMINATE_AGENT_REQUESTED: &str = "TerminateAgentRequested";
pub const EVENT_TYPE_AGENT_SPAWNED: &str = "AgentSpawned";
pub const EVENT_TYPE_AGENT_HEARTBEAT: &str = "AgentHeartbeat";
pub const EVENT_TYPE_AGENT_PROGRESS_REPORTED: &str = "AgentProgressReported";
pub const EVENT_TYPE_AGENT_QUESTION_RAISED: &str = "AgentQuestionRaised";
pub const EVENT_TYPE_AGENT_COMPLETED: &str = "AgentCompleted";
pub const EVENT_TYPE_AGENT_SUSPENDED: &str = "AgentSuspended";
pub const EVENT_TYPE_AGENT_TERMINATED: &str = "AgentTerminated";
pub const EVENT_TYPE_AGENT_SPAWN_FAILED: &str = "AgentSpawnFailed";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpawnAgentRequested {
    pub event_id: String,
    pub agent_id: String,
    pub owner_agent_id: String,
    pub spec_ref: String,
    pub bootstrap_ref: String,
    pub lifecycle_mode: String,
    pub task_profile: String,
    pub requested_at: String,
    pub delivery_backend: Option<String>,
    pub worker_namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskAssigned {
    pub event_id: String,
    pub agent_id: String,
    pub request_id: String,
    pub task_ref: String,
    pub assigned_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuspendAgentRequested {
    pub event_id: String,
    pub agent_id: String,
    pub reason: Option<String>,
    pub requested_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeAgentRequested {
    pub event_id: String,
    pub agent_id: String,
    pub requested_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TerminateAgentRequested {
    pub event_id: String,
    pub agent_id: String,
    pub reason: String,
    pub requested_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpawnedBootstrapRequest {
    pub request_id: String,
    pub message: String,
    #[serde(default)]
    pub pack_id: Option<String>,
    #[serde(default)]
    pub task_profile: Option<String>,
    pub max_history_messages: Option<usize>,
    pub max_tool_iterations: Option<usize>,
    pub compact_context: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCompleted {
    pub event_id: String,
    pub agent_id: String,
    pub request_id: String,
    pub result_ref: String,
    pub result_json_ref: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentFactoryValues {
    pub worker_image_repository: String,
    pub worker_image_tag: String,
    pub namespace: String,
    pub service_account: String,
    pub runtime_secret_name: String,
    pub redpanda_brokers: Vec<String>,
    pub command_topic: String,
    pub event_topic: String,
    pub heartbeat_topic: String,
    pub rustfs_endpoint: String,
    pub rustfs_bucket: String,
    pub rustfs_prefix: String,
    pub rustfs_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentMetadata {
    pub name: String,
    pub namespace: String,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DedicatedAgentDeploymentSpec {
    pub metadata: DeploymentMetadata,
    pub image: String,
    pub agent_id: String,
    pub owner_agent_id: String,
    pub pack_id: Option<String>,
    pub spec_ref: String,
    pub bootstrap_ref: String,
    pub redpanda_brokers: Vec<String>,
    pub command_topic: String,
    pub event_topic: String,
    pub heartbeat_topic: String,
    pub rustfs_endpoint: String,
    pub rustfs_bucket: String,
    pub rustfs_prefix: String,
    pub rustfs_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSpawnedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub runtime_backend: String,
    pub workload_kind: String,
    pub workload_namespace: String,
    pub workload_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentHeartbeatEvent {
    pub event_id: String,
    pub agent_id: String,
    pub service_state: String,
    pub runtime_backend: String,
    pub workload_name: String,
    pub observed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentProgressReportedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub stage: String,
    pub detail: String,
    pub reported_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentQuestionRaisedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub request_id: String,
    pub question_ref: Option<String>,
    pub question_summary: Option<String>,
    pub blocking: bool,
    pub raised_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSuspendedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub reason: Option<String>,
    pub suspended_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentTerminatedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub reason: String,
    pub terminated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSpawnFailedEvent {
    pub event_id: String,
    pub agent_id: String,
    pub request_id: Option<String>,
    pub error: String,
    pub failed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BootstrapExecutionOutcome {
    pub result_markdown: Option<String>,
    pub summary: Option<String>,
    pub question_markdown: Option<String>,
    pub question_summary: Option<String>,
    pub artifacts: Vec<GeneratedArtifact>,
    pub fatal_error: Option<String>,
    pub status: String,
    pub questions: Vec<String>,
    pub confidence: f32,
    pub validation_status: String,
    pub direct_write_recommendation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratedArtifact {
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerArtifactReference {
    pub name: String,
    pub reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerResultSidecar {
    pub status: String,
    pub summary: String,
    pub questions: Vec<String>,
    pub confidence: f32,
    pub validation_status: String,
    pub artifacts: Vec<WorkerArtifactReference>,
    pub direct_write_recommendation: bool,
}

pub fn deployment_name(agent_id: &str) -> String {
    let mut normalized = agent_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while normalized.contains("--") {
        normalized = normalized.replace("--", "-");
    }
    let normalized = normalized.trim_matches('-');
    let base = format!("agent-{}", normalized);
    if base.len() <= 63 {
        base
    } else {
        base[..63].trim_matches('-').to_string()
    }
}

pub fn build_dedicated_agent_spec(
    request: &SpawnAgentRequested,
    values: &AgentFactoryValues,
) -> DedicatedAgentDeploymentSpec {
    DedicatedAgentDeploymentSpec {
        metadata: DeploymentMetadata {
            name: deployment_name(&request.agent_id),
            namespace: values.namespace.clone(),
            labels: vec![
                ("app".into(), "labaclaw-agent".into()),
                ("labaclaw/agent-id".into(), request.agent_id.clone()),
                (
                    "labaclaw/owner-agent-id".into(),
                    request.owner_agent_id.clone(),
                ),
                ("labaclaw/task-profile".into(), request.task_profile.clone()),
            ],
        },
        image: format!(
            "{}:{}",
            values.worker_image_repository, values.worker_image_tag
        ),
        agent_id: request.agent_id.clone(),
        owner_agent_id: request.owner_agent_id.clone(),
        pack_id: None,
        spec_ref: request.spec_ref.clone(),
        bootstrap_ref: request.bootstrap_ref.clone(),
        redpanda_brokers: values.redpanda_brokers.clone(),
        command_topic: values.command_topic.clone(),
        event_topic: values.event_topic.clone(),
        heartbeat_topic: values.heartbeat_topic.clone(),
        rustfs_endpoint: values.rustfs_endpoint.clone(),
        rustfs_bucket: values.rustfs_bucket.clone(),
        rustfs_prefix: values.rustfs_prefix.clone(),
        rustfs_region: values.rustfs_region.clone(),
    }
}

pub fn build_deployment_manifest(
    spec: &DedicatedAgentDeploymentSpec,
    service_account: &str,
    runtime_secret_name: &str,
) -> serde_json::Value {
    let labels = spec
        .metadata
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect::<serde_json::Map<String, serde_json::Value>>();

    let mut env = vec![
        json!({"name": "LABACLAW_AGENT_ID", "value": spec.agent_id}),
        json!({"name": "LABACLAW_OWNER_AGENT_ID", "value": spec.owner_agent_id}),
        json!({"name": "LABACLAW_AGENT_SPEC_REF", "value": spec.spec_ref}),
        json!({"name": "LABACLAW_BOOTSTRAP_REF", "value": spec.bootstrap_ref}),
        json!({"name": "LABACLAW_WORKER_PLANE_REDPANDA_BROKERS", "value": spec.redpanda_brokers.join(",")}),
        json!({"name": "LABACLAW_COMMAND_TOPIC", "value": spec.command_topic}),
        json!({"name": "LABACLAW_EVENT_TOPIC", "value": spec.event_topic}),
        json!({"name": "LABACLAW_HEARTBEAT_TOPIC", "value": spec.heartbeat_topic}),
        json!({"name": "LABACLAW_RUSTFS_ENDPOINT", "value": spec.rustfs_endpoint}),
        json!({"name": "LABACLAW_RUSTFS_BUCKET", "value": spec.rustfs_bucket}),
        json!({"name": "LABACLAW_RUSTFS_PREFIX", "value": spec.rustfs_prefix}),
        json!({"name": "LABACLAW_RUSTFS_REGION", "value": spec.rustfs_region}),
        json!({"name": "LABACLAW_RUNTIME_BACKEND", "value": "redpanda_k8s"}),
    ];
    env.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));

    let mut spec_json = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": spec.metadata.name,
            "namespace": spec.metadata.namespace,
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "app": "labaclaw-agent",
                    "labaclaw/agent-id": spec.agent_id,
                }
            },
            "template": {
                "metadata": {
                    "labels": labels,
                },
                "spec": {
                    "serviceAccountName": service_account,
                    "containers": [{
                        "name": "agent-runner",
                        "image": spec.image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["/usr/local/bin/agent-runner"],
                        "env": env,
                    }]
                }
            }
        }
    });

    if !runtime_secret_name.trim().is_empty() {
        spec_json["spec"]["template"]["spec"]["containers"][0]["envFrom"] =
            json!([{ "secretRef": { "name": runtime_secret_name } }]);
    }

    spec_json
}

pub fn render_deployment_yaml(
    spec: &DedicatedAgentDeploymentSpec,
    service_account: &str,
    runtime_secret_name: &str,
) -> Result<String> {
    serde_yaml::to_string(&build_deployment_manifest(
        spec,
        service_account,
        runtime_secret_name,
    ))
    .context("Failed to render Deployment manifest as YAML")
}

pub fn parse_s3_ref(reference: &str) -> Result<(String, String)> {
    let trimmed = reference.trim();
    let remainder = trimmed
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("Unsupported artifact ref '{trimmed}', expected s3://"))?;
    let mut parts = remainder.splitn(2, '/');
    let bucket = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Missing bucket in artifact ref '{trimmed}'"))?;
    let key = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Missing object key in artifact ref '{trimmed}'"))?;
    Ok((bucket.to_string(), key.to_string()))
}

pub fn summarize_text_for_event(text: &str) -> String {
    let first_non_empty = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    let mut summary = first_non_empty.to_string();
    if summary.len() > 280 {
        summary.truncate(277);
        summary.push_str("...");
    }
    summary
}

pub fn execute_bootstrap_request(request: &SpawnedBootstrapRequest) -> BootstrapExecutionOutcome {
    let message = request.message.as_str();
    match request.pack_id.as_deref() {
        Some("software_builder") => {
            if is_rust_calculator_request(message) {
                return match build_rust_calculator_outcome() {
                    Ok(outcome) => outcome,
                    Err(error) => fatal_outcome(error),
                };
            }

            return question_outcome(
                "Dedicated software worker needs an exact target and validation scope before execution",
                "QUESTION FOR ORCHESTRATOR\nThis software builder supports verified Rust calculator delivery in v1. Provide the exact target, required interface, and validation scope, or route the task through a requirements analyst first.",
            );
        }
        Some("requirements_analyst") => return build_requirements_analyst_outcome(message),
        Some("market_technical_analyst") => {
            return build_market_technical_analysis_outcome(message)
        }
        _ => {}
    }

    let revenue = extract_named_number(message, "revenue");
    let costs =
        extract_named_number(message, "costs").or_else(|| extract_named_number(message, "cost"));
    if let (Some(revenue), Some(costs)) = (revenue, costs) {
        if revenue > 0.0 {
            let margin = ((revenue - costs) / revenue) * 100.0;
            let result = format!(
                "RESULT FOR ORCHESTRATOR\nMargin is {:.1}%.\nRevenue: {:.2}\nCosts: {:.2}\nComputed by dedicated worker-plane finance validator.",
                margin, revenue, costs
            );
            return completed_outcome(
                format!("Margin computed at {:.1}%", margin),
                result,
                Vec::new(),
                0.98,
                "green",
                false,
            );
        }
    }

    if message.to_ascii_lowercase().contains("margin") {
        return question_outcome(
            "Missing revenue/cost inputs required for margin calculation",
            "QUESTION FOR ORCHESTRATOR\nRevenue and costs are required to compute margin. Please provide both values or attach the source document.",
        );
    }

    if is_software_delivery_request(message) {
        return question_outcome(
            "Dedicated software worker needs a supported Rust CLI scope before execution",
            "QUESTION FOR ORCHESTRATOR\nThis production worker currently supports verified Rust CLI calculator delivery. Please restate the task with the exact target and validation scope, or provision a broader software builder worker.",
        );
    }

    let result = format!(
        "RESULT FOR ORCHESTRATOR\nBootstrap mission executed.\nSummary: {}",
        summarize_text_for_event(message)
    );
    completed_outcome(
        summarize_text_for_event(&result),
        result,
        Vec::new(),
        0.6,
        "not_applicable",
        false,
    )
}

pub fn execute_bootstrap_message(message: &str) -> BootstrapExecutionOutcome {
    execute_bootstrap_request(&SpawnedBootstrapRequest {
        request_id: "ad-hoc".into(),
        message: message.into(),
        pack_id: None,
        task_profile: None,
        max_history_messages: None,
        max_tool_iterations: None,
        compact_context: false,
        created_at: Utc::now().to_rfc3339(),
    })
}

fn is_rust_calculator_request(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    contains_any(&lowered, &["rust", "cargo"])
        && contains_any(&lowered, &["calculator", "kalkulator"])
        && contains_any(&lowered, &["floating", "float", "zmiennoprzecink", "f64"])
        && contains_any(&lowered, &["parentheses", "nawias"])
}

fn is_software_delivery_request(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    contains_any(
        &lowered,
        &[
            "rust",
            "cargo",
            "code",
            "implement",
            "application",
            "app",
            "compile",
            "build",
            "test",
            "api",
            "cli",
            "library",
            "repo",
        ],
    )
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn completed_outcome(
    summary: impl Into<String>,
    result_markdown: impl Into<String>,
    artifacts: Vec<GeneratedArtifact>,
    confidence: f32,
    validation_status: &str,
    direct_write_recommendation: bool,
) -> BootstrapExecutionOutcome {
    let summary = summary.into();
    BootstrapExecutionOutcome {
        result_markdown: Some(result_markdown.into()),
        summary: Some(summary.clone()),
        question_markdown: None,
        question_summary: None,
        artifacts,
        fatal_error: None,
        status: "completed".into(),
        questions: Vec::new(),
        confidence,
        validation_status: validation_status.into(),
        direct_write_recommendation,
    }
}

fn question_outcome(
    summary: impl Into<String>,
    question_markdown: impl Into<String>,
) -> BootstrapExecutionOutcome {
    let summary = summary.into();
    let question = question_markdown.into();
    BootstrapExecutionOutcome {
        result_markdown: None,
        summary: None,
        question_markdown: Some(question.clone()),
        question_summary: Some(summary.clone()),
        artifacts: Vec::new(),
        fatal_error: None,
        status: "questions_pending".into(),
        questions: vec![question],
        confidence: 0.25,
        validation_status: "needs_input".into(),
        direct_write_recommendation: false,
    }
}

fn fatal_outcome(error: anyhow::Error) -> BootstrapExecutionOutcome {
    BootstrapExecutionOutcome {
        result_markdown: None,
        summary: None,
        question_markdown: None,
        question_summary: None,
        artifacts: Vec::new(),
        fatal_error: Some(error.to_string()),
        status: "failed".into(),
        questions: Vec::new(),
        confidence: 0.0,
        validation_status: "failed".into(),
        direct_write_recommendation: false,
    }
}

fn build_requirements_analyst_outcome(message: &str) -> BootstrapExecutionOutcome {
    let lowered = message.to_ascii_lowercase();
    if contains_any(&lowered, &["calculator", "kalkulator"]) {
        let interface = extract_first_named_text(message, &["interface", "target", "form"])
            .or_else(|| {
                contains_any(&lowered, &["cli", "command line", "terminal"]).then(|| "cli".into())
            })
            .or_else(|| contains_any(&lowered, &["library", "crate"]).then(|| "library".into()));
        let delivery = extract_first_named_text(message, &["delivery", "artifact", "workspace"]);
        let expression_input = extract_first_named_text(message, &["input", "expression"]);

        let mut missing = Vec::new();
        if interface.is_none() {
            missing.push("target interface (CLI or library)");
        }
        if expression_input.is_none() {
            missing.push("expression input format");
        }
        if delivery.is_none() {
            missing.push("delivery target (artifacts or direct workspace integration)");
        }

        if !missing.is_empty() {
            return question_outcome(
                "Requirements analyst still needs a few calculator details",
                format!(
                    "QUESTION FOR ORCHESTRATOR\nFor the calculator brief, please confirm: {}.",
                    missing.join(", ")
                ),
            );
        }

        let brief = format!(
            "RESULT FOR ORCHESTRATOR\nImplementation brief prepared.\n\nGoal:\n- Deliver a Rust floating-point calculator.\n\nInterface:\n- {interface}\n\nInput:\n- {expression_input}\n\nValidation:\n- `cargo test --quiet`\n- `cargo build --release --quiet`\n\nDelivery:\n- {delivery}\n\nRequired behavior:\n- support `f64`\n- support `+`, `-`, `*`, `/`\n- support parentheses\n- support unary minus",
            interface = interface.unwrap_or_else(|| "unknown".into()),
            expression_input = expression_input.unwrap_or_else(|| "unknown".into()),
            delivery = delivery.unwrap_or_else(|| "unknown".into()),
        );
        return completed_outcome(
            "Requirements analyst prepared a Rust calculator brief",
            brief,
            Vec::new(),
            0.84,
            "green",
            false,
        );
    }

    question_outcome(
        "Requirements analyst needs a narrower problem statement",
        "QUESTION FOR ORCHESTRATOR\nRestate the task with a concrete target, constraints, and validation scope so I can prepare an implementation brief.",
    )
}

fn build_market_technical_analysis_outcome(message: &str) -> BootstrapExecutionOutcome {
    let ticker = extract_first_named_text(message, &["ticker", "instrument", "symbol"]);
    let timeframe = extract_first_named_text(message, &["timeframe", "tf"]);
    let horizon = extract_first_named_text(message, &["horizon", "holding"]);
    let exchange = extract_first_named_text(message, &["exchange", "market"]);
    let source = extract_first_named_text(message, &["source", "data", "chart"]);
    let trend = extract_first_named_text(message, &["trend", "bias"]);
    let macd = extract_first_named_text(message, &["macd"]);
    let price = extract_named_number(message, "price");
    let support = extract_named_number(message, "support");
    let resistance = extract_named_number(message, "resistance");
    let rsi = extract_named_number(message, "rsi");

    let mut missing = Vec::new();
    if ticker.is_none() {
        missing.push("ticker");
    }
    if timeframe.is_none() {
        missing.push("timeframe");
    }
    if horizon.is_none() {
        missing.push("horizon");
    }
    if source.is_none() {
        missing.push("data/chart source");
    }
    if price.is_none() {
        missing.push("price");
    }
    if support.is_none() {
        missing.push("support");
    }
    if resistance.is_none() {
        missing.push("resistance");
    }

    if !missing.is_empty() {
        return question_outcome(
            "Market technical analysis worker is missing required setup inputs",
            format!(
                "QUESTION FOR ORCHESTRATOR\nTo prepare a technical-analysis trade setup, provide these fields: {}. Recommended format: `ticker=... timeframe=... horizon=... exchange=... source=... price=... support=... resistance=... rsi=... macd=... trend=...`.",
                missing.join(", ")
            ),
        );
    }

    let ticker = ticker.unwrap_or_else(|| "UNKNOWN".into());
    let timeframe = timeframe.unwrap_or_else(|| "unknown".into());
    let horizon = horizon.unwrap_or_else(|| "unknown".into());
    let exchange = exchange.unwrap_or_else(|| "unspecified".into());
    let source = source.unwrap_or_else(|| "unspecified".into());
    let price = price.unwrap_or(0.0);
    let support = support.unwrap_or(0.0);
    let resistance = resistance.unwrap_or(price);
    let trend = trend.unwrap_or_else(|| {
        if resistance > support && price >= support {
            "bullish".into()
        } else {
            "bearish".into()
        }
    });
    let bullish = trend.to_ascii_lowercase().contains("bull");
    let range = (resistance - support).abs();
    let invalidation = if bullish {
        support - (range * 0.2)
    } else {
        resistance + (range * 0.2)
    };
    let tp1 = if bullish { resistance } else { support };
    let tp2 = if bullish {
        resistance + range.max(price * 0.01)
    } else {
        support - range.max(price * 0.01)
    };
    let setup = if bullish { "long" } else { "short" };
    let indicator_note = match (rsi, macd.as_deref()) {
        (Some(rsi), Some(macd)) => format!("RSI={rsi:.1}, MACD={macd}"),
        (Some(rsi), None) => format!("RSI={rsi:.1}"),
        (None, Some(macd)) => format!("MACD={macd}"),
        (None, None) => "No explicit RSI/MACD snapshot supplied".into(),
    };
    let result = format!(
        "RESULT FOR ORCHESTRATOR\nTrade setup for {ticker} on {timeframe} ({exchange}).\n\nBias:\n- {trend}\n\nSetup:\n- Type: {setup}\n- Entry zone: {entry_low:.2} - {entry_high:.2}\n- Invalidation: {invalidation:.2}\n- TP1: {tp1:.2}\n- TP2: {tp2:.2}\n\nRationale:\n- Source: {source}\n- Price snapshot: {price:.2}\n- Support: {support:.2}\n- Resistance: {resistance:.2}\n- Indicators: {indicator_note}\n- Horizon: {horizon}\n\nRisks:\n- Setup depends on the supplied snapshot staying representative for the stated horizon.\n- If price invalidates the key level, the thesis is broken.\n\nDisclaimer:\n- This is market technical analysis, not investment advice.",
        entry_low = if bullish { support } else { resistance.min(price) },
        entry_high = if bullish { price.max(support) } else { resistance.max(price) },
    );
    let confidence = if rsi.is_some() || macd.is_some() {
        0.9
    } else {
        0.84
    };
    completed_outcome(
        format!("Trade setup prepared for {ticker} on {timeframe}"),
        result,
        Vec::new(),
        confidence,
        "green",
        false,
    )
}

fn extract_first_named_text(text: &str, labels: &[&str]) -> Option<String> {
    labels
        .iter()
        .find_map(|label| extract_named_text(text, label))
}

fn extract_named_text(text: &str, label: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    let label = label.to_ascii_lowercase();
    for separator in ["=", ":"].iter() {
        let marker = format!("{label}{separator}");
        if let Some(start) = lower.find(&marker) {
            let raw = &text[start + marker.len()..];
            let value = raw
                .split(['\n', ',', ';'])
                .next()
                .map(str::trim)
                .filter(|candidate| !candidate.is_empty())?;
            return Some(value.to_string());
        }
    }
    None
}

fn build_rust_calculator_outcome() -> Result<BootstrapExecutionOutcome> {
    let project_root =
        std::env::temp_dir().join(format!("labaclaw-rust-calculator-{}", uuid::Uuid::new_v4()));
    let src_dir = project_root.join("src");
    fs::create_dir_all(&src_dir)
        .with_context(|| format!("Failed to create {}", src_dir.display()))?;

    let cargo_toml = r#"[package]
name = "rust_calculator_worker"
version = "0.1.0"
edition = "2021"

[dependencies]
"#;
    let main_rs = rust_calculator_source();

    fs::write(project_root.join("Cargo.toml"), cargo_toml).with_context(|| {
        format!(
            "Failed to write {}",
            project_root.join("Cargo.toml").display()
        )
    })?;
    fs::write(src_dir.join("main.rs"), main_rs)
        .with_context(|| format!("Failed to write {}", src_dir.join("main.rs").display()))?;

    let test_log = run_cargo_command(&project_root, &["test", "--quiet"])?;
    let build_log = run_cargo_command(&project_root, &["build", "--release", "--quiet"])?;
    let result = "RESULT FOR ORCHESTRATOR\nRust CLI calculator project generated and verified.\nChecks passed:\n- cargo test --quiet\n- cargo build --release --quiet\nSupported scope:\n- floating-point numbers (f64)\n- parentheses\n- operators +, -, *, /\n- unary minus\nExample run:\n- cargo run -- \"1 + 2 * (3.5 - 1)\"";

    let outcome = completed_outcome(
        "Rust calculator project generated and compiled successfully",
        result,
        vec![
            GeneratedArtifact {
                relative_path: "Cargo.toml".into(),
                bytes: cargo_toml.as_bytes().to_vec(),
            },
            GeneratedArtifact {
                relative_path: "src/main.rs".into(),
                bytes: main_rs.as_bytes().to_vec(),
            },
            GeneratedArtifact {
                relative_path: "logs/cargo-test.log".into(),
                bytes: test_log.into_bytes(),
            },
            GeneratedArtifact {
                relative_path: "logs/cargo-build-release.log".into(),
                bytes: build_log.into_bytes(),
            },
        ],
        0.94,
        "green",
        false,
    );

    let _ = fs::remove_dir_all(&project_root);
    Ok(outcome)
}

fn run_cargo_command(project_root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("cargo")
        .args(args)
        .current_dir(project_root)
        .output()
        .with_context(|| format!("Failed to start `cargo {}`", args.join(" ")))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let mut combined = String::new();
    if !stdout.is_empty() {
        combined.push_str("STDOUT:\n");
        combined.push_str(&stdout);
        combined.push('\n');
    }
    if !stderr.is_empty() {
        combined.push_str("STDERR:\n");
        combined.push_str(&stderr);
        combined.push('\n');
    }
    if combined.trim().is_empty() {
        combined = format!("`cargo {}` completed with no output.\n", args.join(" "));
    }

    if !output.status.success() {
        anyhow::bail!(
            "`cargo {}` failed with status {}.\n{}",
            args.join(" "),
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".into()),
            combined
        );
    }

    Ok(combined)
}

fn rust_calculator_source() -> &'static str {
    r#"use std::env;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

fn main() {
    let expression = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if expression.trim().is_empty() {
        eprintln!("usage: cargo run -- \"1 + 2 * (3.5 - 1)\"");
        std::process::exit(2);
    }

    match evaluate(&expression) {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn evaluate(input: &str) -> Result<f64, String> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    let value = parser.parse_expression()?;
    if parser.peek().is_some() {
        return Err("unexpected trailing tokens".into());
    }
    Ok(value)
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(&ch) = chars.peek() {
        match ch {
            '0'..='9' | '.' => tokens.push(Token::Number(read_number(&mut chars)?)),
            '+' => {
                chars.next();
                tokens.push(Token::Plus);
            }
            '-' => {
                chars.next();
                tokens.push(Token::Minus);
            }
            '*' => {
                chars.next();
                tokens.push(Token::Star);
            }
            '/' => {
                chars.next();
                tokens.push(Token::Slash);
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            ch if ch.is_whitespace() => {
                chars.next();
            }
            other => return Err(format!("unexpected character: {other}")),
        }
    }

    Ok(tokens)
}

fn read_number<I>(chars: &mut std::iter::Peekable<I>) -> Result<f64, String>
where
    I: Iterator<Item = char>,
{
    let mut buffer = String::new();
    let mut seen_dot = false;

    while let Some(&ch) = chars.peek() {
        match ch {
            '0'..='9' => {
                buffer.push(ch);
                chars.next();
            }
            '.' if !seen_dot => {
                seen_dot = true;
                buffer.push(ch);
                chars.next();
            }
            _ => break,
        }
    }

    buffer
        .parse::<f64>()
        .map_err(|_| format!("invalid floating-point number: {buffer}"))
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse_expression(&mut self) -> Result<f64, String> {
        let mut value = self.parse_term()?;
        while let Some(token) = self.peek() {
            match token {
                Token::Plus => {
                    self.index += 1;
                    value += self.parse_term()?;
                }
                Token::Minus => {
                    self.index += 1;
                    value -= self.parse_term()?;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_term(&mut self) -> Result<f64, String> {
        let mut value = self.parse_factor()?;
        while let Some(token) = self.peek() {
            match token {
                Token::Star => {
                    self.index += 1;
                    value *= self.parse_factor()?;
                }
                Token::Slash => {
                    self.index += 1;
                    let rhs = self.parse_factor()?;
                    if rhs == 0.0 {
                        return Err("division by zero".into());
                    }
                    value /= rhs;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_factor(&mut self) -> Result<f64, String> {
        match self.peek() {
            Some(Token::Minus) => {
                self.index += 1;
                Ok(-self.parse_factor()?)
            }
            Some(Token::Plus) => {
                self.index += 1;
                self.parse_factor()
            }
            Some(Token::Number(value)) => {
                let number = *value;
                self.index += 1;
                Ok(number)
            }
            Some(Token::LParen) => {
                self.index += 1;
                let value = self.parse_expression()?;
                match self.peek() {
                    Some(Token::RParen) => {
                        self.index += 1;
                        Ok(value)
                    }
                    _ => Err("missing closing parenthesis".into()),
                }
            }
            _ => Err("expected a number or parenthesized expression".into()),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn evaluates_parentheses_and_precedence() {
        assert_eq!(evaluate("2 * (3.5 + 1.5)").unwrap(), 10.0);
    }

    #[test]
    fn evaluates_unary_minus() {
        assert_eq!(evaluate("-3 + 4 * 2").unwrap(), 5.0);
    }

    #[test]
    fn evaluates_floats() {
        assert_eq!(evaluate("1.25 + 2.75").unwrap(), 4.0);
    }
}
"#
}

pub fn spawn_failed_event(
    agent_id: &str,
    request_id: Option<String>,
    error: impl Into<String>,
) -> AgentSpawnFailedEvent {
    AgentSpawnFailedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        request_id,
        error: error.into(),
        failed_at: Utc::now().to_rfc3339(),
    }
}

pub fn progress_event(
    agent_id: &str,
    request_id: Option<String>,
    stage: impl Into<String>,
    detail: impl Into<String>,
) -> AgentProgressReportedEvent {
    AgentProgressReportedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        request_id,
        stage: stage.into(),
        detail: detail.into(),
        reported_at: Utc::now().to_rfc3339(),
    }
}

pub fn heartbeat_event(
    agent_id: &str,
    workload_name: &str,
    service_state: &str,
) -> AgentHeartbeatEvent {
    AgentHeartbeatEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        service_state: service_state.to_string(),
        runtime_backend: "redpanda_k8s".into(),
        workload_name: workload_name.to_string(),
        observed_at: Utc::now().to_rfc3339(),
    }
}

pub fn spawned_event(agent_id: &str, namespace: &str, workload_name: &str) -> AgentSpawnedEvent {
    AgentSpawnedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        runtime_backend: "redpanda_k8s".into(),
        workload_kind: "Deployment".into(),
        workload_namespace: namespace.to_string(),
        workload_name: workload_name.to_string(),
    }
}

pub fn suspended_event(agent_id: &str, reason: Option<String>) -> AgentSuspendedEvent {
    AgentSuspendedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        reason,
        suspended_at: Utc::now().to_rfc3339(),
    }
}

pub fn terminated_event(agent_id: &str, reason: impl Into<String>) -> AgentTerminatedEvent {
    AgentTerminatedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        reason: reason.into(),
        terminated_at: Utc::now().to_rfc3339(),
    }
}

pub fn completed_event(
    agent_id: &str,
    request_id: &str,
    result_ref: impl Into<String>,
    result_json_ref: Option<String>,
    summary: impl Into<String>,
) -> AgentCompleted {
    AgentCompleted {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        request_id: request_id.to_string(),
        result_ref: result_ref.into(),
        result_json_ref,
        summary: summary.into(),
    }
}

pub fn question_event(
    agent_id: &str,
    request_id: &str,
    question_ref: Option<String>,
    question_summary: Option<String>,
) -> AgentQuestionRaisedEvent {
    AgentQuestionRaisedEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        request_id: request_id.to_string(),
        question_ref,
        question_summary,
        blocking: true,
        raised_at: Utc::now().to_rfc3339(),
    }
}

fn extract_named_number(text: &str, label: &str) -> Option<f64> {
    let lower = text.to_ascii_lowercase();
    let label_lower = label.to_ascii_lowercase();
    let start = lower.find(&label_lower)?;
    let remainder = &text[start + label_lower.len()..];
    extract_first_number(remainder)
}

fn extract_first_number(text: &str) -> Option<f64> {
    let mut started = false;
    let mut buffer = String::new();
    for ch in text.chars() {
        if ch.is_ascii_digit() || (!started && (ch == '-' || ch == '+')) || (started && ch == '.') {
            started = true;
            buffer.push(ch);
            continue;
        }
        if started {
            break;
        }
    }
    if buffer.is_empty() {
        None
    } else {
        buffer.parse::<f64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_name_is_kubernetes_safe() {
        let name = deployment_name("financial-analyst-ABC_123");
        assert_eq!(name, "agent-financial-analyst-abc-123");
    }

    #[test]
    fn render_deployment_includes_runtime_env() {
        let request = SpawnAgentRequested {
            event_id: "evt-1".into(),
            agent_id: "financial-analyst-1".into(),
            owner_agent_id: "orchestrator".into(),
            spec_ref: "s3://laba-artifacts/labaclaw/specs/financial-analyst-1/v1/agent-spec.json"
                .into(),
            bootstrap_ref:
                "s3://laba-artifacts/labaclaw/bootstrap/financial-analyst-1/req-1/request.json"
                    .into(),
            lifecycle_mode: "dedicated".into(),
            task_profile: "deep_reasoning".into(),
            requested_at: "2026-03-13T00:00:00Z".into(),
            delivery_backend: Some("redpanda_k8s".into()),
            worker_namespace: Some("labaclaw-workers".into()),
        };
        let values = AgentFactoryValues {
            worker_image_repository: "ghcr.io/nauron-ai/labaclaw-worker-plane".into(),
            worker_image_tag: "latest".into(),
            namespace: "labaclaw-workers".into(),
            service_account: "labaclaw-worker-plane".into(),
            runtime_secret_name: "labaclaw-worker-plane-runtime".into(),
            redpanda_brokers: vec!["redpanda.platform.svc.cluster.local:9092".into()],
            command_topic: "agent.command.v1".into(),
            event_topic: "agent.event.v1".into(),
            heartbeat_topic: "agent.heartbeat.v1".into(),
            rustfs_endpoint: "http://shared-rustfs-svc.platform.svc.cluster.local:9000".into(),
            rustfs_bucket: "laba-artifacts".into(),
            rustfs_prefix: "labaclaw".into(),
            rustfs_region: "us-east-1".into(),
        };
        let spec = build_dedicated_agent_spec(&request, &values);
        let yaml = render_deployment_yaml(
            &spec,
            "labaclaw-worker-plane",
            "labaclaw-worker-plane-runtime",
        )
        .unwrap();
        assert!(yaml.contains("LABACLAW_AGENT_SPEC_REF"));
        assert!(yaml.contains("LABACLAW_WORKER_PLANE_REDPANDA_BROKERS"));
        assert!(yaml.contains("/usr/local/bin/agent-runner"));
        assert!(yaml.contains("envFrom"));
    }

    #[test]
    fn parse_s3_ref_splits_bucket_and_key() {
        let (bucket, key) =
            parse_s3_ref("s3://laba-artifacts/labaclaw/results/agent-1/req-1/result.md")
                .expect("s3 ref should parse");
        assert_eq!(bucket, "laba-artifacts");
        assert_eq!(key, "labaclaw/results/agent-1/req-1/result.md");
    }

    #[test]
    fn execute_bootstrap_message_computes_margin() {
        let outcome = execute_bootstrap_message(
            "Compute the margin from Revenue=1000 and Costs=700. Return RESULT FOR ORCHESTRATOR.",
        );
        let result = outcome.result_markdown.expect("result should be present");
        assert!(result.contains("30.0%"));
        assert!(outcome.question_markdown.is_none());
    }

    #[test]
    fn execute_bootstrap_message_asks_for_missing_finance_inputs() {
        let outcome = execute_bootstrap_message("Analyze the margin drop.");
        assert!(outcome.result_markdown.is_none());
        assert!(outcome
            .question_summary
            .expect("question summary")
            .contains("Missing revenue/cost"));
    }

    #[test]
    fn execute_bootstrap_message_builds_rust_calculator_artifacts() {
        let outcome = execute_bootstrap_request(&SpawnedBootstrapRequest {
            request_id: "req-calc".into(),
            message: "Create a CLI calculator in Rust using floating-point numbers, parentheses, and cargo build verification. interface=cli input=single quoted expression delivery=artifacts".into(),
            pack_id: Some("software_builder".into()),
            task_profile: Some("deep_reasoning".into()),
            max_history_messages: None,
            max_tool_iterations: None,
            compact_context: false,
            created_at: "2026-03-15T00:00:00Z".into(),
        });
        let result = outcome.result_markdown.expect("result should be present");
        assert!(result.contains("Rust CLI calculator project generated and verified"));
        assert!(outcome.fatal_error.is_none());
        assert!(outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.relative_path == "Cargo.toml"));
        assert!(outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.relative_path == "src/main.rs"));
        assert!(outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.relative_path == "logs/cargo-build-release.log"));
        assert_eq!(outcome.validation_status, "green");
        assert!(outcome.confidence >= 0.8);
    }

    #[test]
    fn requirements_analyst_requests_missing_calculator_scope() {
        let outcome = execute_bootstrap_request(&SpawnedBootstrapRequest {
            request_id: "req-analyst".into(),
            message: "Prepare a brief for a floating-point calculator in Rust.".into(),
            pack_id: Some("requirements_analyst".into()),
            task_profile: Some("fast_conversational".into()),
            max_history_messages: None,
            max_tool_iterations: None,
            compact_context: false,
            created_at: "2026-03-15T00:00:00Z".into(),
        });

        assert_eq!(outcome.status, "questions_pending");
        assert!(outcome
            .question_summary
            .expect("question summary")
            .contains("calculator details"));
    }

    #[test]
    fn market_ta_worker_returns_trade_setup() {
        let outcome = execute_bootstrap_request(&SpawnedBootstrapRequest {
            request_id: "req-market".into(),
            message: "ticker=BTCUSDT timeframe=4h horizon=3d exchange=Binance source=manual chart price=64000 support=61800 resistance=66800 rsi=58 macd=bullish-cross trend=bullish".into(),
            pack_id: Some("market_technical_analyst".into()),
            task_profile: Some("deep_reasoning".into()),
            max_history_messages: None,
            max_tool_iterations: None,
            compact_context: false,
            created_at: "2026-03-15T00:00:00Z".into(),
        });

        let result = outcome
            .result_markdown
            .expect("trade setup should be present");
        assert!(result.contains("Trade setup for BTCUSDT"));
        assert!(result.contains("Disclaimer"));
        assert_eq!(outcome.validation_status, "green");
        assert!(outcome.confidence >= 0.8);
    }
}
