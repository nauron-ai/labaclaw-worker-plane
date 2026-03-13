use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    env.sort_by(|left, right| {
        left["name"]
            .as_str()
            .cmp(&right["name"].as_str())
    });

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

pub fn render_deployment_yaml(spec: &DedicatedAgentDeploymentSpec, service_account: &str, runtime_secret_name: &str) -> Result<String> {
    serde_yaml::to_string(&build_deployment_manifest(spec, service_account, runtime_secret_name))
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

pub fn execute_bootstrap_message(message: &str) -> BootstrapExecutionOutcome {
    let revenue = extract_named_number(message, "revenue");
    let costs = extract_named_number(message, "costs").or_else(|| extract_named_number(message, "cost"));
    if let (Some(revenue), Some(costs)) = (revenue, costs) {
        if revenue > 0.0 {
            let margin = ((revenue - costs) / revenue) * 100.0;
            let result = format!(
                "RESULT FOR ORCHESTRATOR\nMargin is {:.1}%.\nRevenue: {:.2}\nCosts: {:.2}\nComputed by dedicated worker-plane finance validator.",
                margin, revenue, costs
            );
            return BootstrapExecutionOutcome {
                summary: Some(format!("Margin computed at {:.1}%", margin)),
                result_markdown: Some(result),
                question_markdown: None,
                question_summary: None,
            };
        }
    }

    if message.to_ascii_lowercase().contains("margin") {
        return BootstrapExecutionOutcome {
            result_markdown: None,
            summary: None,
            question_markdown: Some(
                "QUESTION FOR ORCHESTRATOR\nRevenue and costs are required to compute margin. Please provide both values or attach the source document."
                    .into(),
            ),
            question_summary: Some("Missing revenue/cost inputs required for margin calculation".into()),
        };
    }

    let result = format!(
        "RESULT FOR ORCHESTRATOR\nBootstrap mission executed.\nSummary: {}",
        summarize_text_for_event(message)
    );
    BootstrapExecutionOutcome {
        summary: Some(summarize_text_for_event(&result)),
        result_markdown: Some(result),
        question_markdown: None,
        question_summary: None,
    }
}

pub fn spawn_failed_event(agent_id: &str, request_id: Option<String>, error: impl Into<String>) -> AgentSpawnFailedEvent {
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

pub fn heartbeat_event(agent_id: &str, workload_name: &str, service_state: &str) -> AgentHeartbeatEvent {
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
    summary: impl Into<String>,
) -> AgentCompleted {
    AgentCompleted {
        event_id: uuid::Uuid::new_v4().to_string(),
        agent_id: agent_id.to_string(),
        request_id: request_id.to_string(),
        result_ref: result_ref.into(),
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
            spec_ref: "s3://laba-artifacts/labaclaw/specs/financial-analyst-1/v1/agent-spec.json".into(),
            bootstrap_ref: "s3://laba-artifacts/labaclaw/bootstrap/financial-analyst-1/req-1/request.json".into(),
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
        let yaml = render_deployment_yaml(&spec, "labaclaw-worker-plane", "labaclaw-worker-plane-runtime").unwrap();
        assert!(yaml.contains("LABACLAW_AGENT_SPEC_REF"));
        assert!(yaml.contains("LABACLAW_WORKER_PLANE_REDPANDA_BROKERS"));
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
}
