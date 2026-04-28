use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_json::{json, Value};
use sparsekernel_core::{
    probe_browser_endpoint, AppendTranscriptEventInput, ArtifactStore, AuditInput, BrowserBroker,
    CapabilityCheck, CompleteToolCallInput, CreateToolCallInput, EnqueueTaskInput,
    GrantCapabilityInput, LedgerToolBroker, LocalSandboxBroker, MockBrowserBroker, SandboxBroker,
    SparseKernelDb, SparseKernelPaths, ToolBroker, UpsertSessionInput,
};
use std::error::Error;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use tiny_http::{Header, Response, Server};

#[derive(Debug, Parser)]
#[command(name = "sparsekernel")]
#[command(about = "SparseKernel local multi-agent kernel CLI")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init,
    Status(JsonFlag),
    Daemon(DaemonArgs),
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
    Tasks {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
}

#[derive(Debug, Args)]
struct JsonFlag {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[arg(long, default_value = "127.0.0.1:8765")]
    listen: String,
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    Migrate(JsonFlag),
    Inspect(JsonFlag),
}

#[derive(Debug, Subcommand)]
enum TaskCommand {
    Enqueue(TaskEnqueueArgs),
    List(TaskListArgs),
}

#[derive(Debug, Args)]
struct TaskEnqueueArgs {
    #[arg(long)]
    kind: String,
    #[arg(long, default_value_t = 0)]
    priority: i64,
    #[arg(long)]
    id: Option<String>,
}

#[derive(Debug, Args)]
struct TaskListArgs {
    #[arg(long, default_value_t = 100)]
    limit: i64,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
    List(AuditListArgs),
    Tail(AuditListArgs),
}

#[derive(Debug, Args)]
struct AuditListArgs {
    #[arg(long, default_value_t = 100)]
    limit: i64,
    #[arg(long)]
    json: bool,
}

pub fn run_cli() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init => {
            let db = SparseKernelDb::open_default()?;
            let inspect = db.inspect()?;
            println!("SparseKernel initialized at {}", inspect.path.display());
            println!("Schema version: {}", inspect.schema_version);
        }
        Command::Status(flag) => {
            let db = SparseKernelDb::open_default()?;
            write_status(&db, flag.json)?;
        }
        Command::Daemon(args) => run_daemon(&args.listen)?,
        Command::Db { command } => match command {
            DbCommand::Migrate(flag) => {
                let db = SparseKernelDb::open_default()?;
                db.migrate()?;
                write_status(&db, flag.json)?;
            }
            DbCommand::Inspect(flag) => {
                let db = SparseKernelDb::open_default()?;
                write_status(&db, flag.json)?;
            }
        },
        Command::Tasks { command } => match command {
            TaskCommand::Enqueue(args) => {
                let db = SparseKernelDb::open_default()?;
                let task = db.enqueue_task(EnqueueTaskInput {
                    id: args.id,
                    agent_id: None,
                    session_id: None,
                    kind: args.kind,
                    priority: args.priority,
                    idempotency_key: None,
                    input: None,
                })?;
                println!("{}", serde_json::to_string_pretty(&task)?);
            }
            TaskCommand::List(args) => {
                let db = SparseKernelDb::open_default()?;
                let tasks = db.list_tasks(args.limit)?;
                if args.json {
                    println!("{}", serde_json::to_string_pretty(&tasks)?);
                } else if tasks.is_empty() {
                    println!("No tasks.");
                } else {
                    for task in tasks {
                        println!(
                            "{}\t{}\t{}\tpriority={}",
                            task.id, task.status, task.kind, task.priority
                        );
                    }
                }
            }
        },
        Command::Audit { command } => {
            let args = match command {
                AuditCommand::List(args) | AuditCommand::Tail(args) => args,
            };
            let db = SparseKernelDb::open_default()?;
            let events = db.list_audit(args.limit)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&events)?);
            } else {
                for event in events.into_iter().rev() {
                    println!(
                        "{}\t{}\t{}\t{}",
                        event.id,
                        event.created_at,
                        event.action,
                        event.object_id.unwrap_or_default()
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn run_daemon_from_env() -> Result<(), Box<dyn Error>> {
    run_daemon("127.0.0.1:8765")
}

pub fn run_daemon(listen: &str) -> Result<(), Box<dyn Error>> {
    let addr = listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("invalid listen address: {listen}"))?;
    let server = Server::http(addr)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
    eprintln!("sparsekerneld listening on http://{addr}");
    for mut request in server.incoming_requests() {
        let mut body = Vec::new();
        request.as_reader().read_to_end(&mut body)?;
        let mut db = SparseKernelDb::open_default()?;
        let response =
            match handle_api_request(&mut db, request.method().as_str(), request.url(), &body) {
                Ok(reply) => json_response_with_status(&reply.body, reply.status_code)?,
                Err(err) => json_response_with_status(&json!({ "error": err.to_string() }), 400)?,
            };
        request.respond(response)?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct ApiReply {
    pub status_code: u16,
    pub body: Value,
}

#[derive(Debug, Deserialize)]
struct ClaimTaskRequest {
    worker_id: String,
    #[serde(default)]
    kinds: Vec<String>,
    lease_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ClaimTaskByIdRequest {
    task_id: String,
    worker_id: String,
    lease_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct HeartbeatTaskRequest {
    task_id: String,
    worker_id: String,
    lease_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CompleteTaskRequest {
    task_id: String,
    worker_id: String,
    result_artifact_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FailTaskRequest {
    task_id: String,
    worker_id: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseExpiredLeasesRequest {
    now: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RevokeCapabilityRequest {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ListCapabilitiesRequest {
    subject_type: String,
    subject_id: String,
}

#[derive(Debug, Deserialize)]
struct AcquireBrowserContextRequest {
    agent_id: Option<String>,
    session_id: Option<String>,
    task_id: Option<String>,
    trust_zone_id: String,
    max_contexts: Option<i64>,
    cdp_endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReleaseBrowserContextRequest {
    context_id: String,
}

#[derive(Debug, Deserialize)]
struct BrowserObservationRequest {
    context_id: String,
    target_id: Option<String>,
    observation_type: String,
    payload: Option<Value>,
    created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AllocateSandboxRequest {
    agent_id: Option<String>,
    task_id: Option<String>,
    trust_zone_id: String,
    backend: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReleaseSandboxRequest {
    allocation_id: String,
}

#[derive(Debug, Deserialize)]
struct ProbeBrowserPoolRequest {
    cdp_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallIdRequest {
    id: String,
}

#[derive(Debug, Deserialize)]
struct CompleteToolCallRequest {
    id: String,
    output: Option<Value>,
    #[serde(default)]
    artifact_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FailToolCallRequest {
    id: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct ListTranscriptEventsRequest {
    session_id: String,
    limit: Option<i64>,
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, Box<dyn Error>> {
    if body.is_empty() {
        return Err("request body is required".into());
    }
    Ok(serde_json::from_slice(body)?)
}

pub fn handle_api_request(
    db: &mut SparseKernelDb,
    method: &str,
    url: &str,
    body: &[u8],
) -> Result<ApiReply, Box<dyn Error>> {
    handle_api_request_with_artifact_root(db, method, url, body, None)
}

#[derive(Debug, Deserialize)]
struct ArtifactSubject {
    subject_type: String,
    subject_id: String,
    permission: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateArtifactRequest {
    content_base64: Option<String>,
    content_text: Option<String>,
    mime_type: Option<String>,
    retention_policy: Option<String>,
    subject: Option<ArtifactSubject>,
}

#[derive(Debug, Deserialize)]
struct ArtifactAccessRequest {
    id: String,
    subject: Option<ArtifactSubject>,
}

fn artifact_root_path(override_root: Option<&Path>) -> PathBuf {
    override_root
        .map(Path::to_path_buf)
        .unwrap_or_else(|| SparseKernelPaths::from_env().artifact_root)
}

fn decode_artifact_content(input: &CreateArtifactRequest) -> Result<Vec<u8>, Box<dyn Error>> {
    match (&input.content_base64, &input.content_text) {
        (Some(_), Some(_)) => Err("pass only one of content_base64 or content_text".into()),
        (Some(raw), None) => Ok(BASE64_STANDARD.decode(raw)?),
        (None, Some(text)) => Ok(text.as_bytes().to_vec()),
        (None, None) => Err("content_base64 or content_text is required".into()),
    }
}

fn require_artifact_capability(
    db: &SparseKernelDb,
    subject: &ArtifactSubject,
    artifact_id: Option<&str>,
    action: &str,
) -> Result<(), Box<dyn Error>> {
    let allowed = db.check_capability(CapabilityCheck {
        subject_type: subject.subject_type.clone(),
        subject_id: subject.subject_id.clone(),
        resource_type: "artifact".to_string(),
        resource_id: artifact_id.map(str::to_string),
        action: action.to_string(),
        context: None,
        audit_denied: true,
    })?;
    if allowed {
        return Ok(());
    }
    Err(format!(
        "{} {} lacks artifact {action} capability",
        subject.subject_type, subject.subject_id
    )
    .into())
}

pub fn handle_api_request_with_artifact_root(
    db: &mut SparseKernelDb,
    method: &str,
    url: &str,
    body: &[u8],
    artifact_root: Option<&Path>,
) -> Result<ApiReply, Box<dyn Error>> {
    let reply = match (method, url) {
        ("GET", "/health") => ApiReply {
            status_code: 200,
            body: json!({
                "ok": true,
                "service": "sparsekerneld",
                "version": env!("CARGO_PKG_VERSION"),
            }),
        },
        ("GET", "/status") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.inspect()?)?,
        },
        ("GET", "/tasks") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_tasks(100)?)?,
        },
        ("GET", "/sessions") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_sessions(100)?)?,
        },
        ("POST", "/sessions/upsert") => {
            let input: UpsertSessionInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.upsert_session(input)?)?,
            }
        }
        ("POST", "/transcript-events/append") => {
            let input: AppendTranscriptEventInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.append_transcript_event(input)?)?,
            }
        }
        ("POST", "/transcript-events/list") => {
            let input: ListTranscriptEventsRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(
                    db.list_transcript_events(&input.session_id, input.limit.unwrap_or(100))?,
                )?,
            }
        }
        ("GET", "/tool-calls") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_tool_calls(100)?)?,
        },
        ("POST", "/tool-calls/create") => {
            let input: CreateToolCallInput = parse_body(body)?;
            let broker = LedgerToolBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.create_call(input)?)?,
            }
        }
        ("POST", "/tool-calls/start") => {
            let input: ToolCallIdRequest = parse_body(body)?;
            let broker = LedgerToolBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.start_call(&input.id)?)?,
            }
        }
        ("POST", "/tool-calls/complete") => {
            let input: CompleteToolCallRequest = parse_body(body)?;
            let broker = LedgerToolBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.complete_call(
                    &input.id,
                    CompleteToolCallInput {
                        output: input.output,
                        artifact_ids: input.artifact_ids,
                    },
                )?)?,
            }
        }
        ("POST", "/tool-calls/fail") => {
            let input: FailToolCallRequest = parse_body(body)?;
            let broker = LedgerToolBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.fail_call(&input.id, &input.error)?)?,
            }
        }
        ("GET", "/browser/contexts") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_browser_contexts(100)?)?,
        },
        ("GET", "/browser/pools") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_browser_pools()?)?,
        },
        ("POST", "/browser/pools/probe") => {
            let input: ProbeBrowserPoolRequest = parse_body(body)?;
            let probe = probe_browser_endpoint(&input.cdp_endpoint);
            db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "browser_pool.probed".to_string(),
                object_type: Some("browser_pool".to_string()),
                object_id: None,
                payload: Some(json!({
                    "reachable": probe.reachable,
                    "statusCode": probe.status_code,
                    "loopbackOnly": true
                })),
            })?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(probe)?,
            }
        }
        ("POST", "/browser/contexts/acquire") => {
            let input: AcquireBrowserContextRequest = parse_body(body)?;
            let broker = MockBrowserBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.acquire_context(
                    input.agent_id.as_deref(),
                    input.session_id.as_deref(),
                    input.task_id.as_deref(),
                    &input.trust_zone_id,
                    input.max_contexts.unwrap_or(2),
                    input.cdp_endpoint.as_deref(),
                )?)?,
            }
        }
        ("POST", "/browser/contexts/release") => {
            let input: ReleaseBrowserContextRequest = parse_body(body)?;
            let broker = MockBrowserBroker { db };
            ApiReply {
                status_code: 200,
                body: json!({ "released": broker.release_context(&input.context_id)? }),
            }
        }
        ("POST", "/browser/contexts/observe") => {
            let input: BrowserObservationRequest = parse_body(body)?;
            db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "browser_context.observation".to_string(),
                object_type: Some("browser_context".to_string()),
                object_id: Some(input.context_id),
                payload: Some(json!({
                    "targetId": input.target_id,
                    "observationType": input.observation_type,
                    "payload": input.payload,
                    "observedAt": input.created_at,
                })),
            })?;
            ApiReply {
                status_code: 200,
                body: json!({ "ok": true }),
            }
        }
        ("POST", "/tasks/enqueue") => {
            let input: EnqueueTaskInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.enqueue_task(input)?)?,
            }
        }
        ("POST", "/tasks/claim") => {
            let input: ClaimTaskRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.claim_next_task(
                    &input.worker_id,
                    &input.kinds,
                    input.lease_seconds.unwrap_or(300),
                )?)?,
            }
        }
        ("POST", "/tasks/claim-id") => {
            let input: ClaimTaskByIdRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.claim_task(
                    &input.task_id,
                    &input.worker_id,
                    input.lease_seconds.unwrap_or(300),
                )?)?,
            }
        }
        ("POST", "/tasks/heartbeat") => {
            let input: HeartbeatTaskRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "ok": db.heartbeat_task(
                        &input.task_id,
                        &input.worker_id,
                        input.lease_seconds.unwrap_or(300),
                    )?,
                }),
            }
        }
        ("POST", "/tasks/complete") => {
            let input: CompleteTaskRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "ok": db.complete_task(
                        &input.task_id,
                        &input.worker_id,
                        input.result_artifact_id.as_deref(),
                    )?,
                }),
            }
        }
        ("POST", "/tasks/fail") => {
            let input: FailTaskRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "ok": db.fail_task(&input.task_id, &input.worker_id, &input.error)?,
                }),
            }
        }
        ("POST", "/sandbox/allocate") => {
            let input: AllocateSandboxRequest = parse_body(body)?;
            let broker = LocalSandboxBroker { db };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(broker.allocate_sandbox(
                    input.agent_id.as_deref(),
                    input.task_id.as_deref(),
                    &input.trust_zone_id,
                    input.backend.as_deref(),
                )?)?,
            }
        }
        ("POST", "/sandbox/release") => {
            let input: ReleaseSandboxRequest = parse_body(body)?;
            let broker = LocalSandboxBroker { db };
            ApiReply {
                status_code: 200,
                body: json!({ "released": broker.release_sandbox(&input.allocation_id)? }),
            }
        }
        ("POST", "/leases/release-expired") => {
            let input: ReleaseExpiredLeasesRequest = if body.is_empty() {
                ReleaseExpiredLeasesRequest { now: None }
            } else {
                parse_body(body)?
            };
            let now = input.now.unwrap_or_else(|| Utc::now().to_rfc3339());
            let (tasks, resources) = db.release_expired_leases(&now)?;
            ApiReply {
                status_code: 200,
                body: json!({ "tasks": tasks, "resources": resources }),
            }
        }
        ("POST", "/capabilities/grant") => {
            let input: GrantCapabilityInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.grant_capability(input)?)?,
            }
        }
        ("POST", "/capabilities/check") => {
            let input: CapabilityCheck = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: json!({ "allowed": db.check_capability(input)? }),
            }
        }
        ("POST", "/capabilities/revoke") => {
            let input: RevokeCapabilityRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: json!({ "revoked": db.revoke_capability(&input.id)? }),
            }
        }
        ("POST", "/capabilities/list") => {
            let input: ListCapabilitiesRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(
                    db.list_capabilities(&input.subject_type, &input.subject_id)?,
                )?,
            }
        }
        ("POST", "/artifacts/create") => {
            let input: CreateArtifactRequest = parse_body(body)?;
            let bytes = decode_artifact_content(&input)?;
            if let Some(subject) = &input.subject {
                require_artifact_capability(db, subject, None, "write")?;
            }
            let store = ArtifactStore::new(db, artifact_root_path(artifact_root));
            let subject = input.subject.as_ref().map(|subject| {
                (
                    subject.subject_type.as_str(),
                    subject.subject_id.as_str(),
                    subject.permission.as_deref().unwrap_or("read"),
                )
            });
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(store.write(
                    &bytes,
                    input.mime_type.as_deref(),
                    input.retention_policy.as_deref(),
                    subject,
                )?)?,
            }
        }
        ("POST", "/artifacts/read") => {
            let input: ArtifactAccessRequest = parse_body(body)?;
            if let Some(subject) = &input.subject {
                require_artifact_capability(db, subject, Some(&input.id), "read")?;
            }
            let store = ArtifactStore::new(db, artifact_root_path(artifact_root));
            let subject = input.subject.as_ref().map(|subject| {
                (
                    subject.subject_type.as_str(),
                    subject.subject_id.as_str(),
                    subject.permission.as_deref().unwrap_or("read"),
                )
            });
            let bytes = store.read(&input.id, subject)?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "artifact": db.get_artifact(&input.id)?,
                    "content_base64": BASE64_STANDARD.encode(bytes),
                }),
            }
        }
        ("POST", "/artifacts/metadata") => {
            let input: ArtifactAccessRequest = parse_body(body)?;
            if let Some(subject) = &input.subject {
                require_artifact_capability(db, subject, Some(&input.id), "read")?;
                if !db.has_artifact_access(
                    &input.id,
                    &subject.subject_type,
                    &subject.subject_id,
                    subject.permission.as_deref().unwrap_or("read"),
                )? {
                    db.record_audit(sparsekernel_core::AuditInput {
                        actor_type: Some(subject.subject_type.clone()),
                        actor_id: Some(subject.subject_id.clone()),
                        action: "artifact_access.denied".to_string(),
                        object_type: Some("artifact".to_string()),
                        object_id: Some(input.id.clone()),
                        payload: Some(json!({ "permission": subject.permission.as_deref().unwrap_or("read") })),
                    })?;
                    return Err(format!("artifact access denied: {}", input.id).into());
                }
            }
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.get_artifact(&input.id)?)?,
            }
        }
        ("GET", "/audit") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.list_audit(100)?)?,
        },
        _ => ApiReply {
            status_code: 404,
            body: json!({ "error": "not found" }),
        },
    };
    Ok(reply)
}

fn write_status(db: &SparseKernelDb, json: bool) -> Result<(), Box<dyn Error>> {
    let inspect = db.inspect()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&inspect)?);
        return Ok(());
    }
    println!("SparseKernel DB: {}", inspect.path.display());
    println!("Schema version: {}", inspect.schema_version);
    println!("Tables:");
    for (table, count) in inspect.counts {
        println!("  {table}: {count}");
    }
    let paths = SparseKernelPaths::from_env();
    println!("Artifact root: {}", paths.artifact_root.display());
    Ok(())
}

fn json_response_with_status(
    value: &serde_json::Value,
    status_code: u16,
) -> Result<Response<std::io::Cursor<Vec<u8>>>, Box<dyn Error>> {
    let body = serde_json::to_vec(value)?;
    let header = Header::from_bytes("content-type", "application/json")
        .map_err(|_| "failed to build content-type header")?;
    Ok(Response::from_data(body)
        .with_status_code(status_code)
        .with_header(header))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn json_call(db: &mut SparseKernelDb, method: &str, url: &str, body: Value) -> Value {
        handle_api_request(
            db,
            method,
            url,
            serde_json::to_string(&body).unwrap().as_bytes(),
        )
        .unwrap()
        .body
    }

    fn json_call_with_artifact_root(
        db: &mut SparseKernelDb,
        artifact_root: &Path,
        method: &str,
        url: &str,
        body: Value,
    ) -> Value {
        handle_api_request_with_artifact_root(
            db,
            method,
            url,
            serde_json::to_string(&body).unwrap().as_bytes(),
            Some(artifact_root),
        )
        .unwrap()
        .body
    }

    #[test]
    fn task_api_enqueues_claims_and_completes() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let task = json_call(
            &mut db,
            "POST",
            "/tasks/enqueue",
            json!({ "kind": "demo", "priority": 4 }),
        );
        let task_id = task["id"].as_str().unwrap().to_string();

        let claimed = json_call(
            &mut db,
            "POST",
            "/tasks/claim-id",
            json!({ "task_id": task_id, "worker_id": "worker-a", "lease_seconds": 60 }),
        );
        assert_eq!(claimed["id"], task_id);
        assert_eq!(claimed["lease_owner"], "worker-a");

        let empty_claim = json_call(
            &mut db,
            "POST",
            "/tasks/claim",
            json!({ "worker_id": "worker-b", "kinds": ["demo"] }),
        );
        assert!(empty_claim.is_null());

        let completed = json_call(
            &mut db,
            "POST",
            "/tasks/complete",
            json!({ "task_id": task_id, "worker_id": "worker-a" }),
        );
        assert_eq!(completed["ok"], true);
    }

    #[test]
    fn capability_api_grants_checks_lists_and_revokes() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let capability = json_call(
            &mut db,
            "POST",
            "/capabilities/grant",
            json!({
                "subject_type": "agent",
                "subject_id": "main",
                "resource_type": "tool",
                "resource_id": "browser",
                "action": "invoke",
            }),
        );
        let capability_id = capability["id"].as_str().unwrap().to_string();

        let check = json_call(
            &mut db,
            "POST",
            "/capabilities/check",
            json!({
                "subject_type": "agent",
                "subject_id": "main",
                "resource_type": "tool",
                "resource_id": "browser",
                "action": "invoke",
                "audit_denied": true,
            }),
        );
        assert_eq!(check["allowed"], true);

        let list = json_call(
            &mut db,
            "POST",
            "/capabilities/list",
            json!({ "subject_type": "agent", "subject_id": "main" }),
        );
        assert_eq!(list.as_array().unwrap().len(), 1);

        let revoked = json_call(
            &mut db,
            "POST",
            "/capabilities/revoke",
            json!({ "id": capability_id }),
        );
        assert_eq!(revoked["revoked"], true);
    }

    #[test]
    fn session_api_upserts_and_lists_sessions() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let session = json_call(
            &mut db,
            "POST",
            "/sessions/upsert",
            json!({
                "id": "session-a",
                "agent_id": "agent-a",
                "session_key": "agent:agent-a:main",
                "channel": "discord",
                "status": "active",
                "current_token_count": 12,
                "last_activity_at": "2026-04-27T00:00:00Z",
            }),
        );
        assert_eq!(session["id"], "session-a");
        assert_eq!(session["agent_id"], "agent-a");
        assert_eq!(session["current_token_count"], 12);

        let sessions = handle_api_request(&mut db, "GET", "/sessions", &[])
            .unwrap()
            .body;
        assert_eq!(sessions.as_array().unwrap().len(), 1);

        let event = json_call(
            &mut db,
            "POST",
            "/transcript-events/append",
            json!({
                "session_id": "session-a",
                "role": "user",
                "event_type": "message",
                "content": { "text": "hi" },
            }),
        );
        assert_eq!(event["seq"], 1);
        let events = json_call(
            &mut db,
            "POST",
            "/transcript-events/list",
            json!({ "session_id": "session-a" }),
        );
        assert_eq!(events.as_array().unwrap().len(), 1);
    }

    #[test]
    fn tool_call_api_tracks_lifecycle_artifacts_and_denials() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let artifact_root = tempfile::tempdir().unwrap();
        let artifact = ArtifactStore::new(&db, artifact_root.path())
            .write(b"pixels", Some("image/png"), Some("debug"), None)
            .unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "tool".to_string(),
            resource_id: Some("browser.capture".to_string()),
            action: "invoke".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();

        let denied = handle_api_request(
            &mut db,
            "POST",
            "/tool-calls/create",
            serde_json::to_string(&json!({
                "agent_id": "main",
                "tool_name": "exec",
            }))
            .unwrap()
            .as_bytes(),
        );
        assert!(denied.is_err());

        let created = json_call(
            &mut db,
            "POST",
            "/tool-calls/create",
            json!({
                "id": "tool-call-a",
                "agent_id": "main",
                "tool_name": "browser.capture",
                "input": { "url": "https://example.com" },
            }),
        );
        assert_eq!(created["status"], "created");

        let started = json_call(
            &mut db,
            "POST",
            "/tool-calls/start",
            json!({ "id": "tool-call-a" }),
        );
        assert_eq!(started["status"], "running");

        let completed = json_call(
            &mut db,
            "POST",
            "/tool-calls/complete",
            json!({
                "id": "tool-call-a",
                "output": { "ok": true },
                "artifact_ids": [artifact.id.clone()],
            }),
        );
        assert_eq!(completed["status"], "completed");
        assert_eq!(completed["output"]["artifact_ids"][0], artifact.id);

        let calls = handle_api_request(&mut db, "GET", "/tool-calls", &[])
            .unwrap()
            .body;
        assert_eq!(calls.as_array().unwrap().len(), 1);
        let actions: Vec<String> = db
            .list_audit(10)
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"tool_call.denied".to_string()));
        assert!(actions.contains(&"tool_call.completed".to_string()));
    }

    #[test]
    fn browser_api_acquires_lists_and_releases_contexts() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        db.enqueue_task(EnqueueTaskInput {
            id: Some("task-a".to_string()),
            agent_id: None,
            session_id: None,
            kind: "browser".to_string(),
            priority: 0,
            idempotency_key: None,
            input: None,
        })
        .unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "browser_context".to_string(),
            resource_id: Some("public_web".to_string()),
            action: "allocate".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();

        let context = json_call(
            &mut db,
            "POST",
            "/browser/contexts/acquire",
            json!({
                "agent_id": "main",
                "task_id": "task-a",
                "trust_zone_id": "public_web",
                "max_contexts": 1,
                "cdp_endpoint": "http://127.0.0.1:9222",
            }),
        );
        let context_id = context["id"].as_str().unwrap().to_string();
        assert_eq!(context["status"], "active");

        let observed = json_call(
            &mut db,
            "POST",
            "/browser/contexts/observe",
            json!({
                "context_id": context_id,
                "target_id": "target-1",
                "observation_type": "browser_console",
                "payload": { "text": "hello" },
                "created_at": "2026-04-28T00:00:00Z",
            }),
        );
        assert_eq!(observed["ok"], true);
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "browser_context.observation");
        assert_eq!(audit[0].object_id.as_deref(), Some(context_id.as_str()));
        assert_eq!(
            audit[0].payload.as_ref().unwrap()["observationType"],
            "browser_console"
        );
        assert_eq!(audit[0].payload.as_ref().unwrap()["targetId"], "target-1");

        let denied = handle_api_request(
            &mut db,
            "POST",
            "/browser/contexts/acquire",
            serde_json::to_string(&json!({
                "agent_id": "main",
                "trust_zone_id": "public_web",
                "max_contexts": 1,
            }))
            .unwrap()
            .as_bytes(),
        );
        assert!(denied.is_err());

        let contexts = handle_api_request(&mut db, "GET", "/browser/contexts", &[])
            .unwrap()
            .body;
        assert_eq!(contexts.as_array().unwrap().len(), 1);

        let pools = handle_api_request(&mut db, "GET", "/browser/pools", &[])
            .unwrap()
            .body;
        assert_eq!(pools[0]["trust_zone_id"], "public_web");
        assert_eq!(pools[0]["browser_kind"], "cdp");
        assert_eq!(pools[0]["cdp_endpoint"], "http://127.0.0.1:9222");

        let released = json_call(
            &mut db,
            "POST",
            "/browser/contexts/release",
            json!({ "context_id": context_id }),
        );
        assert_eq!(released["released"], true);
        let contexts = handle_api_request(&mut db, "GET", "/browser/contexts", &[])
            .unwrap()
            .body;
        assert_eq!(contexts[0]["status"], "released");
    }

    #[test]
    fn sandbox_api_allocates_and_releases_with_capability() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        db.enqueue_task(EnqueueTaskInput {
            id: Some("task-sandbox".to_string()),
            agent_id: None,
            session_id: None,
            kind: "sandbox".to_string(),
            priority: 0,
            idempotency_key: None,
            input: None,
        })
        .unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "sandbox".to_string(),
            resource_id: Some("code_execution".to_string()),
            action: "allocate".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();

        let allocation = json_call(
            &mut db,
            "POST",
            "/sandbox/allocate",
            json!({
                "agent_id": "main",
                "task_id": "task-sandbox",
                "trust_zone_id": "code_execution",
                "backend": "local/no_isolation",
            }),
        );
        let allocation_id = allocation["id"].as_str().unwrap().to_string();
        assert_eq!(allocation["backend"], "local/no_isolation");

        let released = json_call(
            &mut db,
            "POST",
            "/sandbox/release",
            json!({ "allocation_id": allocation_id }),
        );
        assert_eq!(released["released"], true);
    }

    #[test]
    fn browser_probe_api_checks_loopback_cdp_endpoint() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"{"Browser":"Chrome/123.0","webSocketDebuggerUrl":"ws://127.0.0.1/devtools/browser/test"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let probe = json_call(
            &mut db,
            "POST",
            "/browser/pools/probe",
            json!({ "cdp_endpoint": format!("http://{addr}") }),
        );
        server.join().unwrap();

        assert_eq!(probe["reachable"], true);
        assert_eq!(probe["browser"], "Chrome/123.0");
        assert_eq!(probe["status_code"], 200);
        assert_eq!(db.list_audit(1).unwrap()[0].action, "browser_pool.probed");
    }

    #[test]
    fn artifact_api_creates_reads_and_checks_access() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let root = tempfile::tempdir().unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "artifact".to_string(),
            resource_id: None,
            action: "write".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();

        let created = json_call_with_artifact_root(
            &mut db,
            root.path(),
            "POST",
            "/artifacts/create",
            json!({
                "content_text": "hello",
                "mime_type": "text/plain",
                "retention_policy": "session",
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }),
        );
        let artifact_id = created["id"].as_str().unwrap().to_string();
        let read_without_capability = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/read",
            serde_json::to_string(&json!({
                "id": artifact_id,
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }))
            .unwrap()
            .as_bytes(),
            Some(root.path()),
        );
        assert!(read_without_capability.is_err());

        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "artifact".to_string(),
            resource_id: Some(artifact_id.clone()),
            action: "read".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();
        let metadata = json_call_with_artifact_root(
            &mut db,
            root.path(),
            "POST",
            "/artifacts/metadata",
            json!({
                "id": artifact_id,
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }),
        );
        assert_eq!(metadata["mime_type"], "text/plain");

        let read = json_call_with_artifact_root(
            &mut db,
            root.path(),
            "POST",
            "/artifacts/read",
            json!({
                "id": metadata["id"],
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }),
        );
        let decoded = BASE64_STANDARD
            .decode(read["content_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, b"hello");
    }
}
