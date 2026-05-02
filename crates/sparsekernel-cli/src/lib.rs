use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_json::{json, Value};
use sparsekernel_core::{
    probe_browser_endpoint, probe_sandbox_backends, AppendTranscriptEventInput, ArtifactStore,
    AuditInput, BrowserBroker, BrowserContextAcquireInput, CapabilityCheck, CompleteToolCallInput,
    CreateToolCallInput, EnqueueTaskInput, GrantCapabilityInput, LedgerToolBroker,
    ListBrowserObservationsInput, ListBrowserTargetsInput, LocalSandboxBroker, MockBrowserBroker,
    RecordBrowserObservationInput, RecordBrowserTargetInput, ResourceBudgetUpdateInput,
    SandboxAllocateInput, SandboxBroker, SparseKernelDb, SparseKernelPaths, ToolBroker,
    UpsertSessionInput, SPARSEKERNEL_PROTOCOL_VERSION,
};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv6Addr, Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{env, io};
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

#[derive(Debug, Parser)]
#[command(name = "sparsekerneld")]
#[command(about = "Run the SparseKernel local daemon")]
struct DaemonCli {
    #[arg(long, default_value = "127.0.0.1:8765")]
    listen: String,
}

pub fn run_daemon_cli() -> Result<(), Box<dyn Error>> {
    let args = DaemonCli::parse();
    run_daemon(&args.listen)
}

pub fn run_daemon(listen: &str) -> Result<(), Box<dyn Error>> {
    let addr = listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("invalid listen address: {listen}"))?;
    let server = Server::http(addr).map_err(|err| std::io::Error::other(err.to_string()))?;
    eprintln!("sparsekerneld listening on http://{addr}");
    let mut daemon_state = DaemonState::default();
    for mut request in server.incoming_requests() {
        let mut body = Vec::new();
        request.as_reader().read_to_end(&mut body)?;
        let mut db = SparseKernelDb::open_default()?;
        let response = match handle_api_request_with_daemon_state(
            &mut db,
            request.method().as_str(),
            request.url(),
            &body,
            None,
            &mut daemon_state,
        ) {
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

#[derive(Default)]
pub struct DaemonState {
    egress_proxies: HashMap<String, SupervisedEgressProxyProcess>,
}

struct SupervisedEgressProxyProcess {
    trust_zone_id: String,
    proxy_ref: String,
    host: String,
    port: u16,
    pid: Option<u32>,
    child: Option<Child>,
    shutdown: Option<Arc<AtomicBool>>,
    thread: Option<JoinHandle<()>>,
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
    allowed_origins: Option<Value>,
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
struct CloseBrowserTargetRequest {
    context_id: String,
    target_id: String,
    reason: Option<String>,
    closed_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AllocateSandboxRequest {
    agent_id: Option<String>,
    task_id: Option<String>,
    trust_zone_id: String,
    backend: Option<String>,
    docker_image: Option<String>,
    max_runtime_ms: Option<i64>,
    max_bytes_out: Option<i64>,
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

#[derive(Debug, Deserialize)]
struct TrustZoneNetworkPolicyRequest {
    trust_zone_id: String,
}

#[derive(Debug, Deserialize)]
struct AttachTrustZoneProxyRequest {
    trust_zone_id: String,
    proxy_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StartEgressProxyRequest {
    trust_zone_id: String,
    host: Option<String>,
    port: Option<u16>,
    mode: Option<String>,
    command: Option<String>,
    args: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct StopEgressProxyRequest {
    trust_zone_id: String,
    clear_proxy_ref: Option<bool>,
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
    let mut daemon_state = DaemonState::default();
    handle_api_request_with_daemon_state(db, method, url, body, None, &mut daemon_state)
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
struct ImportArtifactFileRequest {
    staged_path: String,
    mime_type: Option<String>,
    retention_policy: Option<String>,
    subject: Option<ArtifactSubject>,
}

#[derive(Debug, Deserialize)]
struct ArtifactAccessRequest {
    id: String,
    subject: Option<ArtifactSubject>,
}

#[derive(Debug, Deserialize)]
struct ExportArtifactFileRequest {
    id: String,
    file_name: Option<String>,
    subject: Option<ArtifactSubject>,
}

fn artifact_root_path(override_root: Option<&Path>) -> PathBuf {
    override_root
        .map(Path::to_path_buf)
        .unwrap_or_else(|| SparseKernelPaths::from_env().artifact_root)
}

fn artifact_staging_root(artifact_root: &Path) -> PathBuf {
    env::var_os("SPARSEKERNEL_ARTIFACT_STAGING_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| artifact_root.join(".staging"))
}

fn artifact_export_root(artifact_root: &Path) -> PathBuf {
    env::var_os("SPARSEKERNEL_ARTIFACT_EXPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| artifact_root.join(".exports"))
}

fn base64_artifact_compat_disabled() -> bool {
    matches!(
        env::var("SPARSEKERNEL_ARTIFACT_BASE64")
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("0" | "false" | "off" | "disabled" | "deny")
    ) || matches!(
        env::var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS")
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

fn base64_artifact_compat_max_bytes() -> Option<usize> {
    let raw = env::var("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES").ok();
    let Some(value) = raw
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Some(1024 * 1024);
    };
    let normalized = value.to_ascii_lowercase();
    if matches!(normalized.as_str(), "unlimited" | "none" | "off") {
        return None;
    }
    value
        .parse::<usize>()
        .ok()
        .map(Some)
        .unwrap_or(Some(1024 * 1024))
}

fn enforce_base64_artifact_compat_size(
    bytes_len: usize,
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    let Some(max_bytes) = base64_artifact_compat_max_bytes() else {
        return Ok(());
    };
    if bytes_len <= max_bytes {
        return Ok(());
    }
    Err(format!(
        "base64 artifact {operation} is limited to {max_bytes} bytes; use /artifacts/import-file or /artifacts/export-file"
    )
    .into())
}

fn canonical_child_path(root: &Path, raw_path: &str) -> Result<PathBuf, Box<dyn Error>> {
    fs::create_dir_all(root)?;
    let canonical_root = root.canonicalize()?;
    let candidate = Path::new(raw_path);
    let path = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        root.join(candidate)
    };
    let canonical_path = path.canonicalize()?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(format!(
            "artifact file path must be inside {}",
            canonical_root.display()
        )
        .into());
    }
    Ok(canonical_path)
}

fn sanitize_export_file_name(raw: Option<&str>, fallback: &str) -> String {
    let candidate = raw.unwrap_or(fallback).trim();
    let sanitized: String = candidate
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches('.');
    if trimmed.is_empty() || trimmed == ".." {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn artifact_export_path(root: &Path, artifact_id: &str, file_name: Option<&str>) -> PathBuf {
    let safe_name = sanitize_export_file_name(file_name, artifact_id);
    root.join(safe_name)
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

fn normalize_proxy_host(host: Option<String>) -> Result<String, Box<dyn Error>> {
    let host = host.unwrap_or_else(|| "127.0.0.1".to_string());
    match host.as_str() {
        "127.0.0.1" | "localhost" | "::1" => Ok(host),
        _ => Err("egress proxy supervisor only binds loopback hosts".into()),
    }
}

fn allocate_proxy_port(host: &str, port: Option<u16>) -> Result<u16, Box<dyn Error>> {
    if let Some(port) = port {
        return Ok(port);
    }
    let listener = TcpListener::bind((host, 0))?;
    Ok(listener.local_addr()?.port())
}

fn default_proxy_command_args(trust_zone_id: &str, host: &str, port: u16) -> Vec<String> {
    vec![
        "runtime".to_string(),
        "egress-proxy".to_string(),
        "--trust-zone".to_string(),
        trust_zone_id.to_string(),
        "--host".to_string(),
        host.to_string(),
        "--port".to_string(),
        port.to_string(),
        "--attach".to_string(),
        "--json".to_string(),
    ]
}

fn proxy_command_args(input: &StartEgressProxyRequest, host: &str, port: u16) -> Vec<String> {
    input
        .args
        .clone()
        .unwrap_or_else(|| default_proxy_command_args(&input.trust_zone_id, host, port))
        .into_iter()
        .map(|arg| {
            arg.replace("{trust_zone}", &input.trust_zone_id)
                .replace("{host}", host)
                .replace("{port}", &port.to_string())
        })
        .collect()
}

fn should_start_command_proxy(input: &StartEgressProxyRequest) -> bool {
    matches!(
        input.mode.as_deref().map(str::trim).map(str::to_ascii_lowercase),
        Some(mode) if mode == "command" || mode == "child" || mode == "external"
    ) || input.command.is_some()
        || env::var("SPARSEKERNEL_EGRESS_PROXY_COMMAND").is_ok()
}

fn spawn_command_egress_proxy(
    db: &mut SparseKernelDb,
    daemon_state: &mut DaemonState,
    input: StartEgressProxyRequest,
    host: String,
) -> Result<Value, Box<dyn Error>> {
    let port = allocate_proxy_port(&host, input.port)?;
    let proxy_ref = format!("http://{host}:{port}/");
    let command = input
        .command
        .clone()
        .or_else(|| env::var("SPARSEKERNEL_EGRESS_PROXY_COMMAND").ok())
        .unwrap_or_else(|| "openclaw".to_string());
    let args = proxy_command_args(&input, &host, port);
    let mut child = ProcessCommand::new(&command)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let pid = child.id();
    std::thread::sleep(Duration::from_millis(100));
    if let Some(status) = child.try_wait()? {
        return Err(format!("egress proxy command exited during startup: {status}").into());
    }
    db.attach_network_policy_proxy_to_trust_zone(&input.trust_zone_id, Some(proxy_ref.clone()))?;
    db.record_audit(AuditInput {
        actor_type: Some("runtime".to_string()),
        actor_id: None,
        action: "egress_proxy.daemon_supervised_started".to_string(),
        object_type: Some("trust_zone".to_string()),
        object_id: Some(input.trust_zone_id.clone()),
        payload: Some(json!({
            "mode": "command",
            "proxyRef": proxy_ref,
            "pid": pid,
            "command": command,
            "args": args,
        })),
    })?;
    daemon_state.egress_proxies.insert(
        input.trust_zone_id.clone(),
        SupervisedEgressProxyProcess {
            trust_zone_id: input.trust_zone_id.clone(),
            proxy_ref: proxy_ref.clone(),
            host,
            port,
            pid: Some(pid),
            child: Some(child),
            shutdown: None,
            thread: None,
        },
    );
    Ok(json!({
        "trust_zone_id": input.trust_zone_id,
        "proxy_ref": proxy_ref,
        "pid": pid,
        "mode": "command",
        "already_running": false,
    }))
}

fn start_builtin_egress_proxy(
    db: &mut SparseKernelDb,
    daemon_state: &mut DaemonState,
    input: StartEgressProxyRequest,
    host: String,
) -> Result<Value, Box<dyn Error>> {
    let listener = TcpListener::bind((host.as_str(), input.port.unwrap_or(0)))?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    let proxy_ref = format!("http://{host}:{port}/");
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread_shutdown = shutdown.clone();
    let trust_zone_id = input.trust_zone_id.clone();
    let handle = thread::spawn(move || {
        while !thread_shutdown.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let trust_zone_id = trust_zone_id.clone();
                    thread::spawn(move || {
                        let _ = handle_builtin_proxy_connection(&trust_zone_id, stream);
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => break,
            }
        }
    });
    db.attach_network_policy_proxy_to_trust_zone(&input.trust_zone_id, Some(proxy_ref.clone()))?;
    db.record_audit(AuditInput {
        actor_type: Some("runtime".to_string()),
        actor_id: None,
        action: "egress_proxy.daemon_supervised_started".to_string(),
        object_type: Some("trust_zone".to_string()),
        object_id: Some(input.trust_zone_id.clone()),
        payload: Some(json!({
            "mode": "builtin",
            "proxyRef": proxy_ref,
            "host": host,
            "port": port,
        })),
    })?;
    daemon_state.egress_proxies.insert(
        input.trust_zone_id.clone(),
        SupervisedEgressProxyProcess {
            trust_zone_id: input.trust_zone_id.clone(),
            proxy_ref: proxy_ref.clone(),
            host,
            port,
            pid: None,
            child: None,
            shutdown: Some(shutdown),
            thread: Some(handle),
        },
    );
    Ok(json!({
        "trust_zone_id": input.trust_zone_id,
        "proxy_ref": proxy_ref,
        "mode": "builtin",
        "already_running": false,
    }))
}

fn start_supervised_egress_proxy(
    db: &mut SparseKernelDb,
    daemon_state: &mut DaemonState,
    input: StartEgressProxyRequest,
) -> Result<Value, Box<dyn Error>> {
    prune_exited_egress_proxies(daemon_state);
    if let Some(existing) = daemon_state.egress_proxies.get(&input.trust_zone_id) {
        return Ok(json!({
            "trust_zone_id": existing.trust_zone_id,
            "proxy_ref": existing.proxy_ref,
            "pid": existing.pid,
            "mode": if existing.child.is_some() { "command" } else { "builtin" },
            "already_running": true,
        }));
    }
    let host = normalize_proxy_host(input.host.clone())?;
    if should_start_command_proxy(&input) {
        spawn_command_egress_proxy(db, daemon_state, input, host)
    } else {
        start_builtin_egress_proxy(db, daemon_state, input, host)
    }
}

fn stop_supervised_egress_proxy(
    db: &mut SparseKernelDb,
    daemon_state: &mut DaemonState,
    input: StopEgressProxyRequest,
) -> Result<Value, Box<dyn Error>> {
    let Some(mut process) = daemon_state.egress_proxies.remove(&input.trust_zone_id) else {
        return Ok(json!({ "trust_zone_id": input.trust_zone_id, "stopped": false }));
    };
    if let Some(shutdown) = &process.shutdown {
        shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect((process.host.as_str(), process.port));
    }
    if let Some(child) = process.child.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
    if let Some(handle) = process.thread.take() {
        let _ = handle.join();
    }
    if input.clear_proxy_ref.unwrap_or(false) {
        db.attach_network_policy_proxy_to_trust_zone(&input.trust_zone_id, None)?;
    }
    db.record_audit(AuditInput {
        actor_type: Some("runtime".to_string()),
        actor_id: None,
        action: "egress_proxy.daemon_supervised_stopped".to_string(),
        object_type: Some("trust_zone".to_string()),
        object_id: Some(input.trust_zone_id.clone()),
        payload: Some(json!({
            "pid": process.pid,
            "mode": if process.child.is_some() { "command" } else { "builtin" },
            "clearProxyRef": input.clear_proxy_ref.unwrap_or(false),
        })),
    })?;
    Ok(json!({
        "trust_zone_id": input.trust_zone_id,
        "proxy_ref": process.proxy_ref,
        "pid": process.pid,
        "mode": if process.child.is_some() { "command" } else { "builtin" },
        "stopped": true,
    }))
}

fn list_supervised_egress_proxies(daemon_state: &mut DaemonState) -> Value {
    prune_exited_egress_proxies(daemon_state);
    json!(daemon_state
        .egress_proxies
        .values()
        .map(|process| {
            json!({
                "trust_zone_id": process.trust_zone_id,
                "proxy_ref": process.proxy_ref,
                "pid": process.pid,
                "mode": if process.child.is_some() { "command" } else { "builtin" },
            })
        })
        .collect::<Vec<_>>())
}

fn prune_exited_egress_proxies(daemon_state: &mut DaemonState) {
    daemon_state.egress_proxies.retain(|_, process| {
        if let Some(child) = process.child.as_mut() {
            return matches!(child.try_wait(), Ok(None));
        }
        !process.thread.as_ref().is_some_and(JoinHandle::is_finished)
    });
}

#[derive(Debug)]
struct ParsedProxyRequest {
    method: String,
    version: String,
    target_url: String,
    host: String,
    port: u16,
    origin_form: String,
}

fn handle_builtin_proxy_connection(trust_zone_id: &str, mut client: TcpStream) -> io::Result<()> {
    let (header_bytes, body_tail) = read_http_header(&mut client)?;
    let header = String::from_utf8_lossy(&header_bytes);
    let Some(first_line) = header.lines().next() else {
        return write_proxy_response(&mut client, 400, "Bad Request", "missing request line");
    };
    if first_line.starts_with("CONNECT ") {
        return handle_builtin_connect_proxy(trust_zone_id, client, first_line, &body_tail);
    }
    let parsed = match parse_proxy_request(first_line, &header) {
        Ok(parsed) => parsed,
        Err(message) => {
            return write_proxy_response(&mut client, 400, "Bad Request", &message);
        }
    };
    if let Err(reason) = enforce_builtin_proxy_policy(trust_zone_id, &parsed) {
        let _ = record_builtin_proxy_denial(trust_zone_id, &parsed.target_url, &reason);
        return write_proxy_response(
            &mut client,
            403,
            "Forbidden",
            &format!("SparseKernel egress denied: {reason}"),
        );
    }
    let mut upstream = TcpStream::connect((parsed.host.as_str(), parsed.port))?;
    let request_head = rewrite_proxy_request_header(&parsed, &header);
    upstream.write_all(request_head.as_bytes())?;
    upstream.write_all(&body_tail)?;
    let mut upstream_for_body = upstream.try_clone()?;
    let mut client_for_body = client.try_clone()?;
    thread::spawn(move || {
        let _ = io::copy(&mut client_for_body, &mut upstream_for_body);
        let _ = upstream_for_body.shutdown(Shutdown::Write);
    });
    io::copy(&mut upstream, &mut client)?;
    Ok(())
}

fn handle_builtin_connect_proxy(
    trust_zone_id: &str,
    mut client: TcpStream,
    first_line: &str,
    body_tail: &[u8],
) -> io::Result<()> {
    let parts = first_line.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 3 {
        return write_proxy_response(&mut client, 400, "Bad Request", "invalid CONNECT request");
    }
    let (host, port) = parse_host_port(parts[1], 443).map_err(io::Error::other)?;
    let parsed = ParsedProxyRequest {
        method: "CONNECT".to_string(),
        version: parts[2].to_string(),
        target_url: format!("https://{}:{port}/", format_host_for_url(&host)),
        host,
        port,
        origin_form: String::new(),
    };
    if let Err(reason) = enforce_builtin_proxy_policy(trust_zone_id, &parsed) {
        let _ = record_builtin_proxy_denial(trust_zone_id, &parsed.target_url, &reason);
        return write_proxy_response(
            &mut client,
            403,
            "Forbidden",
            &format!("SparseKernel egress denied: {reason}"),
        );
    }
    let mut upstream = TcpStream::connect((parsed.host.as_str(), parsed.port))?;
    client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")?;
    if !body_tail.is_empty() {
        upstream.write_all(body_tail)?;
    }
    let mut upstream_for_client = upstream.try_clone()?;
    let mut client_for_upstream = client.try_clone()?;
    thread::spawn(move || {
        let _ = io::copy(&mut client_for_upstream, &mut upstream_for_client);
        let _ = upstream_for_client.shutdown(Shutdown::Write);
    });
    io::copy(&mut upstream, &mut client)?;
    Ok(())
}

fn read_http_header(stream: &mut TcpStream) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if bytes.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy request header too large",
            ));
        }
    }
    let Some(split_at) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "proxy request header incomplete",
        ));
    };
    let body_start = split_at + 4;
    Ok((bytes[..body_start].to_vec(), bytes[body_start..].to_vec()))
}

fn parse_proxy_request(first_line: &str, header: &str) -> Result<ParsedProxyRequest, String> {
    let parts = first_line.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 3 {
        return Err("invalid request line".to_string());
    }
    let method = parts[0].to_string();
    let target = parts[1];
    let version = parts[2].to_string();
    if let Some(parsed) = parse_absolute_http_url(target)? {
        return Ok(ParsedProxyRequest {
            method,
            version,
            target_url: target.to_string(),
            host: parsed.0,
            port: parsed.1,
            origin_form: parsed.2,
        });
    }
    let Some(host_header) = proxy_header_value(header, "host") else {
        return Err("relative proxy request requires Host header".to_string());
    };
    let (host, port) = parse_host_port(host_header, 80)?;
    Ok(ParsedProxyRequest {
        method,
        version,
        target_url: format!("http://{}:{port}{target}", format_host_for_url(&host)),
        host,
        port,
        origin_form: target.to_string(),
    })
}

fn parse_absolute_http_url(raw: &str) -> Result<Option<(String, u16, String)>, String> {
    let Some(rest) = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
    else {
        return Ok(None);
    };
    let default_port = if raw.starts_with("https://") { 443 } else { 80 };
    let slash_index = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..slash_index];
    let path = if slash_index < rest.len() {
        &rest[slash_index..]
    } else {
        "/"
    };
    let (host, port) = parse_host_port(authority, default_port)?;
    Ok(Some((host, port, path.to_string())))
}

fn parse_host_port(raw: &str, default_port: u16) -> Result<(String, u16), String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("missing host".to_string());
    }
    if let Some(rest) = value.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err("invalid IPv6 host".to_string());
        };
        let host = rest[..end].to_string();
        let port = rest[end + 1..]
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Ok((host, port));
    }
    if let Some((host, port)) = value.rsplit_once(':') {
        if !host.contains(':') {
            return Ok((
                host.to_string(),
                port.parse::<u16>().unwrap_or(default_port),
            ));
        }
    }
    Ok((value.to_string(), default_port))
}

fn proxy_header_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    for line in header.lines().skip(1) {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case(name) {
            return Some(value.trim());
        }
    }
    None
}

fn rewrite_proxy_request_header(parsed: &ParsedProxyRequest, header: &str) -> String {
    let mut output = format!(
        "{} {} {}\r\n",
        parsed.method, parsed.origin_form, parsed.version
    );
    for line in header.lines().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let Some((key, _value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        if key.eq_ignore_ascii_case("proxy-connection")
            || key.eq_ignore_ascii_case("connection")
            || key.eq_ignore_ascii_case("keep-alive")
            || key.eq_ignore_ascii_case("transfer-encoding")
        {
            continue;
        }
        output.push_str(line);
        output.push_str("\r\n");
    }
    output.push_str("Connection: close\r\n\r\n");
    output
}

fn write_proxy_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())
}

fn enforce_builtin_proxy_policy(
    trust_zone_id: &str,
    request: &ParsedProxyRequest,
) -> Result<(), String> {
    let db = SparseKernelDb::open_default().map_err(|err| err.to_string())?;
    let Some(policy) = db
        .network_policy_for_trust_zone(trust_zone_id)
        .map_err(|err| err.to_string())?
    else {
        return Err("missing policy".to_string());
    };
    let host = request.host.trim().to_ascii_lowercase();
    if policy
        .denied_cidrs
        .iter()
        .any(|cidr| cidr_contains_host(cidr, &host))
    {
        return Err("denied cidr".to_string());
    }
    if !policy.allow_private_network && is_private_or_local_host(&host) {
        return Err("private network denied".to_string());
    }
    if policy
        .allowed_hosts
        .iter()
        .any(|pattern| host_matches_policy_pattern(&host, pattern))
    {
        return Ok(());
    }
    if !policy.allow_private_network {
        for address in (host.as_str(), request.port)
            .to_socket_addrs()
            .map_err(|err| format!("dns lookup failed: {err}"))?
        {
            if is_private_ip(&address.ip()) {
                return Err("resolved private network denied".to_string());
            }
            if policy
                .denied_cidrs
                .iter()
                .any(|cidr| cidr_contains_ip(cidr, &address.ip()))
            {
                return Err("resolved denied cidr".to_string());
            }
        }
    }
    if policy.default_action == "allow" {
        Ok(())
    } else {
        Err("default deny".to_string())
    }
}

fn record_builtin_proxy_denial(
    trust_zone_id: &str,
    url: &str,
    reason: &str,
) -> Result<(), Box<dyn Error>> {
    let db = SparseKernelDb::open_default()?;
    db.record_audit(AuditInput {
        actor_type: Some("egress_proxy".to_string()),
        actor_id: Some(trust_zone_id.to_string()),
        action: "egress_proxy.denied".to_string(),
        object_type: Some("network_request".to_string()),
        object_id: Some(trust_zone_id.to_string()),
        payload: Some(json!({ "url": url, "reason": reason })),
    })?;
    Ok(())
}

fn host_matches_policy_pattern(host: &str, pattern: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    if pattern.is_empty() {
        return false;
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return host.ends_with(suffix);
    }
    host == pattern
}

fn is_private_or_local_host(host: &str) -> bool {
    host == "localhost"
        || host.ends_with(".localhost")
        || host
            .parse::<IpAddr>()
            .ok()
            .as_ref()
            .is_some_and(is_private_ip)
}

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || is_ipv6_unique_local(ip)
                || is_ipv6_unicast_link_local(ip)
        }
    }
}

fn is_ipv6_unique_local(ip: &Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xfe00) == 0xfc00
}

fn is_ipv6_unicast_link_local(ip: &Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

fn cidr_contains_host(cidr: &str, host: &str) -> bool {
    host.parse::<IpAddr>()
        .ok()
        .is_some_and(|ip| cidr_contains_ip(cidr, &ip))
}

fn cidr_contains_ip(cidr: &str, ip: &IpAddr) -> bool {
    let Some((raw_base, raw_prefix)) = cidr.trim().split_once('/') else {
        return false;
    };
    let Ok(prefix) = raw_prefix.parse::<u32>() else {
        return false;
    };
    match (raw_base.parse::<IpAddr>(), ip) {
        (Ok(IpAddr::V4(base)), IpAddr::V4(ip)) if prefix <= 32 => {
            let base = u32::from(base);
            let ip = u32::from(*ip);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            (base & mask) == (ip & mask)
        }
        (Ok(IpAddr::V6(base)), IpAddr::V6(ip)) if prefix <= 128 => {
            let base = u128::from(base);
            let ip = u128::from(*ip);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (base & mask) == (ip & mask)
        }
        _ => false,
    }
}

fn format_host_for_url(host: &str) -> String {
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

pub fn handle_api_request_with_artifact_root(
    db: &mut SparseKernelDb,
    method: &str,
    url: &str,
    body: &[u8],
    artifact_root: Option<&Path>,
) -> Result<ApiReply, Box<dyn Error>> {
    let mut daemon_state = DaemonState::default();
    handle_api_request_with_daemon_state(db, method, url, body, artifact_root, &mut daemon_state)
}

pub fn handle_api_request_with_daemon_state(
    db: &mut SparseKernelDb,
    method: &str,
    url: &str,
    body: &[u8],
    artifact_root: Option<&Path>,
    daemon_state: &mut DaemonState,
) -> Result<ApiReply, Box<dyn Error>> {
    let reply = match (method, url) {
        ("GET", "/health") => ApiReply {
            status_code: 200,
            body: json!({
                "ok": true,
                "service": "sparsekerneld",
                "version": env!("CARGO_PKG_VERSION"),
                "protocol_version": SPARSEKERNEL_PROTOCOL_VERSION,
                "schema_version": db.schema_version()?,
                "features": [
                    "ledger.v1",
                    "tasks.v1",
                    "artifacts.v1",
                    "artifacts.file-transfer.v1",
                    "capabilities.v1",
                    "browser-broker.v1",
                    "sandbox-broker.v1",
                    "sandbox-backends.probe.v1",
                    "resource-budgets.v1"
                ],
            }),
        },
        ("GET", "/runtime/budgets") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.resource_budgets()?)?,
        },
        ("POST", "/runtime/budgets/update") => {
            let input: ResourceBudgetUpdateInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.update_resource_budgets(input)?)?,
            }
        }
        ("GET", "/status") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(db.inspect()?)?,
        },
        ("POST", "/trust-zones/network-policy") => {
            let input: TrustZoneNetworkPolicyRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(
                    db.network_policy_for_trust_zone(&input.trust_zone_id)?,
                )?,
            }
        }
        ("POST", "/trust-zones/proxy-ref") => {
            let input: AttachTrustZoneProxyRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.attach_network_policy_proxy_to_trust_zone(
                    &input.trust_zone_id,
                    input.proxy_ref,
                )?)?,
            }
        }
        ("GET", "/egress-proxies") => ApiReply {
            status_code: 200,
            body: list_supervised_egress_proxies(daemon_state),
        },
        ("POST", "/egress-proxies/start") => {
            let input: StartEgressProxyRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: start_supervised_egress_proxy(db, daemon_state, input)?,
            }
        }
        ("POST", "/egress-proxies/stop") => {
            let input: StopEgressProxyRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: stop_supervised_egress_proxy(db, daemon_state, input)?,
            }
        }
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
                    BrowserContextAcquireInput {
                        agent_id: input.agent_id.as_deref(),
                        session_id: input.session_id.as_deref(),
                        task_id: input.task_id.as_deref(),
                        trust_zone_id: &input.trust_zone_id,
                        max_contexts: input.max_contexts.unwrap_or(2),
                        cdp_endpoint: input.cdp_endpoint.as_deref(),
                        allowed_origins: input.allowed_origins.as_ref(),
                    },
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
            db.record_browser_observation(RecordBrowserObservationInput {
                context_id: input.context_id,
                target_id: input.target_id,
                observation_type: input.observation_type,
                payload: input.payload,
                created_at: input.created_at,
            })?;
            ApiReply {
                status_code: 200,
                body: json!({ "ok": true }),
            }
        }
        ("POST", "/browser/targets/record") => {
            let input: RecordBrowserTargetInput = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.record_browser_target(input)?)?,
            }
        }
        ("POST", "/browser/targets/close") => {
            let input: CloseBrowserTargetRequest = parse_body(body)?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.close_browser_target(
                    &input.context_id,
                    &input.target_id,
                    input.reason.as_deref(),
                    input.closed_at.as_deref(),
                )?)?,
            }
        }
        ("POST", "/browser/targets/list") => {
            let input: ListBrowserTargetsInput = if body.is_empty() {
                ListBrowserTargetsInput::default()
            } else {
                parse_body(body)?
            };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.list_browser_targets(input)?)?,
            }
        }
        ("POST", "/browser/observations/list") => {
            let input: ListBrowserObservationsInput = if body.is_empty() {
                ListBrowserObservationsInput::default()
            } else {
                parse_body(body)?
            };
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(db.list_browser_observations(input)?)?,
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
                body: serde_json::to_value(broker.allocate_sandbox(SandboxAllocateInput {
                    agent_id: input.agent_id.as_deref(),
                    task_id: input.task_id.as_deref(),
                    trust_zone_id: &input.trust_zone_id,
                    backend: input.backend.as_deref(),
                    docker_image: input.docker_image.as_deref(),
                    max_runtime_ms: input.max_runtime_ms,
                    max_bytes_out: input.max_bytes_out,
                })?)?,
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
        ("GET", "/sandbox/backends/probe") => ApiReply {
            status_code: 200,
            body: serde_json::to_value(probe_sandbox_backends())?,
        },
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
            if base64_artifact_compat_disabled() && input.content_base64.is_some() {
                return Err(
                    "base64 artifact create is disabled; use /artifacts/import-file".into(),
                );
            }
            let bytes = decode_artifact_content(&input)?;
            if input.content_base64.is_some() {
                enforce_base64_artifact_compat_size(bytes.len(), "create")?;
            }
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
        ("POST", "/artifacts/import-file") => {
            let input: ImportArtifactFileRequest = parse_body(body)?;
            if let Some(subject) = &input.subject {
                require_artifact_capability(db, subject, None, "write")?;
            }
            let artifact_root = artifact_root_path(artifact_root);
            let staged_path =
                canonical_child_path(&artifact_staging_root(&artifact_root), &input.staged_path)?;
            let store = ArtifactStore::new(db, &artifact_root);
            let subject = input.subject.as_ref().map(|subject| {
                (
                    subject.subject_type.as_str(),
                    subject.subject_id.as_str(),
                    subject.permission.as_deref().unwrap_or("read"),
                )
            });
            let artifact = store.import_file(
                &staged_path,
                input.mime_type.as_deref(),
                input.retention_policy.as_deref(),
                subject,
            )?;
            db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "artifact.import_file".to_string(),
                object_type: Some("artifact".to_string()),
                object_id: Some(artifact.id.clone()),
                payload: Some(json!({ "sizeBytes": artifact.size_bytes })),
            })?;
            ApiReply {
                status_code: 200,
                body: serde_json::to_value(artifact)?,
            }
        }
        ("POST", "/artifacts/read") => {
            if base64_artifact_compat_disabled() {
                return Err("base64 artifact read is disabled; use /artifacts/export-file".into());
            }
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
            let artifact = db.get_artifact(&input.id)?;
            enforce_base64_artifact_compat_size(artifact.size_bytes as usize, "read")?;
            let bytes = store.read(&input.id, subject)?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "artifact": artifact,
                    "content_base64": BASE64_STANDARD.encode(bytes),
                }),
            }
        }
        ("POST", "/artifacts/export-file") => {
            let input: ExportArtifactFileRequest = parse_body(body)?;
            if let Some(subject) = &input.subject {
                require_artifact_capability(db, subject, Some(&input.id), "read")?;
            }
            let artifact_root = artifact_root_path(artifact_root);
            let export_root = artifact_export_root(&artifact_root);
            fs::create_dir_all(&export_root)?;
            let export_path =
                artifact_export_path(&export_root, &input.id, input.file_name.as_deref());
            let canonical_root = export_root.canonicalize()?;
            let parent = export_path.parent().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid artifact export path")
            })?;
            fs::create_dir_all(parent)?;
            let canonical_parent = parent.canonicalize()?;
            if !canonical_parent.starts_with(&canonical_root) {
                return Err("artifact export path escaped export root".into());
            }
            let store = ArtifactStore::new(db, &artifact_root);
            let subject = input.subject.as_ref().map(|subject| {
                (
                    subject.subject_type.as_str(),
                    subject.subject_id.as_str(),
                    subject.permission.as_deref().unwrap_or("read"),
                )
            });
            let artifact = store.export_file(&input.id, &export_path, subject)?;
            db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "artifact.export_file".to_string(),
                object_type: Some("artifact".to_string()),
                object_id: Some(artifact.id.clone()),
                payload: Some(json!({ "sizeBytes": artifact.size_bytes })),
            })?;
            ApiReply {
                status_code: 200,
                body: json!({
                    "artifact": artifact,
                    "staged_path": export_path.to_string_lossy().to_string(),
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
    use std::sync::{Mutex, OnceLock};
    use std::thread;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    struct EnvRestore {
        name: &'static str,
        previous: Option<String>,
    }

    impl EnvRestore {
        fn remove(name: &'static str) -> Self {
            let previous = env::var(name).ok();
            env::remove_var(name);
            Self { name, previous }
        }

        fn set(name: &'static str, value: &str) -> Self {
            let previous = env::var(name).ok();
            env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => env::set_var(self.name, value),
                None => env::remove_var(self.name),
            }
        }
    }

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

    fn json_call_with_daemon_state(
        db: &mut SparseKernelDb,
        daemon_state: &mut DaemonState,
        method: &str,
        url: &str,
        body: Value,
    ) -> Value {
        handle_api_request_with_daemon_state(
            db,
            method,
            url,
            serde_json::to_string(&body).unwrap().as_bytes(),
            None,
            daemon_state,
        )
        .unwrap()
        .body
    }

    #[test]
    fn sparsekerneld_help_is_handled_by_parser() {
        let err = DaemonCli::try_parse_from(["sparsekerneld", "--help"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
        assert!(err.to_string().contains("Usage: sparsekerneld"));
    }

    #[test]
    fn health_reports_protocol_and_schema_version() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let health = json_call(&mut db, "GET", "/health", json!({}));
        assert_eq!(health["ok"], true);
        assert_eq!(health["service"], "sparsekerneld");
        assert_eq!(health["protocol_version"], SPARSEKERNEL_PROTOCOL_VERSION);
        assert_eq!(health["schema_version"], db.schema_version().unwrap());
        assert!(health["features"]
            .as_array()
            .unwrap()
            .contains(&json!("tasks.v1")));
        assert!(health["features"]
            .as_array()
            .unwrap()
            .contains(&json!("resource-budgets.v1")));
    }

    #[test]
    fn runtime_budget_api_reads_and_updates_budgets() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let initial = json_call(&mut db, "GET", "/runtime/budgets", json!({}));
        assert_eq!(initial["browser_contexts_max"], 2);
        let updated = json_call(
            &mut db,
            "POST",
            "/runtime/budgets/update",
            json!({
                "active_agent_steps_max": 12,
                "browser_contexts_max": 3,
                "heavy_sandboxes_max": 2,
            }),
        );
        assert_eq!(updated["active_agent_steps_max"], 12);
        assert_eq!(updated["browser_contexts_max"], 3);
        assert_eq!(updated["heavy_sandboxes_max"], 2);

        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "resource_budget.updated");
        assert_eq!(audit[0].object_id.as_deref(), Some("budgets"));
        assert_eq!(
            audit[0].payload.as_ref().unwrap()["updatedKeys"],
            json!([
                "activeAgentStepsMax",
                "browserContextsMax",
                "heavySandboxesMax"
            ])
        );
    }

    #[test]
    fn trust_zone_proxy_api_attaches_and_reads_policy() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let attached = json_call(
            &mut db,
            "POST",
            "/trust-zones/proxy-ref",
            json!({
                "trust_zone_id": "public_web",
                "proxy_ref": "http://127.0.0.1:18080/",
            }),
        );
        assert_eq!(attached["network_policy_id"], "public_web_default");
        assert_eq!(attached["proxy_ref"], "http://127.0.0.1:18080/");
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "network_policy.proxy_ref_attached");
        assert_eq!(audit[0].object_id.as_deref(), Some("public_web"));
        assert_eq!(
            audit[0].payload.as_ref().unwrap()["proxyRef"],
            "http://127.0.0.1:18080/"
        );

        let policy = json_call(
            &mut db,
            "POST",
            "/trust-zones/network-policy",
            json!({ "trust_zone_id": "public_web" }),
        );
        assert_eq!(policy["proxy_ref"], "http://127.0.0.1:18080/");

        let cleared = json_call(
            &mut db,
            "POST",
            "/trust-zones/proxy-ref",
            json!({
                "trust_zone_id": "public_web",
                "proxy_ref": null,
            }),
        );
        assert!(cleared["proxy_ref"].is_null());
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "network_policy.proxy_ref_cleared");
        assert_eq!(audit[0].object_id.as_deref(), Some("public_web"));
        assert!(audit[0].payload.as_ref().unwrap()["proxyRef"].is_null());
    }

    #[test]
    fn private_host_detection_covers_ipv6_private_ranges_on_msrv() {
        assert!(is_private_or_local_host("fc00::1"));
        assert!(is_private_or_local_host("fd00::1"));
        assert!(is_private_or_local_host("fe80::1"));
        assert!(is_private_or_local_host("::1"));
        assert!(!is_private_or_local_host("2001:4860:4860::8888"));
    }

    #[test]
    fn daemon_builtin_egress_proxy_enforces_policy_and_stops() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let _home = EnvRestore::set("SPARSEKERNEL_HOME", temp.path().to_str().unwrap());
        let _command = EnvRestore::remove("SPARSEKERNEL_EGRESS_PROXY_COMMAND");
        let mut db = SparseKernelDb::open_default().unwrap();
        let mut daemon_state = DaemonState::default();
        let started = json_call_with_daemon_state(
            &mut db,
            &mut daemon_state,
            "POST",
            "/egress-proxies/start",
            json!({ "trust_zone_id": "public_web", "host": "127.0.0.1" }),
        );
        assert_eq!(started["mode"], "builtin");
        let proxy_ref = started["proxy_ref"].as_str().unwrap();
        let port = proxy_ref
            .trim_end_matches('/')
            .rsplit_once(':')
            .unwrap()
            .1
            .parse::<u16>()
            .unwrap();
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .write_all(
                b"GET http://127.0.0.1/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(response.contains("private network denied"));

        let stopped = json_call_with_daemon_state(
            &mut db,
            &mut daemon_state,
            "POST",
            "/egress-proxies/stop",
            json!({ "trust_zone_id": "public_web", "clear_proxy_ref": true }),
        );
        assert_eq!(stopped["stopped"], true);
        let policy = json_call(
            &mut db,
            "POST",
            "/trust-zones/network-policy",
            json!({ "trust_zone_id": "public_web" }),
        );
        assert!(policy["proxy_ref"].is_null());
    }

    #[test]
    fn task_api_enqueues_claims_and_completes() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let task = json_call(&mut db, "POST", "/tasks/enqueue", json!({ "kind": "demo" }));
        let task_id = task["id"].as_str().unwrap().to_string();
        assert_eq!(task["priority"], 0);

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

        let denied = json_call(
            &mut db,
            "POST",
            "/capabilities/check",
            json!({
                "subject_type": "agent",
                "subject_id": "main",
                "resource_type": "tool",
                "resource_id": "browser",
                "action": "invoke",
            }),
        );
        assert_eq!(denied["allowed"], false);
        let audit = db.list_audit(1).unwrap().into_iter().next().unwrap();
        assert_eq!(audit.action, "capability.denied");
        assert_eq!(audit.object_id.as_deref(), Some("browser"));
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
                "allowed_origins": ["https://example.com"],
            }),
        );
        let context_id = context["id"].as_str().unwrap().to_string();
        assert_eq!(context["status"], "active");
        assert_eq!(context["allowed_origins"][0], "https://example.com");
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "browser_context.acquired");
        assert_eq!(audit[0].object_id.as_deref(), Some(context_id.as_str()));
        assert_eq!(
            audit[0].payload.as_ref().unwrap()["trustZoneId"],
            "public_web"
        );

        let observed = json_call(
            &mut db,
            "POST",
            "/browser/contexts/observe",
            json!({
                "context_id": context_id.clone(),
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
        let targets = json_call(
            &mut db,
            "POST",
            "/browser/targets/list",
            json!({ "context_id": context_id.clone() }),
        );
        assert_eq!(targets[0]["target_id"], "target-1");
        assert_eq!(targets[0]["console_count"], 1);
        let observations = json_call(
            &mut db,
            "POST",
            "/browser/observations/list",
            json!({ "context_id": context_id.clone(), "target_id": "target-1" }),
        );
        assert_eq!(observations[0]["observation_type"], "browser_console");
        let closed = json_call(
            &mut db,
            "POST",
            "/browser/targets/close",
            json!({ "context_id": context_id.clone(), "target_id": "target-1", "reason": "test" }),
        );
        assert_eq!(closed["status"], "closed");

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
        assert_eq!(contexts[0]["allowed_origins"][0], "https://example.com");

        let pools = handle_api_request(&mut db, "GET", "/browser/pools", &[])
            .unwrap()
            .body;
        assert_eq!(pools[0]["trust_zone_id"], "public_web");
        assert_eq!(pools[0]["browser_kind"], "cdp");
        assert_eq!(pools[0]["active_contexts"], 1);
        assert_eq!(pools[0]["cdp_endpoint"], "http://127.0.0.1:9222");

        let released = json_call(
            &mut db,
            "POST",
            "/browser/contexts/release",
            json!({ "context_id": context_id }),
        );
        assert_eq!(released["released"], true);
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "browser_context.released");
        assert_eq!(audit[0].object_id.as_deref(), Some(context_id.as_str()));
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
                "docker_image": "openclaw/plugin-worker:test",
                "max_runtime_ms": 2500,
                "max_bytes_out": 16384,
            }),
        );
        let allocation_id = allocation["id"].as_str().unwrap().to_string();
        assert_eq!(allocation["backend"], "local/no_isolation");
        assert_eq!(allocation["docker_image"], "openclaw/plugin-worker:test");
        assert_eq!(allocation["max_runtime_ms"], 2500);
        assert_eq!(allocation["max_bytes_out"], 16384);
        assert!(allocation["lease_until"].as_str().is_some());
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "sandbox.allocated");
        assert_eq!(audit[0].object_id.as_deref(), Some(allocation_id.as_str()));
        assert_eq!(
            audit[0].payload.as_ref().unwrap()["trustZoneId"],
            "code_execution"
        );

        let released = json_call(
            &mut db,
            "POST",
            "/sandbox/release",
            json!({ "allocation_id": allocation_id }),
        );
        assert_eq!(released["released"], true);
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "sandbox.released");
        assert_eq!(audit[0].object_id.as_deref(), Some(allocation_id.as_str()));
    }

    #[test]
    fn sandbox_probe_api_reports_backend_boundaries() {
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let probes = handle_api_request(&mut db, "GET", "/sandbox/backends/probe", &[])
            .unwrap()
            .body;
        assert!(probes.as_array().unwrap().iter().any(|probe| {
            probe["backend"] == "local/no_isolation" && probe["hard_boundary"] == false
        }));
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
        let _guard = env_lock();
        let _disable_base64 = EnvRestore::remove("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS");
        let _base64_mode = EnvRestore::remove("SPARSEKERNEL_ARTIFACT_BASE64");
        let _base64_limit = EnvRestore::remove("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES");
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

        let staging_dir = root.path().join(".staging");
        fs::create_dir_all(&staging_dir).unwrap();
        let staged_path = staging_dir.join("large.bin");
        fs::write(&staged_path, b"streamed artifact bytes").unwrap();
        let imported = json_call_with_artifact_root(
            &mut db,
            root.path(),
            "POST",
            "/artifacts/import-file",
            json!({
                "staged_path": staged_path,
                "mime_type": "application/octet-stream",
                "retention_policy": "debug",
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }),
        );
        let imported_id = imported["id"].as_str().unwrap().to_string();
        assert_eq!(imported["size_bytes"], 23);
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "artifact".to_string(),
            resource_id: Some(imported_id.clone()),
            action: "read".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();
        let exported = json_call_with_artifact_root(
            &mut db,
            root.path(),
            "POST",
            "/artifacts/export-file",
            json!({
                "id": imported_id,
                "file_name": "exported.bin",
                "subject": {
                    "subject_type": "agent",
                    "subject_id": "main",
                    "permission": "read",
                },
            }),
        );
        let exported_path = PathBuf::from(exported["staged_path"].as_str().unwrap());
        assert!(exported_path.starts_with(root.path().join(".exports")));
        assert_eq!(fs::read(exported_path).unwrap(), b"streamed artifact bytes");

        let outside_path = root.path().join("outside.bin");
        fs::write(&outside_path, b"outside").unwrap();
        let outside = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/import-file",
            serde_json::to_string(&json!({
                "staged_path": outside_path,
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
        assert!(outside.is_err());
    }

    #[test]
    fn artifact_api_can_disable_base64_compatibility() {
        let _guard = env_lock();
        let previous_disable = env::var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS").ok();
        let previous_mode = env::var("SPARSEKERNEL_ARTIFACT_BASE64").ok();
        env::set_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS", "1");
        env::remove_var("SPARSEKERNEL_ARTIFACT_BASE64");
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let root = tempfile::tempdir().unwrap();
        let create = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/create",
            serde_json::to_string(&json!({
                "content_base64": BASE64_STANDARD.encode(b"blocked"),
            }))
            .unwrap()
            .as_bytes(),
            Some(root.path()),
        );
        let text = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/create",
            serde_json::to_string(&json!({
                "content_text": "allowed text",
                "mime_type": "text/plain",
            }))
            .unwrap()
            .as_bytes(),
            Some(root.path()),
        )
        .unwrap()
        .body;
        let read = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/read",
            serde_json::to_string(&json!({ "id": text["id"] }))
                .unwrap()
                .as_bytes(),
            Some(root.path()),
        );
        match previous_disable {
            Some(value) => env::set_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS", value),
            None => env::remove_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS"),
        }
        match previous_mode {
            Some(value) => env::set_var("SPARSEKERNEL_ARTIFACT_BASE64", value),
            None => env::remove_var("SPARSEKERNEL_ARTIFACT_BASE64"),
        }
        assert!(create
            .unwrap_err()
            .to_string()
            .contains("base64 artifact create is disabled"));
        assert!(read
            .unwrap_err()
            .to_string()
            .contains("base64 artifact read is disabled"));
    }

    #[test]
    fn artifact_api_limits_base64_compatibility_size() {
        let _guard = env_lock();
        let previous_limit = env::var("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES").ok();
        let previous_disable = env::var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS").ok();
        let previous_mode = env::var("SPARSEKERNEL_ARTIFACT_BASE64").ok();
        env::set_var("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES", "4");
        env::remove_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS");
        env::remove_var("SPARSEKERNEL_ARTIFACT_BASE64");
        let mut db = SparseKernelDb::open(":memory:").unwrap();
        let root = tempfile::tempdir().unwrap();
        let create = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/create",
            serde_json::to_string(&json!({
                "content_base64": BASE64_STANDARD.encode(b"too large"),
            }))
            .unwrap()
            .as_bytes(),
            Some(root.path()),
        );
        let text = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/create",
            serde_json::to_string(&json!({
                "content_text": "too large for base64",
                "mime_type": "text/plain",
            }))
            .unwrap()
            .as_bytes(),
            Some(root.path()),
        )
        .unwrap()
        .body;
        let read = handle_api_request_with_artifact_root(
            &mut db,
            "POST",
            "/artifacts/read",
            serde_json::to_string(&json!({ "id": text["id"] }))
                .unwrap()
                .as_bytes(),
            Some(root.path()),
        );
        match previous_limit {
            Some(value) => env::set_var("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES", value),
            None => env::remove_var("SPARSEKERNEL_ARTIFACT_BASE64_MAX_BYTES"),
        }
        match previous_disable {
            Some(value) => env::set_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS", value),
            None => env::remove_var("SPARSEKERNEL_DISABLE_BASE64_ARTIFACTS"),
        }
        match previous_mode {
            Some(value) => env::set_var("SPARSEKERNEL_ARTIFACT_BASE64", value),
            None => env::remove_var("SPARSEKERNEL_ARTIFACT_BASE64"),
        }
        assert!(create
            .unwrap_err()
            .to_string()
            .contains("base64 artifact create is limited"));
        assert!(read
            .unwrap_err()
            .to_string()
            .contains("base64 artifact read is limited"));
    }
}
