use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_json::{json, Value};
use sparsekernel_core::{
    CapabilityCheck, EnqueueTaskInput, GrantCapabilityInput, SparseKernelDb, SparseKernelPaths,
};
use std::error::Error;
use std::net::ToSocketAddrs;
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
            "/tasks/claim",
            json!({ "worker_id": "worker-a", "kinds": ["demo"], "lease_seconds": 60 }),
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
}
