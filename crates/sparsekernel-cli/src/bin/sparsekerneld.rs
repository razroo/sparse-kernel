fn main() {
    if let Err(err) = sparsekernel_cli::run_daemon_cli() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
