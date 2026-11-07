mod ci;
mod docs;
mod layers;
mod version;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask error: {err}");
            ExitCode::FAILURE
        },
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(subcommand) = args.next() else {
        print_usage();
        return Err("missing subcommand".to_string());
    };

    match subcommand.as_str() {
        "ci" => ci::run_ci(),
        "layers" => layers::check_layers(),
        "docs" => {
            let check_only = args.any(|arg| arg == "--check");
            docs::run_docs(check_only)
        },
        "version" => {
            let Some(new_version) = args.next() else {
                return Err("usage: cargo xtask version <X.Y.Z>".to_string());
            };
            version::bump_version(&new_version)
        },
        "--help" | "-h" | "help" => {
            print_usage();
            Ok(())
        },
        other => {
            print_usage();
            Err(format!("unknown subcommand '{other}'"))
        },
    }
}

fn print_usage() {
    eprintln!(
        "Usage: cargo xtask <COMMAND>\n\n\
         Commands:\n  \
           ci               Run formatting, clippy, feature checks, tests, layers, and docs check\n  \
           layers           Validate directional crate layering and runtime-free core invariants\n  \
           docs [--check]   Generate or verify docs/scorecard.md from docs/scorecard.toml\n  \
           version <X.Y.Z>  Update workspace package version"
    );
}
