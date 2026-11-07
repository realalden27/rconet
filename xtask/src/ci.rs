use std::process::Command;

use crate::{docs, layers};

pub(crate) fn run_ci() -> Result<(), String> {
    run_cargo(&["fmt", "--all", "--", "--check"])?;
    run_cargo(&["check", "--workspace", "--no-default-features"])?;
    run_cargo(&[
        "clippy",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
    ])?;

    if has_nextest() {
        run_cargo(&[
            "nextest",
            "run",
            "--profile",
            "ci",
            "--workspace",
            "--all-features",
        ])?;
    } else {
        run_cargo(&["test", "--workspace", "--all-features"])?;
    }

    run_cargo(&["test", "--workspace", "--doc", "--all-features"])?;

    layers::check_layers()?;
    docs::run_docs(true)?;

    println!("xtask ci: all verification gates passed");
    Ok(())
}

fn has_nextest() -> bool {
    Command::new("cargo")
        .args(["nextest", "--version"])
        .output()
        .is_ok_and(|out| out.status.success())
}

fn run_cargo(args: &[&str]) -> Result<(), String> {
    println!("==> cargo {}", args.join(" "));
    let status = Command::new("cargo")
        .args(args)
        .status()
        .map_err(|err| format!("failed to spawn cargo {}: {err}", args.join(" ")))?;

    if !status.success() {
        return Err(format!(
            "`cargo {}` failed with status {status}",
            args.join(" ")
        ));
    }
    Ok(())
}
