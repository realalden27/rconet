use std::process::Command;

pub(crate) fn run_fuzz() -> Result<(), String> {
    println!("==> running adversarial panic-hunt and 2048-case property suites");
    let prop_status = Command::new("cargo")
        .env("PROPTEST_CASES", "2048")
        .args([
            "test",
            "-p",
            "rconet-proto",
            "--all-features",
            "--",
            "prop_",
        ])
        .status()
        .map_err(|err| format!("failed to spawn cargo test prop_: {err}"))?;

    if !prop_status.success() {
        return Err(format!(
            "high-iteration proptest suite failed with status {prop_status}"
        ));
    }

    let status = Command::new("cargo")
        .args([
            "test",
            "--workspace",
            "--all-features",
            "--",
            "--ignored",
            "--nocapture",
            "panic_hunt",
        ])
        .status()
        .map_err(|err| format!("failed to spawn cargo test --ignored panic_hunt: {err}"))?;

    if !status.success() {
        return Err(format!(
            "adversarial panic-hunt suite failed with status {status}"
        ));
    }

    println!("fuzz ok: all adversarial panic-hunt and property suites passed");
    Ok(())
}
