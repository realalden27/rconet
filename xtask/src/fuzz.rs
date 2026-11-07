use std::process::Command;

pub(crate) fn run_fuzz() -> Result<(), String> {
    println!("==> running adversarial panic-hunt suites across workspace");
    let status = Command::new("cargo")
        .env("PROPTEST_CASES", "2048")
        .args([
            "test",
            "--workspace",
            "--all-features",
            "--",
            "--ignored",
            "--nocapture",
        ])
        .status()
        .map_err(|err| format!("failed to spawn cargo test --ignored: {err}"))?;

    if !status.success() {
        return Err(format!(
            "adversarial panic-hunt suite failed with status {status}"
        ));
    }

    println!("fuzz ok: all adversarial panic-hunt suites passed");
    Ok(())
}
