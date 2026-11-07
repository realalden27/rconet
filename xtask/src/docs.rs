use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Scorecard {
    metadata: Metadata,
    capabilities: Vec<Capability>,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    project: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct Capability {
    id: String,
    layer: String,
    status: String,
    min_tests: u32,
    description: String,
}

pub(crate) fn run_docs(check_only: bool) -> Result<(), String> {
    let root = workspace_root()?;
    let toml_path = root.join("docs").join("scorecard.toml");
    let md_path = root.join("docs").join("scorecard.md");

    let raw_toml = fs::read_to_string(&toml_path)
        .map_err(|err| format!("failed to read {}: {err}", toml_path.display()))?;

    let scorecard: Scorecard = toml::from_str(&raw_toml)
        .map_err(|err| format!("failed to parse {}: {err}", toml_path.display()))?;

    validate_scorecard(&scorecard)?;
    let rendered = render_markdown(&scorecard);

    if check_only {
        let existing = fs::read_to_string(&md_path).map_err(|err| {
            format!(
                "failed to read {} (run `cargo xtask docs` to generate): {err}",
                md_path.display()
            )
        })?;
        if normalize_newlines(&existing) != normalize_newlines(&rendered) {
            return Err(
                "docs/scorecard.md is out of date with docs/scorecard.toml; run `cargo xtask docs`"
                    .to_string(),
            );
        }
        println!("docs ok: docs/scorecard.md matches docs/scorecard.toml");
    } else {
        fs::write(&md_path, rendered)
            .map_err(|err| format!("failed to write {}: {err}", md_path.display()))?;
        println!("docs ok: wrote {}", md_path.display());
    }

    Ok(())
}

fn validate_scorecard(scorecard: &Scorecard) -> Result<(), String> {
    for cap in &scorecard.capabilities {
        match cap.status.as_str() {
            "planned" | "partial" | "complete" => {},
            other => {
                return Err(format!(
                    "invalid status '{other}' for capability '{}' (expected planned, partial, or complete)",
                    cap.id
                ));
            },
        }
        if cap.status == "complete" && cap.min_tests == 0 {
            return Err(format!(
                "capability '{}' is marked 'complete' but has min_tests = 0",
                cap.id
            ));
        }
    }
    Ok(())
}

fn render_markdown(scorecard: &Scorecard) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "<!-- Generated from docs/scorecard.toml via `cargo xtask docs`. Do not edit directly. -->"
    );
    let _ = writeln!(
        out,
        "# `{}` Quality Scorecard (v{})\n",
        scorecard.metadata.project, scorecard.metadata.version
    );
    let _ = writeln!(
        out,
        "| Capability ID | Layer | Status | Min Verified Tests | Description |"
    );
    let _ = writeln!(out, "| :--- | :--- | :--- | ---: | :--- |");

    for cap in &scorecard.capabilities {
        let _ = writeln!(
            out,
            "| `{}` | `{}` | **{}** | {} | {} |",
            cap.id, cap.layer, cap.status, cap.min_tests, cap.description
        );
    }

    out
}

fn normalize_newlines(input: &str) -> String {
    input.replace("\r\n", "\n")
}

pub(crate) fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir)
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "unable to resolve workspace root from xtask manifest dir".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_complete_capability_with_zero_tests() {
        let sc = Scorecard {
            metadata: Metadata {
                project: "rconet".to_string(),
                version: "0.1.0".to_string(),
            },
            capabilities: vec![Capability {
                id: "proto.test".to_string(),
                layer: "L1".to_string(),
                status: "complete".to_string(),
                min_tests: 0,
                description: "test".to_string(),
            }],
        };
        assert!(validate_scorecard(&sc).is_err());
    }
}
