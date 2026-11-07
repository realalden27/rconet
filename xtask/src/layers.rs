use std::collections::HashMap;
use std::process::Command;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<Package>,
}

#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    dependencies: Vec<Dependency>,
}

#[derive(Debug, Deserialize)]
struct Dependency {
    name: String,
    kind: Option<String>,
}

const FORBIDDEN_RUNTIME_DEPS_IN_CORE: &[&str] = &[
    "tokio",
    "tokio-util",
    "tokio-tungstenite",
    "async-std",
    "smol",
    "mio",
];

fn crate_layer(name: &str) -> Option<u8> {
    match name {
        "rconet-types" => Some(0),
        "rconet-proto" => Some(1),
        "rconet-transport" => Some(2),
        "rconet" => Some(3),
        "rconet-examples" | "rconet-benches" => Some(4),
        "xtask" => Some(99),
        _ => None,
    }
}

pub(crate) fn check_layers() -> Result<(), String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .map_err(|err| format!("failed to execute cargo metadata: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("cargo metadata failed: {stderr}"));
    }

    let metadata: CargoMetadata = serde_json::from_slice(&output.stdout)
        .map_err(|err| format!("failed to parse cargo metadata JSON: {err}"))?;

    let mut workspace_layers = HashMap::new();
    for pkg in &metadata.packages {
        let Some(layer) = crate_layer(&pkg.name) else {
            return Err(format!(
                "unregistered workspace crate '{}' — assign its architectural layer in xtask/src/layers.rs",
                pkg.name
            ));
        };
        workspace_layers.insert(pkg.name.as_str(), layer);
    }

    for pkg in &metadata.packages {
        let Some(&from_layer) = workspace_layers.get(pkg.name.as_str()) else {
            continue;
        };
        validate_package_deps(pkg, from_layer, &workspace_layers)?;
    }

    println!(
        "layers ok: verified {} workspace crates and runtime-free L0/L1 invariants",
        metadata.packages.len()
    );
    Ok(())
}

fn validate_package_deps(
    pkg: &Package,
    from_layer: u8,
    workspace_layers: &HashMap<&str, u8>,
) -> Result<(), String> {
    for dep in &pkg.dependencies {
        let is_dev = dep.kind.as_deref() == Some("dev");

        if from_layer <= 1 && !is_dev && FORBIDDEN_RUNTIME_DEPS_IN_CORE.contains(&dep.name.as_str())
        {
            return Err(format!(
                "layer violation: L{from_layer} crate '{}' must remain runtime-independent, but depends on '{}'",
                pkg.name, dep.name
            ));
        }

        let Some(&to_layer) = workspace_layers.get(dep.name.as_str()) else {
            continue;
        };

        if from_layer == 99 {
            return Err(format!(
                "layer violation: tooling crate 'xtask' must not depend on workspace crate '{}'",
                dep.name
            ));
        }

        if to_layer >= from_layer && !is_dev {
            return Err(format!(
                "layer violation: '{}' (L{from_layer}) cannot depend on '{}' (L{to_layer})",
                pkg.name, dep.name
            ));
        }
    }
    Ok(())
}
