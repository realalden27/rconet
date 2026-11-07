use std::fs;

use crate::docs::workspace_root;

pub(crate) fn bump_version(new_version: &str) -> Result<(), String> {
    validate_semver(new_version)?;

    let root = workspace_root()?;
    let cargo_toml = root.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml)
        .map_err(|err| format!("failed to read {}: {err}", cargo_toml.display()))?;

    let mut updated = String::with_capacity(contents.len());
    let mut in_workspace_package = false;
    let mut replaced_pkg_version = false;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_workspace_package = trimmed == "[workspace.package]";
        }

        if in_workspace_package && trimmed.starts_with("version = ") && !replaced_pkg_version {
            updated.push_str(&format!("version = \"{new_version}\"\n"));
            replaced_pkg_version = true;
        } else {
            updated.push_str(line);
            updated.push('\n');
        }
    }

    if !replaced_pkg_version {
        return Err("could not locate version field under [workspace.package]".to_string());
    }

    fs::write(&cargo_toml, updated)
        .map_err(|err| format!("failed to write {}: {err}", cargo_toml.display()))?;

    println!("updated [workspace.package] version to {new_version}");
    Ok(())
}

fn validate_semver(version: &str) -> Result<(), String> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() != 3
        || !parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
    {
        return Err(format!("invalid version '{version}' (expected X.Y.Z)"));
    }
    Ok(())
}
