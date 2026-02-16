use std::path::PathBuf;
use anyhow::{anyhow, Result};

pub fn get_default_meta_dir(volume_name: &str) -> Result<PathBuf> {
    let home = home::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    Ok(home.join(".studiofs").join("meta").join(volume_name))
}
