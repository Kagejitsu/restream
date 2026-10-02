//! Stream keys live in the OS credential store (Windows Credential Manager,
//! Secret Service on Linux), never in the profile file. Each key is filed
//! under the profile name and destination name.

use anyhow::Result;
use restream_core::Profile;

const SERVICE: &str = "restream";

fn account(profile: &str, dest: &str) -> String {
    format!("{profile}/{dest}")
}

pub fn store(profile: &str, dest: &str, key: &str) -> Result<()> {
    let e = keyring::Entry::new(SERVICE, &account(profile, dest))?;
    if key.is_empty() {
        match e.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(err) => Err(err.into()),
        }
    } else {
        e.set_password(key)?;
        Ok(())
    }
}

pub fn load(profile: &str, dest: &str) -> Option<String> {
    keyring::Entry::new(SERVICE, &account(profile, dest)).ok()?.get_password().ok()
}

/// Fill in keys from the store for every destination that has none.
pub fn hydrate(p: &mut Profile) {
    let name = p.name.clone();
    for d in &mut p.destinations {
        if d.key.is_empty()
            && let Some(k) = load(&name, &d.name) {
                d.key = k;
            }
    }
}

/// Persist every destination's key and return a copy of the profile safe to
/// write to disk (keys blanked).
pub fn persist_and_strip(p: &Profile) -> Result<Profile> {
    for d in &p.destinations {
        store(&p.name, &d.name, &d.key)?;
    }
    Ok(p.without_keys())
}
