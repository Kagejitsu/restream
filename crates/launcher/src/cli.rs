//! `restream-cli.exe`: forwards to `bin\restream-cli.exe`, keeping the console.
#[allow(dead_code)]
mod common;

fn main() -> std::process::ExitCode {
    common::run_wait(if cfg!(windows) { "restream-cli.exe" } else { "restream-cli" })
}
