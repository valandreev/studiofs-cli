//! Entry point for the StudioFS end-to-end test CLI.

use clap::Parser as _;

fn main() -> anyhow::Result<()> {
    let cli = studiofs_e2e::cli::Cli::parse();
    let exit_code = studiofs_e2e::cli::dispatch(cli)?;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}
