use std::path::PathBuf;

use anyhow::Result;
use reedline_repl_rs::Repl;
use taotie::{cli::ReplCommand, ReplContext};

fn main() -> Result<()> {
    let callbacks = taotie::get_callbacks();
    let ctx = ReplContext::new();
    let path = PathBuf::from("assets/command.log");
    let mut repl = Repl::new(ctx)
        .with_banner("Welcome to Taotie, your dataset exploration REPL!")
        .with_derived::<ReplCommand>(callbacks)
        .with_history(path, 1024);

    repl.run()?;

    Ok(())
}
