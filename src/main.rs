// use std::path::PathBuf;

use std::path::PathBuf;

use anyhow::Result;
use reedline_repl_rs::Repl;
use taotie::{cli::ReplCommand, ReplContext};

fn main() -> Result<()> {
    let callbacks = taotie::get_callbacks();
    let ctx = ReplContext::new();
    let path = PathBuf::from("./assets/command.log");
    // let path = dirs::home_dir().expect("expect home dir").join(".taotie_history");
    let mut repl = Repl::new(ctx)
        .with_history(path, 1024)
        .with_banner("Welcome to Taotie, your dataset exploration REPL!")
        .with_derived::<ReplCommand>(callbacks);

    repl.run()?;

    Ok(())
}
