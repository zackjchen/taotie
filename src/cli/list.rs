use super::ReplCommand;
use crate::ReplContext;
use clap::ArgMatches;
use reedline_repl_rs::Result;

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    context.send(ReplCommand::List);
    Ok(None)
}
