use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct ListOpts;

pub fn list(_args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let (msg, tx) = ReplMsg::new(ListOpts);
    let res = context.send(msg, tx);
    Ok(res)
}

impl CmdExector for ListOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        backend.list().await?.display().await
    }
}
