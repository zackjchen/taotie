use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct SqlOpts {
    #[arg(short, long, help = "SQL query")]
    pub query: String,
}

pub fn sql(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let query = args
        .get_one::<String>("query")
        .expect("expect query")
        .to_owned();
    let opts = SqlOpts::new(query);
    let (msg, tx) = ReplMsg::new(opts);
    let res = context.send(msg, tx);
    Ok(res)
}

impl SqlOpts {
    pub fn new(query: String) -> Self {
        SqlOpts { query }
    }
}

impl CmdExector for SqlOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        backend.sql(self).await?.display().await
    }
}
