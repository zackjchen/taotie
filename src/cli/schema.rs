use clap::{ArgMatches, Parser};

use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct SchemaOpts {
    #[arg(help = "Name of the dataset")]
    pub name: String,
}

impl SchemaOpts {
    pub fn new(name: String) -> Self {
        SchemaOpts { name }
    }
}

impl CmdExector for SchemaOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.schema(self).await?;
        df.display().await
    }
}

pub fn schema(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_owned();
    let opts = SchemaOpts::new(name);
    let (msg, tx) = ReplMsg::new(opts);
    let res = context.send(msg, tx);
    Ok(res)
}
