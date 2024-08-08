use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

pub fn describe(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_owned();
    let opts = DescribeOpts::new(name);
    let (msg, tx) = ReplMsg::new(opts);
    let res = context.send(msg, tx);
    Ok(res)
}

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(short, long, help = "Name of the dataset")]
    pub name: String,
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        DescribeOpts { name }
    }
}

impl CmdExector for DescribeOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        let df = backend.describe(self).await?;
        df.display().await
    }
}
