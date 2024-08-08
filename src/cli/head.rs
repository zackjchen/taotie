use crate::{Backend, CmdExector, ReplContext, ReplDisplay, ReplMsg};
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Parser)]
pub struct HeadOpts {
    #[arg(long, help = "Name of the dataset")]
    pub name: String,
    #[arg(short, long, help = "Number of rows to show")]
    pub n: Option<usize>,
}
pub fn head(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_owned();
    let n = args.get_one::<usize>("n").copied();
    let opts = HeadOpts::new(name, n);
    let (msg, tx) = ReplMsg::new(opts);
    let res = context.send(msg, tx);
    Ok(res)
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        HeadOpts { name, n }
    }
}

impl CmdExector for HeadOpts {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String> {
        backend.head(self).await?.display().await
    }
}
