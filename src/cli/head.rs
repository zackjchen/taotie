use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommand;

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
    let cmd = HeadOpts::new(name, n).into();
    context.send(cmd);
    Ok(None)
}

impl From<HeadOpts> for ReplCommand {
    fn from(opts: HeadOpts) -> Self {
        ReplCommand::Head(opts)
    }
}

impl HeadOpts {
    pub fn new(name: String, n: Option<usize>) -> Self {
        HeadOpts { name, n }
    }
}
