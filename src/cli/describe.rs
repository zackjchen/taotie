use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommand;

pub fn describe(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_owned();
    let cmd = DescribeOpts::new(name).into();
    context.send(cmd);
    Ok(None)
}

#[derive(Debug, Parser)]
pub struct DescribeOpts {
    #[arg(short, long, help = "Name of the dataset")]
    pub name: String,
}

impl From<DescribeOpts> for ReplCommand {
    fn from(opts: DescribeOpts) -> Self {
        ReplCommand::Describe(opts)
    }
}

impl DescribeOpts {
    pub fn new(name: String) -> Self {
        DescribeOpts { name }
    }
}
