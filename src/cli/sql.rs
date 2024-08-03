use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

use super::ReplCommand;

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
    let cmd = SqlOpts::new(query).into();
    context.send(cmd);
    Ok(None)
}

impl From<SqlOpts> for ReplCommand {
    fn from(opts: SqlOpts) -> Self {
        ReplCommand::Sql(opts)
    }
}

impl SqlOpts {
    pub fn new(query: String) -> Self {
        SqlOpts { query }
    }
}
