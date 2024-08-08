use std::process::exit;

use clap::Parser;
use describe::DescribeOpts;
use enum_dispatch::enum_dispatch;
use head::HeadOpts;
pub mod connect;
pub mod describe;
pub mod head;
pub mod list;
pub mod sql;
use connect::ConnectOpts;
use list::ListOpts;
use sql::SqlOpts;

use crate::{Backend, CmdExector};

#[derive(Debug, Parser)]
#[enum_dispatch(CmdExector)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "describle", about = "describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "show the first n rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "query a dataset with sql")]
    Sql(SqlOpts),
    #[command(name = "exit", about = "exit the repl")]
    Exit(ExitOpts),
}

#[derive(Debug, Parser)]
pub struct ExitOpts;

impl CmdExector for ExitOpts {
    async fn execute<T: Backend>(&self, _backend: &mut T) -> anyhow::Result<String> {
        exit(0);
    }
}
