use clap::Parser;
use describe::DescribeOpts;
use head::HeadOpts;
pub mod connect;
pub mod describe;
pub mod head;
pub mod list;
pub mod sql;
use connect::ConnectOpts;
use sql::SqlOpts;

#[derive(Debug, Parser)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "Connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List,
    #[command(name = "describle", about = "describe a dataset")]
    Describe(DescribeOpts),
    #[command(name = "head", about = "show the first n rows of a dataset")]
    Head(HeadOpts),
    #[command(name = "sql", about = "query a dataset with sql")]
    Sql(SqlOpts),
    #[command(name = "exit", about = "exit the repl")]
    Exit,
}
