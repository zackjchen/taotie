use super::ReplCommand;
use crate::ReplContext;
use clap::{ArgMatches, Parser};
use reedline_repl_rs::Result;

#[derive(Debug, Clone)]
pub enum DatabaseConn {
    Postgres(String),
    Csv(String),
    Parquet(String),
    Json(String),
    Quit,
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    /// 这里也是help: Connection string to the dataset, could be postgres or local file(support: csv, parquet, json)
    #[arg(value_parser = verify_conn_str)]
    pub conn: DatabaseConn,
    #[arg(short, long, help = "if database, the name of the table")]
    pub table: Option<String>,
    #[arg(short, long, help = "Name of the dataset")]
    pub name: String,
}

pub fn connect(args: ArgMatches, context: &mut ReplContext) -> Result<Option<String>> {
    let conn = args
        .get_one::<DatabaseConn>("conn")
        .expect("expect conn_str")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_owned());
    let name = args
        .get_one::<String>("name")
        .expect("expect conn_str")
        .to_owned();

    let cmd: ReplCommand = ConnectOpts::new(conn, table, name).into();
    context.send(cmd);
    Ok(None)
}

impl From<ConnectOpts> for ReplCommand {
    fn from(opts: ConnectOpts) -> Self {
        ReplCommand::Connect(opts)
    }
}

impl ConnectOpts {
    pub fn new(conn: DatabaseConn, table: Option<String>, name: String) -> Self {
        ConnectOpts { conn, table, name }
    }
}

fn verify_conn_str(s: &str) -> std::result::Result<DatabaseConn, String> {
    if s.starts_with("postgres://") {
        Ok(DatabaseConn::Postgres(s.to_string()))
    } else if s.ends_with(".csv") {
        Ok(DatabaseConn::Csv(s.to_string()))
    } else if s.ends_with(".parquet") {
        Ok(DatabaseConn::Parquet(s.to_string()))
    } else if s.ends_with(".json") {
        Ok(DatabaseConn::Json(s.to_string()))
    } else {
        Err("Invalid connection string".to_string())
    }
}
