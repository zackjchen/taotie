use std::{process::exit, thread};

use backend::datafusion::DataFusionBackend;
use clap::ArgMatches;
use cli::{
    connect::ConnectOpts, describe::DescribeOpts, head::HeadOpts, list::ListOpts,
    schema::SchemaOpts, sql::SqlOpts, ExitOpts, ReplCommand,
};
use crossbeam_channel as mpsc;
use enum_dispatch::enum_dispatch;
use reedline_repl_rs::CallBackMap;
use tokio::runtime::Runtime;

pub mod backend;
pub mod cli;

#[enum_dispatch]
trait CmdExector {
    async fn execute<T: Backend>(&self, backend: &mut T) -> anyhow::Result<String>;
}
trait ReplDisplay {
    async fn display(&self) -> anyhow::Result<String>;
}

trait Backend {
    // type DataFrame: ReplDisplay;
    async fn connect(&mut self, opts: &ConnectOpts) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<impl ReplDisplay>;
    async fn describe(&self, opts: &DescribeOpts) -> anyhow::Result<impl ReplDisplay>;
    async fn schema(&self, opts: &SchemaOpts) -> anyhow::Result<impl ReplDisplay>;
    async fn head(&self, opts: &HeadOpts) -> anyhow::Result<impl ReplDisplay>;
    async fn sql(&self, opts: &SqlOpts) -> anyhow::Result<impl ReplDisplay>;
}
pub struct ReplContext {
    pub tx: mpsc::Sender<ReplMsg>,
}
#[derive(Debug)]
pub struct ReplMsg {
    pub cmd: ReplCommand,
    pub tx: mpsc::Sender<String>,
}

impl ReplMsg {
    pub fn new(cmd: impl Into<ReplCommand>) -> (Self, mpsc::Receiver<String>) {
        let (tx, rt) = mpsc::unbounded();
        (
            Self {
                cmd: cmd.into(),
                tx,
            },
            rt,
        )
    }
}

impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}
impl ReplContext {
    pub fn new() -> Self {
        let mut backend = DataFusionBackend::new();
        let rt = Runtime::new().expect("Failed to create tokio runtime");
        let (tx, rx) = mpsc::unbounded::<ReplMsg>();

        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    let cmd = msg.cmd;
                    if let ReplCommand::Exit(_) = cmd {
                        exit(0);
                    }
                    if let Err(e) = rt.block_on(async {
                        let res = cmd.execute(&mut backend).await?;
                        msg.tx.send(res).unwrap();
                        Ok::<_, anyhow::Error>(())
                    }) {
                        println!("Error: {}", e);
                    }
                }
            })
            .unwrap();
        ReplContext { tx }
    }

    pub fn send(&self, msg: ReplMsg, tx: mpsc::Receiver<String>) -> Option<String> {
        if let Err(e) = self.tx.send(msg) {
            eprintln!("Repl Send Error: {}", e);
            std::process::exit(1);
        }
        match tx.recv() {
            Ok(res) => Some(res),
            Err(e) => {
                // println!("Repl Recv Error: {}", e);
                // std::process::exit(1);
                Some(e.to_string())
            }
        }
    }
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;
pub fn get_callbacks() -> ReplCallBacks {
    let mut callback = CallBackMap::new();
    callback.insert("connect".to_string(), cli::connect::connect);
    callback.insert("list".to_string(), cli::list::list);
    callback.insert("head".to_string(), cli::head::head);
    callback.insert("schema".to_string(), cli::schema::schema);
    callback.insert("describe".to_string(), cli::describe::describe);
    callback.insert("sql".to_string(), cli::sql::sql);
    callback.insert("exit".to_string(), quit);
    callback
}

fn quit(_args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    let (msg, tx) = ReplMsg::new(ExitOpts);
    ctx.send(msg, tx);
    exit(0);
}
