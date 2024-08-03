use std::{process::exit, thread};

use clap::ArgMatches;
use cli::ReplCommand;
use crossbeam_channel as mpsc;
use reedline_repl_rs::CallBackMap;

pub mod cli;

pub struct ReplContext {
    pub tx: mpsc::Sender<ReplCommand>,
}
impl Default for ReplContext {
    fn default() -> Self {
        Self::new()
    }
}
impl ReplContext {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded();
        thread::Builder::new()
            .name("ReplBackend".to_string())
            .spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        ReplCommand::Connect(opts) => {
                            println!("Connect to dataset: {:?}", opts);
                        }
                        ReplCommand::List => {
                            println!("List all datasets");
                        }
                        ReplCommand::Describe(opts) => {
                            println!("Describe dataset: {:?}", opts);
                        }
                        ReplCommand::Head(opts) => {
                            println!("Show head of dataset: {:?}", opts);
                        }
                        ReplCommand::Sql(opts) => {
                            println!("Query dataset with sql: {:?}", opts);
                        }
                        ReplCommand::Exit => {
                            break;
                        }
                    }
                }
            })
            .unwrap();
        ReplContext { tx }
    }

    pub fn send(&self, cmd: ReplCommand) {
        if let Err(e) = self.tx.send(cmd) {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

pub type ReplCallBacks = CallBackMap<ReplContext, reedline_repl_rs::Error>;
pub fn get_callbacks() -> ReplCallBacks {
    let mut callback = CallBackMap::new();
    callback.insert("connect".to_string(), cli::connect::connect);
    callback.insert("list".to_string(), cli::list::list);
    callback.insert("head".to_string(), cli::head::head);
    callback.insert("describle".to_string(), cli::describe::describe);
    callback.insert("sql".to_string(), cli::sql::sql);
    callback.insert("exit".to_string(), quit);
    callback
}

fn quit(_args: ArgMatches, ctx: &mut ReplContext) -> reedline_repl_rs::Result<Option<String>> {
    ctx.send(ReplCommand::Exit);
    exit(0);
}
