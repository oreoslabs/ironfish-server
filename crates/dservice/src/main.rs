use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use db_handler::{DBHandler, DbConfig, PgHandler};
use dservice::run_dserver;
use utils::{handle_signals, initialize_logger};

#[derive(Parser, Debug, Clone)]
pub struct Command {
    /// The ip:port server will listen on for worker to connect
    #[clap(long, default_value = "0.0.0.0:10001")]
    pub dlisten: SocketAddr,
    /// Set your logger level
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
    /// The path to db config file
    #[clap(long)]
    pub dbconfig: String,
    /// The Ironfish rpc node to connect to
    #[clap(short, long, default_value = "127.0.0.1:9092")]
    pub node: String,
    /// The server to connect to
    #[clap(short, long, default_value = "127.0.0.1:9093")]
    pub server: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse();
    let Command {
        dlisten,
        verbosity,
        dbconfig,
        node,
        server,
    } = args;
    initialize_logger(verbosity);
    handle_signals().await?;
    let db_config = DbConfig::load(dbconfig).unwrap();
    let db_handler = PgHandler::from_config(&db_config);
    run_dserver(dlisten.into(), node, db_handler, server).await?;
    Ok(())
}
