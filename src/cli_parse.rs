use clap::{Parser,Subcommand};
use clap_complete::Shell;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(version, about, long_about = None, infer_subcommands = true)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Completions {
        #[arg(short, long)]
        command_name: Option<String>,

        #[arg(value_enum, default_value="zsh")]
        shell: Shell,
    },
    Server {
        #[arg(short, long, env, default_value = "trt-agi-rs_prod")]
        system_name: String,

        #[arg(short='H', long, env,
            default_value = "localhost")]
        host: String,

        #[arg(short, long, env,
            default_value = "8106")]
        port: u16,

        #[arg(short='U', long, env)]
        ami_user: String,

        #[arg(short='P', long, env)]
        password: String,
    },
}
