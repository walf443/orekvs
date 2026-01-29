use clap::{Parser, Subcommand};
use orekvs::client::cli::Commands as ClientCommands;
use orekvs::server::cli::Command as ServerCommand;

#[derive(Parser)]
#[command(name = "orekvs")]
#[command(about = "A simple key-value store implementation", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the key-value store server
    Server(ServerCommand),
    /// Client commands for interacting with the server
    Client {
        /// Server address to connect to
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        addr: String,

        #[command(subcommand)]
        command: ClientCommands,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Server(cmd) => orekvs::server::cli::run(cmd).await,
        Commands::Client { addr, command } => orekvs::client::cli::run(addr.clone(), command).await,
    }
}
