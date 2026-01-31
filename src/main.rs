use clap::{Parser, Subcommand};
use orekvs::client::cli::Commands as ClientCommands;
use orekvs::server::cli::Command as ServerCommand;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

fn init_tracing() {
    #[cfg(feature = "tracy")]
    {
        tracing_subscriber::registry()
            .with(tracing_tracy::TracyLayer::default())
            .init();
    }

    #[cfg(not(feature = "tracy"))]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Server(cmd) => orekvs::server::cli::run(cmd).await,
        Commands::Client { addr, command } => orekvs::client::cli::run(addr.clone(), command).await,
    }
}
