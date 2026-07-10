use std::process::exit;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use pg_dispatcher::{
    Cli, CommandExecutor, CommandSpec, Config, Dispatcher, Error, PgNotificationSource,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let config = Config::from(cli);

    if let Err(e) = run(&config).await {
        eprintln!("{e}");
        exit(1);
    }
}

async fn run(config: &Config) -> Result<(), Error> {
    config.validate()?;

    let mut source = PgNotificationSource::connect(&config.db_url, &config.db_channel).await?;

    tracing::info!(channel = %config.db_channel, "listening");

    let (program, args) = config.command.split_first().ok_or(Error::EmptyCommand)?;

    let spec = CommandSpec::with_args(program.clone(), args.to_vec());
    let runner = CommandExecutor::new(config.max_concurrency, spec);
    let dispatcher = Dispatcher::new(runner);

    // Shut down on Ctrl+C or SIGTERM.
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("received Ctrl+C, initiating graceful shutdown");
    };

    dispatcher.run(&mut source, shutdown).await;

    Ok(())
}
