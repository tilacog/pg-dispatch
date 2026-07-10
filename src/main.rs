use std::process::exit;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use pg_dispatcher::{
    Cli, CommandSpec, Config, Dispatcher, Error, PgNotificationSource, ThreadPool,
};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let config = Config::from(cli);

    if let Err(e) = run(&config) {
        eprintln!("{e}");
        exit(1);
    }
}

fn run(config: &Config) -> Result<(), Error> {
    config.validate()?;

    let mut source = PgNotificationSource::connect(&config.db_url, &config.db_channel)?;

    tracing::info!(channel = %config.db_channel, "listening");

    let command_parts = &config.command;
    let (program, args) = command_parts.split_first().ok_or(Error::EmptyCommand)?;

    let spec = CommandSpec::with_args(program.clone(), args.to_vec());
    let pool = ThreadPool::new(config.max_threads, spec);
    let dispatcher = Dispatcher::new(pool);

    dispatcher.run(&mut source);

    Ok(())
}
