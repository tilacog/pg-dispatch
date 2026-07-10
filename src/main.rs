mod cli;
mod dispatcher;
mod pg_source;
mod thread_pool;
mod traits;

#[cfg(test)]
mod mocks;

use std::process::exit;

use clap::Parser;

use cli::{Cli, Config};
use dispatcher::production;
use pg_source::PgNotificationSource;

fn main() {
    let cli = Cli::parse();
    let config = Config::from(cli);

    let source = match PgNotificationSource::connect(&config.db_url, &config.db_channel) {
        Ok(source) => source,
        Err(e) => {
            eprintln!("{e}.");
            exit(1);
        }
    };

    println!(
        "[pg-dispatch] Listening to channel: \"{}\".",
        config.db_channel
    );

    let dispatcher = production(&config);
    let mut source = source;
    dispatcher.run(&mut source);
}

#[cfg(test)]
mod tests {
    use super::dispatcher::Dispatcher;
    use super::mocks::{FailingCommandRunner, MockCommandRunner, MockNotificationSource};

    #[test]
    fn dispatches_all_payloads() {
        let source =
            MockNotificationSource::new(vec!["hello".into(), "world".into(), "third".into()]);
        let runner = MockCommandRunner::default();
        let invocations = runner.invocations.clone();
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert_eq!(
            invocations.lock().unwrap().clone(),
            vec!["hello", "world", "third"]
        );
    }

    #[test]
    fn empty_source_dispatches_nothing() {
        let source = MockNotificationSource::new(vec![]);
        let runner = MockCommandRunner::default();
        let invocations = runner.invocations.clone();
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert!(invocations.lock().unwrap().is_empty());
    }

    #[test]
    fn stops_on_runner_error() {
        let source =
            MockNotificationSource::new(vec!["first".into(), "second".into(), "third".into()]);
        let runner = FailingCommandRunner::new(2);
        let invocations = runner.invocations.clone();
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        // Should have processed first and second (which failed), then stopped.
        assert_eq!(invocations.lock().unwrap().clone(), vec!["first", "second"]);
    }

    #[test]
    fn single_payload() {
        let source = MockNotificationSource::new(vec!["only".into()]);
        let runner = MockCommandRunner::default();
        let invocations = runner.invocations.clone();
        let dispatcher = Dispatcher::new(runner);

        let mut source = source;
        dispatcher.run(&mut source);

        assert_eq!(invocations.lock().unwrap().clone(), vec!["only"]);
    }
}
