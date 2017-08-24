mod cli;
mod dispatcher;

use dispatcher::{Dispatcher, DispatcherConfig};
use cli::{create_cli_app};

fn main() {
    Dispatcher::from_config(
        &DispatcherConfig::from_matches(
            &create_cli_app().get_matches()));
}
