use env_logger::Env;
use fusion::app::{cli::CliArgs, runtime};

fn init_logger(default_level: &str) {
    env_logger::Builder::from_env(
        Env::default().default_filter_or(default_level),
    )
    .init();
}

#[tokio::main]
async fn main() {
    let config = match CliArgs::parse_config() {
        Ok(config) => config,
        Err(err) => {
            init_logger("warn");
            eprintln!("fusion config error: {err}");
            std::process::exit(2);
        }
    };

    init_logger(&config.log_level);

    if let Err(err) = runtime::run(config).await {
        eprintln!("fusion runtime error: {err}");
        std::process::exit(1);
    }
}
