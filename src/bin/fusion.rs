use env_logger::Env;
use fusion::app::{cli::CliArgs, runtime};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let config = match CliArgs::parse_config() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("fusion config error: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = runtime::run(config).await {
        eprintln!("fusion runtime error: {err}");
        std::process::exit(1);
    }
}
