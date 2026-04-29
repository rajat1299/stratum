#![cfg(feature = "fuser")]

use clap::Parser;
use stratum::config::{CompatibilityTarget, Config};
use stratum::db::StratumDb;
use stratum::fuse_mount;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Parser)]
#[command(name = "stratum-mount", version, about = "Mount stratum through FUSE")]
struct Cli {
    #[arg(long, env = "STRATUM_MOUNTPOINT")]
    mountpoint: PathBuf,

    #[arg(long)]
    read_only: bool,
}

fn main() {
    let cli = Cli::parse();
    let config = Config::from_env().with_compatibility_target(CompatibilityTarget::Posix);
    let db = StratumDb::open(config).expect("failed to open database");
    let fs = Arc::new(Mutex::new(db.snapshot_fs()));
    fuse_mount::mount(fs, cli.mountpoint, cli.read_only).expect("failed to mount filesystem");
}
