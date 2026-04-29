#![cfg(feature = "fuser")]

use clap::Parser;
use lattice::config::{CompatibilityTarget, Config};
use lattice::db::LatticeDb;
use lattice::fuse_mount;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Parser)]
#[command(name = "lattice-mount", version, about = "Mount lattice through FUSE")]
struct Cli {
    #[arg(long, env = "LATTICE_MOUNTPOINT")]
    mountpoint: PathBuf,

    #[arg(long)]
    read_only: bool,
}

fn main() {
    let cli = Cli::parse();
    let config = Config::from_env().with_compatibility_target(CompatibilityTarget::Posix);
    let db = LatticeDb::open(config).expect("failed to open database");
    let fs = Arc::new(Mutex::new(db.snapshot_fs()));
    fuse_mount::mount(fs, cli.mountpoint, cli.read_only).expect("failed to mount filesystem");
}
