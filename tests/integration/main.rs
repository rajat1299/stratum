use stratum::auth::session::Session;
use stratum::cmd;
use stratum::cmd::parser;
use stratum::fs::VirtualFs;

pub fn exec(line: &str, fs: &mut VirtualFs) -> String {
    let pipeline = parser::parse_pipeline(line);
    let mut session = Session::root();
    cmd::execute_pipeline(&pipeline, fs, &mut session).unwrap()
}

pub fn exec_err(line: &str, fs: &mut VirtualFs) -> String {
    let pipeline = parser::parse_pipeline(line);
    let mut session = Session::root();
    cmd::execute_pipeline(&pipeline, fs, &mut session)
        .unwrap_err()
        .to_string()
}

pub fn exec_s(line: &str, fs: &mut VirtualFs, session: &mut Session) -> String {
    let pipeline = parser::parse_pipeline(line);
    cmd::execute_pipeline(&pipeline, fs, session).unwrap()
}

mod dirs;
mod edge_cases;
mod files;
mod metadata;
mod nav;
mod permissions;
mod persist;
mod pipes;
mod posix;
mod rm_mv_cp;
mod search;
mod symlinks;
mod vcs;
mod workflows;
