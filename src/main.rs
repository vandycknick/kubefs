use std::env;

use crate::fuse::KubeFuse;
use clap::{Arg, Command};
use fuser::MountOption;

mod client;
mod fuse;
mod tree;
mod vfs;

fn get_prog_name() -> Option<String> {
    env::current_exe()
        .map_or_else(
            |_| None,
            |path| path.file_name().map(|f| f.to_string_lossy().into()),
        )
        .and_then(|s: String| match s.is_empty() {
            false => Some(s),
            true => None,
        })
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("mount.kubefs")
        .version("1.0.0")
        .author("Nick Van Dyck")
        .about("Your Kubernetes cluster moutned as a file system. Because why not?")
        .arg(Arg::new("namespace"))
        .arg(Arg::new("mountpoint"))
        .arg(Arg::new("options").short('o').required(false))
        .get_matches();

    let mount_point = matches.get_one::<String>("mountpoint").expect("required");

    let options = vec![
        MountOption::RO,
        MountOption::FSName("kubefs".to_string()),
        // MountOption::AutoUnmount,
        // MountOption::AllowRoot,
        // MountOption::AllowOther,
        MountOption::NoExec,
        MountOption::NoSuid,
    ];

    return match get_prog_name() == Some("mount.kubefs".into()) {
        true => KubeFuse::mount_as_daemon(mount_point, &options),
        false => KubeFuse::mount(mount_point, &options),
    };
}
