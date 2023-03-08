use crate::client::KubeClient;
use crate::vfs::KubeVirtualFs;
use daemonize::{Daemonize, Outcome};
use fuser::{
    Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request,
};
use libc::ENOENT;
use std::ffi::OsStr;
use std::fs::{self, File, Metadata};
// use std::os::linux::fs::MetadataExt;
use std::process::exit;
use std::time::{Duration, SystemTime};

pub struct KubeFuse {
    kube_vfs: KubeVirtualFs,
    mount_metadata: Metadata,
    startup: SystemTime,
}

impl KubeFuse {
    pub fn new(mount_point: &str) -> Self {
        let kube_client = KubeClient::new();
        let kube_vfs = KubeVirtualFs::new(kube_client);
        let meta = fs::metadata(mount_point).unwrap();
        KubeFuse {
            kube_vfs,
            mount_metadata: meta,
            startup: SystemTime::now(),
        }
    }

    pub fn mount(mountpoint: &str, options: &Vec<MountOption>) -> anyhow::Result<()> {
        let fuse = KubeFuse::new(mountpoint);
        fuser::mount2(fuse, mountpoint, &options)?;
        Ok(())
    }

    pub fn mount_as_daemon(mountpoint: &str, options: &Vec<MountOption>) -> anyhow::Result<()> {
        let stdout = File::create("/tmp/daemon.out").unwrap();
        let stderr = File::create("/tmp/daemon.err").unwrap();

        let daemon = Daemonize::new().stdout(stdout).stderr(stderr);

        match daemon.execute() {
            Outcome::Parent(Ok(p)) => exit(p.first_child_exit_code),
            Outcome::Parent(Err(err)) => Err(err.into()),
            Outcome::Child(Ok(_)) => {
                KubeFuse::mount(&mountpoint, &options)?;
                Ok(())
            }
            Outcome::Child(Err(err)) => Err(err.into()),
        }
    }
}

const TTL: Duration = Duration::from_secs(1); // 1 second

impl Filesystem for KubeFuse {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!(
            "lookup(parent:{}, name: {})",
            parent,
            name.to_string_lossy()
        );

        if let Some(file) = self
            .kube_vfs
            .get_file_from_parent_by_name_two(parent, name.to_str().unwrap())
        {
            let (_, attr) = file;
            reply.entry(&TTL, &attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino:{})", ino);
        match self.kube_vfs.get_file(ino).map(|(_, f)| f) {
            Some(attr) => reply.attr(&TTL, &attr),
            _ => reply.error(ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        _size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        println!(
            "read(ino: {}, fh: {}, offset: {}, size: {}, flags: {}, lock: {:?})",
            ino, _fh, offset, _size, _flags, _lock
        );

        match self.kube_vfs.get_kube_manifest(ino) {
            Ok(contents) => reply.data(&contents.as_bytes()[offset as usize..]),
            Err(_) => reply.error(ENOENT),
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        println!("opendir(ino: {}, flags: {})", ino, _flags);
        match self.kube_vfs.get_file(ino) {
            Some(attr) => reply.opened(0, attr.1.flags),
            _ => reply.error(ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir(ino: {}, fh: {}, offset: {})", ino, _fh, offset);
        if let Some(files) = self.kube_vfs.list_files_two(ino) {
            for (i, (name, file)) in files.iter().enumerate().skip(offset as usize) {
                if reply.add(file.ino, offset + (i) as i64 + 1, file.kind, name) {
                    break;
                }
            }

            reply.ok();
            return;
        }

        reply.error(ENOENT);
    }
}
