use libc::{c_char, mkfifo};
use log::warn;
use std::fs::OpenOptions;
use std::io::{Error, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use crate::SmdpRequest;

struct ScopeCall<F: FnOnce()> {
    c: Option<F>,
}
impl<F: FnOnce()> Drop for ScopeCall<F> {
    fn drop(&mut self) {
        self.c.take().unwrap()()
    }
}

macro_rules! expr {
    ($e: expr) => {
        $e
    };
}
macro_rules! defer {
    ($($data: tt)*) => (
        let _scope_call = ScopeCall {
            c: Some(|| -> () { expr!({ $($data)* }) })
        };
    )
}

pub struct Fifo {
    path: PathBuf,
}
impl Fifo {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let os_str = path.clone().into_os_string();
        let slice = os_str.as_bytes();
        let mut bytes = Vec::with_capacity(slice.len() + 1);
        bytes.extend_from_slice(slice);
        bytes.push(0); // zero terminated string
        let _ = std::fs::remove_file(&path);
        if unsafe { mkfifo((&bytes[0]) as *const u8 as *const c_char, 0o644) } != 0 {
            Err(Error::last_os_error())
        } else {
            Ok(Fifo { path })
        }
    }

    pub fn listen(&self, sender: Sender<SmdpRequest>) -> Result<(), std::io::Error> {
        loop {
            {
                let mut pipe = Arc::new(OpenOptions::new().read(true).open(&self.path)?);
                let pipe_clone = pipe.clone();
                defer!(drop(pipe_clone));

                let mut buf: String = String::new();
                pipe.read_to_string(&mut buf)?;
                buf = buf.trim().to_string(); // strip newline
                let _ = sender
                    .send(SmdpRequest::Transfer(buf))
                    .map_err(|e| warn!("send error: {e}"));
            }
        }
    }
}

impl Drop for Fifo {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
