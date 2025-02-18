use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Error as IOError, Read, Seek},
    str::Utf8Error,
    time::SystemTimeError,
};

use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use num_derive::FromPrimitive;
use tempfile::tempfile;
use thiserror::Error;

// file path normalization taken from cargo
// https://github.com/rust-lang/cargo/blob/6d6dd9d9be9c91390da620adf43581619c2fa90e/crates/cargo-util/src/paths.rs#L81
// This has been modified as follows:
//   -  Does accept root dir `/` as the first entry.
//   - Operators on Utf8Paths from the camino crate
fn normalize_path(path: &Utf8Path) -> Utf8PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Utf8Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        Utf8PathBuf::from(c.as_str())
    } else {
        Utf8PathBuf::new()
    };
    // if the path begins with any number of rootdir components skip them
    while let Some(_c @ Utf8Component::RootDir) = components.peek().cloned() {
        components.next();
    }

    for component in components {
        match component {
            Utf8Component::Prefix(..) => unreachable!(),
            Utf8Component::RootDir => {
                unreachable!()
            }
            Utf8Component::CurDir => {}
            Utf8Component::ParentDir => {
                ret.pop();
            }
            Utf8Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

pub type FileStoreResult<T> = Result<T, FileStoreError>;
#[derive(Error, Debug)]
pub enum FileStoreError {
    #[error("File data storage error: {0}")]
    IO(#[from] IOError),
    #[error("Error Formating String: {0}")]
    Format(#[from] std::fmt::Error),
    #[error("Error getting SystemTime: {0}")]
    SystemTime(#[from] SystemTimeError),
    #[error("Cannot find relative path between {0:} and {1:}.")]
    PathDiff(String, String),
    #[error("Error converting string from UTF-8: {0:}")]
    UTF8(#[from] Utf8Error),
}

/// Defines any necessary actions a CFDP File Store implementation
/// must perform. Assumes any FileStore has a root path it operates relative to.
pub trait FileStore {
    /// Returns the path to the target with the root path prepended.
    /// Used when manipulating the filesystem relative to the root path.
    fn get_native_path<P: AsRef<Utf8Path>>(&self, path: P) -> Utf8PathBuf;

    /// Creates the directory Relative to the root path.
    fn create_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Remove a directory Relative to the root path.
    fn remove_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Creates a file relative to the root path.
    fn create_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Delete a file relative to the root path.
    fn delete_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Opens a file relative to the root path with the given options.
    fn open<P: AsRef<Utf8Path>>(&self, path: P, options: &mut OpenOptions)
        -> FileStoreResult<File>;

    /// Opens a system temporary file
    fn open_tempfile(&self) -> FileStoreResult<File>;

    /// Retuns the size of the file on disk relative to the root path.
    fn get_size<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<u64>;
}

/// Store the root path information for a FileStore implementation
/// using built in rust [std::fs] interface.
pub struct NativeFileStore {
    root_path: Utf8PathBuf,
}
impl NativeFileStore {
    pub fn new<P: AsRef<Utf8Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.as_ref().to_owned(),
        }
    }
}
impl FileStore for NativeFileStore {
    fn get_native_path<P: AsRef<Utf8Path>>(&self, path: P) -> Utf8PathBuf {
        let path = path.as_ref();
        match path.starts_with(&self.root_path) {
            true => path.to_path_buf(),
            false => {
                let normal_path = normalize_path(path);
                self.root_path.join(normal_path)
            }
        }
    }

    /// This is a wrapper around [fs::create_dir]
    fn create_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::create_dir(full_path)?;
        Ok(())
    }

    /// This function wraps [fs::remove_dir_all]
    fn remove_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_dir_all(full_path)?;
        Ok(())
    }

    /// This is a wrapper around [File::create]
    fn create_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let path = self.get_native_path(path);
        let f = File::create(path)?;
        f.sync_all().map_err(FileStoreError::IO)
    }

    /// This is a wrapper around [fs::remove_file]
    fn delete_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_file(full_path)?;
        Ok(())
    }

    fn open<P: AsRef<Utf8Path>>(
        &self,
        path: P,
        options: &mut OpenOptions,
    ) -> FileStoreResult<File> {
        let full_path = self.get_native_path(path);
        Ok(options.open(full_path)?)
    }

    /// This is an alias for [tempfile::tempfile]
    fn open_tempfile(&self) -> FileStoreResult<File> {
        Ok(tempfile()?)
    }

    /// This function uses [fs::metadata] to read the size of the input file relative to the root path.
    fn get_size<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<u64> {
        let full_path = self.get_native_path(path);
        Ok(fs::metadata(full_path)?.len())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, FromPrimitive, PartialEq, Eq)]
/// CCSDS enumerated checksum types
pub enum ChecksumType {
    /// Turn every 4 bytes into a u32 and accumulate
    Modular = 0,
    /// This checksum is always 0
    Null = 15,
}

/// Computes all pre-defined CCSDS checksums
pub trait FileChecksum {
    /// Given the input [ChecksumType] compute the appropriate algorithm.
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32>;
}

impl<R: Read + Seek + ?Sized> FileChecksum for R {
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32> {
        match checksum_type {
            ChecksumType::Null => Ok(0_u32),
            ChecksumType::Modular => {
                let mut reader = BufReader::new(self);
                // reset the file pointer to the beginning
                reader.rewind()?;

                let mut checksum: u32 = 0;
                'outer: loop {
                    // fill_buffer will return an empty slice when EoF is reached
                    // on the internal Read instance
                    let buffer = reader.fill_buf()?;

                    if buffer.is_empty() {
                        // if nothing was read break from the loop

                        break 'outer;
                    }

                    // chunks_exact can some times be more efficient than chunks
                    // we'll have to deal with the remainder anyway.
                    // Take 4 bytes at a time from the buffer, convert to u32 and add
                    let mut iter = buffer.chunks_exact(4);
                    (&mut iter).for_each(|chunk| {
                        // we can unwrap because we are guaranteed to have a length 4 slice
                        checksum =
                            checksum.wrapping_add(u32::from_be_bytes(chunk.try_into().unwrap()));
                    });
                    // handle any remainder by resizing to 4-bytes then adding
                    if !iter.remainder().is_empty() {
                        let mut remainder = iter.remainder().to_vec();
                        remainder.resize(4, 0_u8);
                        // we can unwrap because we are guaranteed to have a length 4 vector
                        checksum = checksum
                            .wrapping_add(u32::from_be_bytes(remainder.try_into().unwrap()));
                    }

                    let len = buffer.len();
                    // update the internal buffer to let it know
                    // len bytes were consumed
                    reader.consume(len);
                }
                Ok(checksum)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Write;

    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    #[once]
    fn test_filestore(tempdir_fixture: &TempDir) -> NativeFileStore {
        NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        )
    }

    #[rstest]
    fn create_file(test_filestore: &NativeFileStore) {
        let path = Utf8Path::new("create_file.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);

        assert!(full_path.exists())
    }

    #[rstest]
    fn delete_file(test_filestore: &NativeFileStore) {
        let path = Utf8Path::new("delete_file.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists());

        test_filestore.delete_file(path).unwrap();

        assert!(!full_path.exists())
    }

    #[rstest]
    fn create_tmpfile(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let mut file = test_filestore.open_tempfile()?;

        {
            file.write_all("hello, world!".as_bytes())?;
            file.sync_all()?;
        }
        file.rewind()?;
        let mut recovered_text = String::new();
        file.read_to_string(&mut recovered_text)?;

        assert_eq!("hello, world!".to_owned(), recovered_text);
        Ok(())
    }

    #[rstest]
    fn get_filesize(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let input_text = "Hello, world!";
        let expected = input_text.as_bytes().len() as u64;
        {
            let mut file =
                test_filestore.open("test.dat", OpenOptions::new().create(true).write(true))?;
            file.write_all(input_text.as_bytes())?;
            file.sync_all()?;
        }

        let size = test_filestore.get_size("test.dat")?;

        assert_eq!(expected, size);
        Ok(())
    }

    #[rstest]
    fn checksum_cursor(
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
    ) -> FileStoreResult<()> {
        let file_data: Vec<u8> = vec![0x8a, 0x1b, 0x37, 0x44, 0x78, 0x91, 0xab, 0x03, 0x46, 0x12];

        let expected_checksum = match &checksum_type {
            ChecksumType::Null => 0_u32,
            ChecksumType::Modular => 0x48BEE247_u32,
        };

        let recovered_checksum = std::io::Cursor::new(file_data).checksum(checksum_type)?;

        assert_eq!(expected_checksum, recovered_checksum);
        Ok(())
    }

    #[rstest]
    fn checksum_file(
        test_filestore: &NativeFileStore,
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
    ) -> FileStoreResult<()> {
        let file_data: Vec<u8> = vec![0x8a, 0x1b, 0x37, 0x44, 0x78, 0x91, 0xab, 0x03, 0x46, 0x12];

        {
            let mut file = test_filestore.open(
                "checksum.txt",
                OpenOptions::new().create(true).truncate(true).write(true),
            )?;
            file.write_all(file_data.as_slice())?;
            file.sync_all()?;
        }
        let expected_checksum = match &checksum_type {
            ChecksumType::Null => 0_u32,
            ChecksumType::Modular => 0x48BEE247_u32,
        };

        let recovered_checksum = {
            let mut file =
                test_filestore.open("checksum.txt", OpenOptions::new().create(false).read(true))?;
            file.checksum(checksum_type)?
        };

        assert_eq!(expected_checksum, recovered_checksum);
        Ok(())
    }

    #[fixture]
    #[once]
    fn failure_dir() -> TempDir {
        TempDir::new().unwrap()
    }
}
