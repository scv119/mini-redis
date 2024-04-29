use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct MultiGet {
    /// Name of the keys to get
    keys: Vec<String>,
}

impl MultiGet {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(keys: Vec<String>) -> MultiGet {
        MultiGet {
            keys,
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<MultiGet> {
        // The `MULTIGET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let num_keys = parse.next_int()?;
        let mut keys = Vec::new();
        for _ in 0..num_keys {
            let key = parse.next_string()?;
            keys.push(key);
        }

        Ok(MultiGet::new(keys))
    }

    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Get the value from the shared database state
        let mut response = Frame::array();
        for key in self.keys {
            if let Some(value) = db.get(&key) {
                response.push_bulk(value);
            } else {
                response.push_null();
            }
        }
        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Get` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("multiget".as_bytes()));
        frame.push_int(self.keys.len() as u64);
        for key in self.keys {
            frame.push_bulk(Bytes::from(key.into_bytes()));
        }
        frame
    }
}
