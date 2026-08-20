//! Bounded local framing.
//!
//! Every frame is a 4-byte big-endian length followed by exactly that many
//! bytes of UTF-8 JSON. The length prefix is what makes the protocol bounded:
//! a reader knows a frame's size before it allocates for it and refuses
//! anything past `max_frame_bytes` instead of growing until the process dies.
//! Nothing here is line-delimited, so a JSON string containing a newline is
//! ordinary data rather than a framing hazard.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Generous enough for a large module bundle, small enough that one bad frame
/// cannot exhaust the host's memory.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

const HEADER_BYTES: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("module host peer closed the connection")]
    Closed,
    #[error("frame of {size} bytes exceeds the {limit} byte frame limit")]
    TooLarge { size: usize, limit: usize },
    #[error("frame is not valid JSON: {0}")]
    Malformed(String),
    #[error("module host transport failed: {0}")]
    Io(#[from] io::Error),
}

impl FrameError {
    /// True once the connection can no longer be used, so the caller stops
    /// reading instead of spinning on a broken stream.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, Self::Malformed(_))
    }
}

pub async fn read_frame<R>(reader: &mut R, limit: usize) -> Result<Vec<u8>, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; HEADER_BYTES];
    if let Err(err) = reader.read_exact(&mut header).await {
        return Err(match err.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Closed,
            _ => FrameError::Io(err),
        });
    }
    let size = u32::from_be_bytes(header) as usize;
    if size == 0 {
        return Err(FrameError::Malformed("frame is empty".to_owned()));
    }
    if size > limit {
        // The payload is deliberately not drained: a peer that overshoots the
        // limit has lost framing sync, so the connection is finished either way.
        return Err(FrameError::TooLarge { size, limit });
    }
    let mut payload = vec![0u8; size];
    if let Err(err) = reader.read_exact(&mut payload).await {
        return Err(match err.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Closed,
            _ => FrameError::Io(err),
        });
    }
    Ok(payload)
}

pub async fn write_frame<W>(writer: &mut W, payload: &[u8], limit: usize) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > limit {
        return Err(FrameError::TooLarge {
            size: payload.len(),
            limit,
        });
    }
    let header = (payload.len() as u32).to_be_bytes();
    writer.write_all(&header).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}
