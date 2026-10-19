use std::io::{Cursor, Read, Write};

use anyhow::anyhow;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

use crate::context::RPCContext;
use crate::rpc::*;
use crate::xdr::*;
use crate::{mount, mount_handlers, nfs, nfs_handlers, portmap, portmap_handlers};

// Information from RFC 5531
// https://datatracker.ietf.org/doc/html/rfc5531

const NFS_ACL_PROGRAM: u32 = 100227;
const NFS_ID_MAP_PROGRAM: u32 = 100270;
const NFS_METADATA_PROGRAM: u32 = 200024;

async fn handle_rpc(
    input: &mut impl Read,
    output: &mut impl Write,
    mut context: RPCContext,
) -> Result<bool, anyhow::Error> {
    let mut recv = rpc_msg::default();
    recv.deserialize(input)?;
    let xid = recv.xid;
    if let rpc_body::CALL(call) = recv.body {
        if let auth_flavor::AUTH_UNIX = call.cred.flavor {
            let mut auth = auth_unix::default();
            auth.deserialize(&mut Cursor::new(&call.cred.body))?;
            context.auth = auth;
        }
        if call.rpcvers != 2 {
            warn!("Invalid RPC version {} != 2", call.rpcvers);
            rpc_vers_mismatch(xid).serialize(output)?;
            return Ok(true);
        }

        if let Some(reply) = context.transaction_tracker.completed_response(xid, &context.client_addr) {
            output.write_all(&reply)?;
            return Ok(true);
        }
        if context.transaction_tracker.is_retransmission(xid, &context.client_addr)? {
            // This is a retransmission
            // Drop the message and return
            debug!("Retransmission detected, xid: {}, client_addr: {}, call: {:?}", xid, context.client_addr, call);
            return Ok(false);
        }

        // Only NFSv3 procedures 1..21 have an nfsstat3 result. NULL and
        // unrelated RPC services must retain their ordinary replay behavior.
        let nfs_status_reply = call.prog == nfs::PROGRAM && call.vers == 3 && (1..=21).contains(&call.proc);
        let mut response = Vec::new();
        let destination = &mut response;
        let res = {
            if call.prog == nfs::PROGRAM {
                nfs_handlers::handle_nfs(xid, call, input, destination, &context).await
            } else if call.prog == portmap::PROGRAM {
                portmap_handlers::handle_portmap(xid, call, input, destination, &context)
            } else if call.prog == mount::PROGRAM {
                mount_handlers::handle_mount(xid, call, input, destination, &context).await
            } else if call.prog == NFS_ACL_PROGRAM
                || call.prog == NFS_ID_MAP_PROGRAM
                || call.prog == NFS_METADATA_PROGRAM
            {
                trace!("ignoring NFS_ACL packet");
                prog_unavail_reply_message(xid).serialize(destination)?;
                Ok(())
            } else {
                warn!("Unknown RPC Program number {} != {}", call.prog, nfs::PROGRAM);
                prog_unavail_reply_message(xid).serialize(destination)?;
                Ok(())
            }
        }
        .map(|_| true);
        if res.is_ok() {
            output.write_all(&response)?;
            // The emitted success envelope uses an empty AUTH_NULL verifier.
            // Check every discriminant before inspecting its NFS status: a
            // successful mutation payload must never be mistaken for overload.
            let success_header = [xid, 1, 0, 0, 0, 0].map(u32::to_be_bytes);
            let transient = nfs_status_reply
                && response.get(..24) == Some(success_header.as_flattened())
                && response.get(24..28) == Some((nfs::nfsstat3::NFS3ERR_JUKEBOX as u32).to_be_bytes().as_slice());
            if transient {
                context.transaction_tracker.abandon(xid, &context.client_addr);
            } else {
                context.transaction_tracker.mark_processed(xid, &context.client_addr, response);
            }
        }
        if res.is_err() { context.transaction_tracker.abandon(xid,&context.client_addr); }
        res
    } else {
        error!("Unexpectedly received a Reply instead of a Call");
        Err(anyhow!("Bad RPC Call format"))
    }
}

/// RFC 1057 Section 10
/// When RPC messages are passed on top of a byte stream transport
/// protocol (like TCP), it is necessary to delimit one message from
/// another in order to detect and possibly recover from protocol errors.
/// This is called record marking (RM).  Sun uses this RM/TCP/IP
/// transport for passing RPC messages on TCP streams.  One RPC message
/// fits into one RM record.
///
/// A record is composed of one or more record fragments.  A record
/// fragment is a four-byte header followed by 0 to (2**31) - 1 bytes of
/// fragment data.  The bytes encode an unsigned binary number; as with
/// XDR integers, the byte order is from highest to lowest.  The number
/// encodes two values -- a boolean which indicates whether the fragment
/// is the last fragment of the record (bit value 1 implies the fragment
/// is the last fragment) and a 31-bit unsigned binary value which is the
/// length in bytes of the fragment's data.  The boolean value is the
/// highest-order bit of the header; the length is the 31 low-order bits.
/// (Note that this record specification is NOT in XDR standard form!)
async fn read_fragment(socket: &mut DuplexStream, append_to: &mut Vec<u8>) -> Result<bool, anyhow::Error> {
    let mut header_buf = [0_u8; 4];
    socket.read_exact(&mut header_buf).await?;
    let fragment_header = u32::from_be_bytes(header_buf);
    let is_last = (fragment_header & (1 << 31)) > 0;
    let length = (fragment_header & ((1 << 31) - 1)) as usize;
    trace!("Reading fragment length:{}, last:{}", length, is_last);
    let start_offset = append_to.len();
    let total = append_to.len().checked_add(length).filter(|n| *n <= 2 * 1024 * 1024)
        .ok_or_else(|| anyhow!("RPC record exceeds 2 MiB limit"))?;
    append_to.resize(total, 0);
    socket.read_exact(&mut append_to[start_offset..]).await?;
    trace!("Finishing Reading fragment length:{}, last:{}", length, is_last);
    Ok(is_last)
}

pub async fn write_fragment(socket: &mut (impl tokio::io::AsyncWrite + Unpin), buf: &[u8]) -> Result<(), anyhow::Error> {
    // TODO: split into many fragments
    assert!(buf.len() < (1 << 31));
    // set the last flag
    let fragment_header = buf.len() as u32 + (1 << 31);
    let header_buf = u32::to_be_bytes(fragment_header);
    socket.write_all(&header_buf).await?;
    trace!("Writing fragment length:{}", buf.len());
    socket.write_all(buf).await?;
    Ok(())
}

pub type SocketMessageType = Result<Vec<u8>, anyhow::Error>;

/// The Socket Message Handler reads from a TcpStream and spawns off
/// subtasks to handle each message. replies are queued into the
/// reply_send_channel.
#[derive(Debug)]
pub struct SocketMessageHandler {
    cur_fragment: Vec<u8>,
    socket_receive_channel: DuplexStream,
    reply_send_channel: mpsc::Sender<SocketMessageType>,
    context: RPCContext,
}

impl SocketMessageHandler {
    /// Creates a new SocketMessageHandler with the receiver for queued message replies
    pub fn new(context: &RPCContext) -> (Self, DuplexStream, mpsc::Receiver<SocketMessageType>) {
        let (socksend, sockrecv) = tokio::io::duplex(256000);
        let (msgsend, msgrecv) = mpsc::channel(2);
        (
            Self {
                cur_fragment: Vec::new(),
                socket_receive_channel: sockrecv,
                reply_send_channel: msgsend,
                context: context.clone(),
            },
            socksend,
            msgrecv,
        )
    }

    /// Reads a fragment from the socket. This should be looped.
    pub async fn read(&mut self) -> Result<(), anyhow::Error> {
        let is_last = read_fragment(&mut self.socket_receive_channel, &mut self.cur_fragment).await?;
        if is_last {
            let permit = self.context.requests.clone().acquire_owned().await?;
            let fragment = std::mem::take(&mut self.cur_fragment);
            let context = self.context.clone();
            let send = self.reply_send_channel.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let mut write_buf: Vec<u8> = Vec::new();
                let mut write_cursor = Cursor::new(&mut write_buf);
                let maybe_reply = handle_rpc(&mut Cursor::new(fragment), &mut write_cursor, context).await;
                match maybe_reply {
                    Err(e) => {
                        error!("RPC Error: {:?}", e);
                        let _ = send.send(Err(e)).await;
                    },
                    Ok(true) => {
                        let _ = std::io::Write::flush(&mut write_cursor);
                        let _ = send.send(Ok(write_buf)).await;
                    },
                    Ok(false) => {
                        // do not reply
                    },
                }
            });
        }
        Ok(())
    }
}
