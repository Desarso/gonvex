//! Connection-owned native snapshot delivery. Credits are restored only by a
//! cumulative durable-stage ACK, never by socket send completion or a retry.
use crate::replica::SnapshotPages;
use gonvex_protocol::{ClientCapabilities, ServerMessage};
use std::collections::{BTreeMap, VecDeque};

const FRAME_LIMIT: usize = 128 * 1024;
const IN_FLIGHT_BYTES: usize = 512 * 1024;
const IN_FLIGHT_FRAMES: usize = 8;
const SUBSCRIPTION_LIMIT: usize = 256;

pub(crate) fn negotiated(caps: Option<&ClientCapabilities>) -> bool {
    caps.is_some_and(|caps| {
        caps.replica_native_rows == Some(1)
            && caps.replica_set_digest == Some(1)
            && caps.replica_snapshot_pages == Some(1)
    })
}

struct Pending {
    pages: SnapshotPages,
    snapshot: String,
    sent: Option<u64>,
    acked: Option<u64>,
    in_flight: VecDeque<(u64, usize)>,
    last_sent: bool,
    buffered: Option<(Vec<u8>, u64, bool)>,
    ready: Option<ServerMessage>,
}

#[derive(Default)]
pub(crate) struct DeliveryBatch {
    pub(crate) frames: Vec<Vec<u8>>,
    pub(crate) errors: Vec<ServerMessage>,
}

#[derive(Default)]
pub(crate) struct SnapshotDelivery {
    pending: BTreeMap<String, Pending>,
}

impl SnapshotDelivery {
    pub(crate) fn clear(&mut self) {
        self.pending.clear();
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub(crate) fn remove(&mut self, id: &str) {
        self.pending.remove(id);
    }

    pub(crate) fn begin(&mut self, snapshot: ServerMessage) -> Result<(), &'static str> {
        let pages = SnapshotPages::native(snapshot);
        let (id, snapshot) = pages.identity();
        let (id, snapshot) = (id.to_owned(), snapshot.to_owned());
        if !self.pending.contains_key(&id) && self.pending.len() == SUBSCRIPTION_LIMIT {
            return Err("native snapshot subscription limit exceeded");
        }
        self.pending.insert(
            id,
            Pending {
                pages,
                snapshot,
                sent: None,
                acked: None,
                in_flight: VecDeque::new(),
                last_sent: false,
                buffered: None,
                ready: None,
            },
        );
        Ok(())
    }

    /// Reauth/context teardown removes pending deliveries. Late old-generation
    /// ACKs can race close/reopen and are harmless; future ACKs on the current
    /// generation are invalid and never mint credits.
    pub(crate) fn acknowledge(
        &mut self,
        id: &str,
        snapshot: &str,
        sequence: u64,
    ) -> Result<Option<ServerMessage>, &'static str> {
        let Some(pending) = self.pending.get_mut(id) else {
            return Ok(None);
        };
        if pending.snapshot != snapshot {
            return Ok(None);
        }
        if pending.sent.is_none_or(|sent| sequence > sent) {
            return Err("snapshot acknowledgement exceeds sent sequence");
        }
        if pending.acked.is_some_and(|acked| sequence <= acked) {
            return Ok(None);
        }
        pending.acked = Some(sequence);
        while pending
            .in_flight
            .front()
            .is_some_and(|(sent, _)| *sent <= sequence)
        {
            pending.in_flight.pop_front();
        }
        if pending.last_sent && pending.in_flight.is_empty() {
            return Ok(self.pending.remove(id).and_then(|pending| pending.ready));
        }
        Ok(None)
    }

    /// Delay certificates while their baseline is still on disk in isolation.
    /// Keep only the newest certificate, bounded by the subscription count.
    pub(crate) fn gate(&mut self, message: &ServerMessage) -> Option<ServerMessage> {
        match message {
            ServerMessage::ReplicaReady { id, .. } => {
                if let Some(pending) = self.pending.get_mut(id) {
                    pending.ready = Some(message.clone());
                    None
                } else {
                    Some(message.clone())
                }
            }
            ServerMessage::ReplicaReadyMany { ready } => {
                let mut visible = Vec::new();
                for entry in ready {
                    let Some(id) = entry.get("id").and_then(|id| id.as_str()) else {
                        continue;
                    };
                    if let Some(pending) = self.pending.get_mut(id) {
                        let mut entry = entry.clone();
                        entry["type"] = "replica.ready".into();
                        if let Ok(message) = serde_json::from_value(entry) {
                            pending.ready = Some(message);
                        }
                    } else {
                        visible.push(entry.clone());
                    }
                }
                (!visible.is_empty()).then_some(ServerMessage::ReplicaReadyMany { ready: visible })
            }
            ServerMessage::ReplicaError { id, .. } => {
                self.remove(id);
                Some(message.clone())
            }
            _ => Some(message.clone()),
        }
    }

    /// A socket turn encodes at most eight bounded frames. Across all active
    /// subscriptions only 512 KiB may await a durable client acknowledgement.
    /// A subscription may retain one unsent frame (at most 128 KiB); exact
    /// admission fills the byte window without reserving a maximum-size frame.
    pub(crate) fn pump(&mut self) -> DeliveryBatch {
        let mut bytes: usize = self
            .pending
            .values()
            .flat_map(|pending| &pending.in_flight)
            .map(|(_, bytes)| bytes)
            .sum();
        let mut count: usize = self
            .pending
            .values()
            .map(|pending| pending.in_flight.len())
            .sum();
        let mut batch = DeliveryBatch::default();
        'pump: loop {
            let mut progressed = false;
            for (id, pending) in self
                .pending
                .iter_mut()
                .filter(|(_, pending)| !pending.last_sent)
            {
                if count == IN_FLIGHT_FRAMES || bytes == IN_FLIGHT_BYTES {
                    break 'pump;
                }
                let encoded = pending.buffered.take().map(Ok).unwrap_or_else(|| {
                    pending
                        .pages
                        .next()
                        .ok_or("snapshot iterator ended without final frame")
                        .and_then(|page| {
                            let ServerMessage::ReplicaSnapshotEncodedPage {
                                sequence, last, ..
                            } = &page
                            else {
                                unreachable!()
                            };
                            let encoded = serde_json::to_vec(&page)
                                .map_err(|_| "snapshot encoding failed")?;
                            if encoded.len() > FRAME_LIMIT {
                                return Err("snapshot row exceeds native frame budget");
                            }
                            Ok((encoded, *sequence, *last))
                        })
                });
                match encoded {
                    Ok((encoded, sequence, last)) => {
                        if bytes + encoded.len() > IN_FLIGHT_BYTES {
                            pending.buffered = Some((encoded, sequence, last));
                            break 'pump;
                        }
                        pending.sent = Some(sequence);
                        pending.last_sent = last;
                        pending.in_flight.push_back((sequence, encoded.len()));
                        count += 1;
                        bytes += encoded.len();
                        batch.frames.push(encoded);
                        progressed = true;
                    }
                    Err(error) => {
                        pending.last_sent = true;
                        count -= pending.in_flight.len();
                        bytes -= pending
                            .in_flight
                            .iter()
                            .map(|(_, bytes)| bytes)
                            .sum::<usize>();
                        batch.errors.push(ServerMessage::ReplicaError {
                            id: id.clone(),
                            path: None,
                            error: error.into(),
                        });
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        for error in &batch.errors {
            if let ServerMessage::ReplicaError { id, .. } = error {
                self.pending.remove(id);
            }
        }
        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gonvex_protocol::ReplicaCursor;
    use serde_json::json;
    fn snapshot(id: &str, n: usize) -> ServerMessage {
        ServerMessage::ReplicaSnapshot {
            id: id.into(),
            key: "_id".into(),
            cursor: ReplicaCursor {
                epoch: "e".into(),
                revision: 1,
            },
            result: (0..n)
                .map(|id| json!({"_id":id,"body":"a".repeat(1000)}))
                .collect(),
            metadata: BTreeMap::from([(
                "digest".into(),
                json!("sum128-v1:00000000000000000000000000000000"),
            )]),
        }
    }
    #[test]
    fn negotiated_only_with_native_hash_and_paging_contracts() {
        let mut caps = ClientCapabilities::default();
        assert!(!negotiated(None));
        caps.replica_native_rows = Some(1);
        assert!(!negotiated(Some(&caps)));
        caps.replica_set_digest = Some(1);
        caps.replica_snapshot_pages = Some(1);
        assert!(negotiated(Some(&caps)));
        caps.replica_native_rows = Some(2);
        assert!(!negotiated(Some(&caps)));
    }
    #[test]
    fn credits_are_connection_bounded_and_duplicate_acks_do_not_mint_more() {
        let mut delivery = SnapshotDelivery::default();
        delivery.begin(snapshot("one", 2000)).unwrap();
        delivery.begin(snapshot("two", 2000)).unwrap();
        let frames = delivery.pump().frames;
        assert!(!frames.is_empty());
        assert!(frames.len() <= 8);
        assert!(frames.iter().map(Vec::len).sum::<usize>() <= IN_FLIGHT_BYTES);
        assert!(delivery.pump().frames.is_empty());
        let frame: serde_json::Value = serde_json::from_slice(&frames[0]).unwrap();
        let id = frame["id"].as_str().unwrap();
        let snapshot = frame["snapshotId"].as_str().unwrap();
        assert!(delivery.acknowledge(id, snapshot, 999).is_err());
        assert!(delivery.pump().frames.is_empty());
        delivery.acknowledge(id, snapshot, 0).unwrap();
        assert!(!delivery.pump().frames.is_empty());
        delivery.acknowledge(id, snapshot, 0).unwrap();
        assert!(delivery.pump().frames.is_empty());
        delivery.acknowledge(id, "old-generation", 999).unwrap();
        assert!(delivery.pump().frames.is_empty());
    }
    #[test]
    fn exact_frame_admission_fills_the_window_without_minting_credit() {
        let mut delivery = SnapshotDelivery::default();
        delivery.begin(snapshot("exact", 2000)).unwrap();
        let frames = delivery.pump().frames;
        assert_eq!(frames.len(), IN_FLIGHT_FRAMES);
        assert!(frames.iter().map(Vec::len).sum::<usize>() <= IN_FLIGHT_BYTES);
        let last: serde_json::Value = serde_json::from_slice(frames.last().unwrap()).unwrap();
        assert!(
            delivery
                .acknowledge(
                    "exact",
                    last["snapshotId"].as_str().unwrap(),
                    last["sequence"].as_u64().unwrap() + 1
                )
                .is_err()
        );
        assert!(delivery.pump().frames.is_empty());
    }
    #[test]
    fn a_buffered_frame_is_not_sent_or_acknowledgeable_until_credit_returns() {
        let mut delivery = SnapshotDelivery::default();
        let mut input = snapshot("large", 8);
        if let ServerMessage::ReplicaSnapshot { result, .. } = &mut input {
            for row in result {
                row["body"] = json!("x".repeat(100_000));
            }
        }
        delivery.begin(input).unwrap();
        let frames = delivery.pump().frames;
        assert_eq!(frames.len(), 5);
        let first: serde_json::Value = serde_json::from_slice(&frames[0]).unwrap();
        let generation = first["snapshotId"].as_str().unwrap();
        assert!(delivery.pending["large"].buffered.is_some());
        assert!(delivery.acknowledge("large", generation, 5).is_err());
        assert!(delivery.pump().frames.is_empty());
        delivery.acknowledge("large", generation, 0).unwrap();
        let next = delivery.pump().frames;
        assert_eq!(next.len(), 1);
        let next: serde_json::Value = serde_json::from_slice(&next[0]).unwrap();
        assert_eq!(next["sequence"], 5);
    }
    #[test]
    fn final_ack_releases_latest_certificate_and_close_releases_credits() {
        let mut delivery = SnapshotDelivery::default();
        delivery.begin(snapshot("one", 1)).unwrap();
        let frame: serde_json::Value = serde_json::from_slice(&delivery.pump().frames[0]).unwrap();
        let ready = ServerMessage::ReplicaReady {
            id: "one".into(),
            cursor: ReplicaCursor {
                epoch: "e".into(),
                revision: 5,
            },
            digest: "newer".into(),
            metadata: BTreeMap::new(),
        };
        assert!(delivery.gate(&ready).is_none());
        assert!(!delivery.is_empty());
        assert_eq!(
            delivery
                .acknowledge("one", frame["snapshotId"].as_str().unwrap(), 0)
                .unwrap(),
            Some(ready)
        );
        assert!(delivery.is_empty());
        delivery.begin(snapshot("one", 2000)).unwrap();
        delivery.pump().frames;
        delivery.clear();
        assert!(delivery.pump().frames.is_empty());
    }
    #[test]
    fn canonical_strings_preserve_hash_encoding_and_batched_certificates_wait() {
        let mut delivery = SnapshotDelivery::default();
        delivery.begin(snapshot("one", 1)).unwrap();
        let frame: serde_json::Value = serde_json::from_slice(&delivery.pump().frames[0]).unwrap();
        assert!(frame["canonicalRows"][0].is_string());
        assert!(frame.get("result").is_none());
        let ready = json!({"id":"one","cursor":{"epoch":"e","revision":9},"digest":"new"});
        assert_eq!(
            delivery.gate(&ServerMessage::ReplicaReadyMany {
                ready: vec![ready.clone(), json!({"id":"other"})]
            }),
            Some(ServerMessage::ReplicaReadyMany {
                ready: vec![json!({"id":"other"})]
            })
        );
        assert_eq!(delivery.acknowledge("one",frame["snapshotId"].as_str().unwrap(),0).unwrap().unwrap(),serde_json::from_value::<ServerMessage>(json!({"type":"replica.ready","id":"one","cursor":{"epoch":"e","revision":9},"digest":"new"})).unwrap());
    }
    #[test]
    fn oversized_rows_fail_explicitly_without_losing_other_deliveries() {
        let mut delivery = SnapshotDelivery::default();
        delivery.begin(snapshot("a-valid", 1)).unwrap();
        let mut oversized = snapshot("z-oversized", 1);
        if let ServerMessage::ReplicaSnapshot { result, .. } = &mut oversized {
            result[0]["body"] = json!("x".repeat(FRAME_LIMIT));
        }
        delivery.begin(oversized).unwrap();
        let batch = delivery.pump();
        assert_eq!(batch.frames.len(), 1);
        assert_eq!(batch.errors.len(), 1);
        assert!(
            matches!(&batch.errors[0],ServerMessage::ReplicaError{id,error,..} if id=="z-oversized" && error.contains("frame budget"))
        );
        assert!(delivery.pending.contains_key("a-valid"));
        assert!(!delivery.pending.contains_key("z-oversized"));
    }
}
