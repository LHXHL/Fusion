use std::{
    collections::{BTreeMap, VecDeque},
    io::{Error, ErrorKind},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimplexFragment {
    pub message_id: u64,
    pub index: u32,
    pub total: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ArqPacket {
    pub sequence: u64,
    pub payload: Vec<u8>,
    pub sent_at: Instant,
    pub retries: u32,
}

#[derive(Debug, Default)]
pub struct SrArqWindow {
    queue: VecDeque<ArqPacket>,
    next_sequence: u64,
}

impl SrArqWindow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }

    pub fn track(&mut self, sequence: u64, payload: Vec<u8>) {
        self.queue.push_back(ArqPacket {
            sequence,
            payload,
            sent_at: Instant::now(),
            retries: 0,
        });
    }

    pub fn enqueue(&mut self, payload: Vec<u8>) -> u64 {
        let sequence = self.allocate_sequence();
        self.track(sequence, payload);
        sequence
    }

    pub fn ack(&mut self, sequence: u64) -> bool {
        if let Some(pos) = self.queue.iter().position(|pkt| pkt.sequence == sequence) {
            self.queue.remove(pos);
            return true;
        }
        false
    }

    pub fn next_retry(&mut self, now: Instant, timeout: Duration) -> Option<ArqPacket> {
        let packet = self
            .queue
            .iter_mut()
            .find(|pkt| now.duration_since(pkt.sent_at) >= timeout)?;
        packet.sent_at = now;
        packet.retries = packet.retries.saturating_add(1);
        Some(packet.clone())
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

pub fn fragment_payload(
    message_id: u64,
    payload: &[u8],
    max_fragment_size: usize,
) -> Vec<SimplexFragment> {
    if payload.is_empty() {
        return vec![SimplexFragment {
            message_id,
            index: 0,
            total: 1,
            payload: Vec::new(),
        }];
    }

    let chunk_size = max_fragment_size.max(1);
    let total = payload.len().div_ceil(chunk_size) as u32;
    payload
        .chunks(chunk_size)
        .enumerate()
        .map(|(index, chunk)| SimplexFragment {
            message_id,
            index: index as u32,
            total,
            payload: chunk.to_vec(),
        })
        .collect()
}

pub fn reassemble_fragments(fragments: &[SimplexFragment]) -> Result<Vec<u8>, Error> {
    let first = fragments
        .first()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no fragments to reassemble"))?;
    let total = first.total;
    let message_id = first.message_id;
    if fragments.len() != total as usize {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "fragment count does not match declared total",
        ));
    }

    let mut ordered = BTreeMap::new();
    for fragment in fragments {
        if fragment.message_id != message_id || fragment.total != total {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "fragment set contains mixed message metadata",
            ));
        }
        ordered.insert(fragment.index, fragment.payload.clone());
    }

    let mut out = Vec::new();
    for index in 0..total {
        let Some(chunk) = ordered.get(&index) else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("missing fragment index {index}"),
            ));
        };
        out.extend_from_slice(chunk);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{fragment_payload, reassemble_fragments, SrArqWindow};

    #[test]
    fn fragment_and_reassemble_roundtrip() {
        let payload = b"fusion-simplex-http-fragmentation";
        let fragments = fragment_payload(7, payload, 8);
        assert!(fragments.len() > 1);
        let rebuilt = reassemble_fragments(&fragments).unwrap();
        assert_eq!(rebuilt, payload);
    }

    #[test]
    fn reassemble_rejects_missing_fragment() {
        let payload = b"abcdefghi";
        let mut fragments = fragment_payload(9, payload, 3);
        fragments.pop();
        assert!(reassemble_fragments(&fragments).is_err());
    }

    #[test]
    fn sr_arq_tracks_ack_and_retry() {
        let mut window = SrArqWindow::new();
        let seq = window.enqueue(b"hello".to_vec());
        assert_eq!(window.len(), 1);
        let retry = window
            .next_retry(
                Instant::now() + Duration::from_secs(5),
                Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(retry.sequence, seq);
        assert_eq!(retry.retries, 1);
        assert!(window.ack(seq));
        assert_eq!(window.len(), 0);
    }
}
