use serde_json::{json, Value};
use std::collections::HashMap;

pub const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_NAMES: usize = 16_384;

#[derive(Default)]
pub struct BinaryReplicaCodec {
    layouts: Vec<Vec<String>>,
    layout_ids: HashMap<Vec<String>, usize>,
    entities: Vec<String>,
    name_ids: HashMap<String, usize>,
}

impl BinaryReplicaCodec {
    pub fn supports(message: &crate::ServerMessage) -> bool {
        matches!(
            message,
            crate::ServerMessage::ReplicaSnapshot { .. }
                | crate::ServerMessage::ReplicaSnapshotPage { .. }
                | crate::ServerMessage::ReplicaDelta { .. }
                | crate::ServerMessage::ReplicaTransaction { .. }
        )
    }

    pub fn client_revision_is_safe(message: &crate::ServerMessage) -> bool {
        let cursor = match message {
            crate::ServerMessage::ReplicaSnapshot { cursor, .. }
            | crate::ServerMessage::ReplicaSnapshotPage { cursor, .. }
            | crate::ServerMessage::ReplicaDelta { cursor, .. }
            | crate::ServerMessage::ReplicaTransaction { cursor, .. } => cursor,
            _ => return false,
        };
        cursor.revision <= 9_007_199_254_740_991
    }

    pub fn encode(&mut self, message: &crate::ServerMessage) -> Result<Vec<u8>, String> {
        if !Self::supports(message) {
            return Err("not a binary replica frame".into());
        }
        let offset = self.layouts.len();
        let entity_offset = self.entities.len();
        let value = serde_json::to_value(message).map_err(|e| e.to_string())?;
        let payload = self.pack(value, 0, "")?;
        if !dictionary_within_limits(&self.layouts, &self.entities) {
            return Err("replica dictionary limit".into());
        }
        let layout_ids: Vec<Vec<usize>> = self.layouts[offset..]
            .iter()
            .map(|keys| keys.iter().map(|key| self.name_ids[key]).collect())
            .collect();
        let envelope = json!([
            1,
            offset,
            layout_ids,
            self.entities[entity_offset..],
            payload
        ]);
        let bytes = rmp_serde::to_vec(&envelope).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_BYTES {
            return Err("replica frame limit".into());
        }
        Ok(bytes)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<crate::ServerMessage, String> {
        serde_json::from_value(self.decode_value(bytes)?).map_err(|e| e.to_string())
    }

    pub fn decode_value(&mut self, bytes: &[u8]) -> Result<Value, String> {
        use serde::Deserialize;
        if bytes.len() > MAX_BYTES {
            return Err("replica frame limit".into());
        }
        let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
        deserializer.set_max_depth(140);
        let envelope = Value::deserialize(&mut deserializer).map_err(|e| e.to_string())?;
        if deserializer.position() != bytes.len() as u64 {
            return Err("trailing replica bytes".into());
        }
        let parts = envelope
            .as_array()
            .filter(|v| v.len() == 5)
            .ok_or("invalid replica envelope")?;
        if parts[0] != 1 || parts[1].as_u64() != Some(self.layouts.len() as u64) {
            return Err("invalid replica dictionary offset".into());
        }
        let mut entities = self.entities.clone();
        entities.extend(
            serde_json::from_value::<Vec<String>>(parts[3].clone()).map_err(|e| e.to_string())?,
        );
        let mut layouts = self.layouts.clone();
        let additions: Vec<Vec<usize>> =
            serde_json::from_value(parts[2].clone()).map_err(|e| e.to_string())?;
        for keys in additions {
            if keys.len() > MAX_NAMES
                || keys.iter().collect::<std::collections::HashSet<_>>().len() != keys.len()
            {
                return Err("invalid replica layout".into());
            }
            layouts.push(
                keys.into_iter()
                    .map(|id| {
                        entities
                            .get(id)
                            .cloned()
                            .ok_or("unknown name ID".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        if !dictionary_within_limits(&layouts, &entities) {
            return Err("replica dictionary limit".into());
        }
        let mut nodes = 0;
        let value = unpack(&parts[4], &layouts, &entities, 0, &mut nodes)?;
        let message = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        if !Self::supports(&message) {
            return Err("not a binary replica frame".into());
        }
        self.layouts = layouts;
        self.entities = entities;
        Ok(value)
    }

    fn intern(&mut self, name: String) -> usize {
        if let Some(id) = self.name_ids.get(&name) {
            return *id;
        }
        let id = self.entities.len();
        self.entities.push(name.clone());
        self.name_ids.insert(name, id);
        id
    }

    fn pack(&mut self, value: Value, depth: usize, key: &str) -> Result<Value, String> {
        if depth > 64 {
            return Err("replica nesting limit".into());
        }
        Ok(match value {
            Value::String(name) if key == "entity" || key == "changedColumns" => {
                json!([-2, self.intern(name)])
            }
            Value::Array(values) => json!([
                -1,
                values
                    .into_iter()
                    .map(|v| self.pack(v, depth + 1, key))
                    .collect::<Result<Vec<_>, _>>()?
            ]),
            Value::Object(object) => {
                let keys: Vec<String> = object.keys().cloned().collect();
                let id = match self.layout_ids.get(&keys) {
                    Some(id) => *id,
                    None => {
                        let id = self.layouts.len();
                        for key in &keys {
                            self.intern(key.clone());
                        }
                        self.layouts.push(keys.clone());
                        self.layout_ids.insert(keys, id);
                        id
                    }
                };
                let values = object
                    .into_iter()
                    .map(|(k, v)| self.pack(v, depth + 1, &k))
                    .collect::<Result<Vec<_>, _>>()?;
                json!([id, values])
            }
            scalar => scalar,
        })
    }
}

fn unpack(
    value: &Value,
    layouts: &[Vec<String>],
    entities: &[String],
    depth: usize,
    nodes: &mut usize,
) -> Result<Value, String> {
    *nodes += 1;
    if depth > 64 || *nodes > 2_000_000 {
        return Err("replica decode limit".into());
    }
    if let Value::Array(parts) = value {
        if parts.len() != 2 {
            return Err("invalid replica value".into());
        }
        let id = parts[0].as_i64().ok_or("invalid replica ID")?;
        if id == -2 {
            let index = parts[1].as_u64().ok_or("invalid entity ID")? as usize;
            return entities
                .get(index)
                .map(|v| Value::String(v.clone()))
                .ok_or("unknown entity ID".into());
        }
        let values = parts[1].as_array().ok_or("invalid replica values")?;
        if id == -1 {
            return values
                .iter()
                .map(|v| unpack(v, layouts, entities, depth + 1, nodes))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array);
        }
        let keys = usize::try_from(id)
            .ok()
            .and_then(|id| layouts.get(id))
            .ok_or("unknown layout ID")?;
        if keys.len() != values.len() {
            return Err("invalid layout values".into());
        }
        let mut object = serde_json::Map::new();
        for (key, value) in keys.iter().zip(values) {
            object.insert(
                key.clone(),
                unpack(value, layouts, entities, depth + 1, nodes)?,
            );
        }
        Ok(Value::Object(object))
    } else if value.is_object() {
        Err("invalid replica scalar".into())
    } else {
        Ok(value.clone())
    }
}

fn dictionary_within_limits(layouts: &[Vec<String>], entities: &[String]) -> bool {
    if layouts.len() > MAX_NAMES || entities.len() > MAX_NAMES {
        return false;
    }
    let keys: usize = layouts.iter().map(Vec::len).sum();
    let bytes: usize = entities
        .iter()
        .map(|name| name.encode_utf16().count())
        .sum();
    keys <= 65_536 && bytes <= 1_048_576
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_replica_raw_round_trip_preserves_presence_and_u64() {
        let raw = json!({"type":"replica.transaction","cursor":{"epoch":"epoch","revision":u64::MAX},
            "changes":[{"entity":"tasks","id":"1","operation":"delete","oldValue":null,"changedColumns":[]}]});
        let typed: crate::ServerMessage = serde_json::from_value(raw.clone()).unwrap();
        assert!(!BinaryReplicaCodec::client_revision_is_safe(&typed));
        let mut encoder = BinaryReplicaCodec::default();
        let payload = encoder.pack(raw.clone(), 0, "").unwrap();
        let layouts: Vec<Vec<usize>> = encoder
            .layouts
            .iter()
            .map(|keys| keys.iter().map(|key| encoder.name_ids[key]).collect())
            .collect();
        let bytes = rmp_serde::to_vec(&json!([1, 0, layouts, encoder.entities, payload])).unwrap();
        let mut decoder = BinaryReplicaCodec::default();
        let decoded = decoder.decode_value(&bytes).unwrap();
        assert_eq!(decoded, raw);
        assert!(decoded["changes"][0].get("newValue").is_none());
        assert!(decoded["changes"][0].get("oldValue").unwrap().is_null());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(BinaryReplicaCodec::default()
            .decode_value(&trailing)
            .is_err());
    }

    #[test]
    fn binary_replica_envelope_preserves_values_and_session_dictionaries() {
        let message: crate::ServerMessage = serde_json::from_value(json!({
            "type":"replica.transaction", "cursor":{"epoch":"é😀","revision":9007199254740991_u64},
            "originCommandId":"origin", "changes":[{"entity":"tasks","id":"1","operation":"update",
            "oldValue":null,"newValue":{"null":null,"json":{"empty":[],"unicode":"你好"}}, "changedColumns":[]}]
        }))
        .unwrap();
        let mut codec = BinaryReplicaCodec::default();
        assert!(BinaryReplicaCodec::client_revision_is_safe(&message));
        assert_eq!(
            serde_json::to_value(&message).unwrap()["changes"][0]["oldValue"],
            Value::Null
        );
        let first = codec.encode(&message).unwrap();
        let second = codec.encode(&message).unwrap();
        let mut decoder = BinaryReplicaCodec::default();
        assert!(decoder.decode(&first[..first.len() - 1]).is_err());
        for bytes in [&first, &second] {
            let decoded = decoder.decode(bytes).unwrap();
            assert_eq!(
                serde_json::to_value(decoded).unwrap(),
                serde_json::to_value(&message).unwrap()
            );
        }
        assert!(decoder.decode(&first).is_err());
        assert!(decoder.decode(&vec![0; MAX_BYTES + 1]).is_err());
        let envelope: Value = rmp_serde::from_slice(&first).unwrap();
        assert_eq!(envelope[0], 1);
        assert_eq!(envelope[1], 0);
        assert!(envelope[3].as_array().unwrap().contains(&json!("tasks")));
        let second_envelope: Value = rmp_serde::from_slice(&second).unwrap();
        assert_eq!(second_envelope[2], json!([]));
        assert_eq!(second_envelope[3], json!([]));
        assert!(second.len() < first.len());
        assert!(!BinaryReplicaCodec::supports(
            &crate::ServerMessage::ReplicaWatermark { revision: 1 }
        ));
    }
}
