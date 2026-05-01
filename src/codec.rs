use serde::{Serialize, de::DeserializeOwned};

pub type DecodeError = wincode::error::ReadError;
pub type EncodeError = wincode::error::WriteError;

const LIMIT_1_MIB: usize = 1024 * 1024;
const LIMIT_4_MIB: usize = 4 * LIMIT_1_MIB;
const LIMIT_16_MIB: usize = 16 * LIMIT_1_MIB;
const LIMIT_64_MIB: usize = 64 * LIMIT_1_MIB;
const LIMIT_256_MIB: usize = 256 * LIMIT_1_MIB;
const MAX_CODEC_INPUT_BYTES: usize = 1024 * LIMIT_1_MIB;
const DECODE_PREALLOCATION_FACTOR: usize = 16;

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, EncodeError> {
    serialize_with_limit::<T, MAX_CODEC_INPUT_BYTES>(value)
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    if bytes.len() > MAX_CODEC_INPUT_BYTES {
        return Err(DecodeError::PreallocationSizeLimit {
            needed: bytes.len(),
            limit: MAX_CODEC_INPUT_BYTES,
        });
    }

    let limit = decode_preallocation_limit(bytes.len());
    if limit <= LIMIT_1_MIB {
        deserialize_with_limit::<T, LIMIT_1_MIB>(bytes)
    } else if limit <= LIMIT_4_MIB {
        deserialize_with_limit::<T, LIMIT_4_MIB>(bytes)
    } else if limit <= LIMIT_16_MIB {
        deserialize_with_limit::<T, LIMIT_16_MIB>(bytes)
    } else if limit <= LIMIT_64_MIB {
        deserialize_with_limit::<T, LIMIT_64_MIB>(bytes)
    } else if limit <= LIMIT_256_MIB {
        deserialize_with_limit::<T, LIMIT_256_MIB>(bytes)
    } else {
        deserialize_with_limit::<T, MAX_CODEC_INPUT_BYTES>(bytes)
    }
}

fn serialize_with_limit<T: Serialize, const LIMIT: usize>(
    value: &T,
) -> Result<Vec<u8>, EncodeError> {
    <serde_wincode::SerdeCompat<T> as wincode::config::Serialize<_>>::serialize(
        value,
        wincode::config::Configuration::default().with_preallocation_size_limit::<LIMIT>(),
    )
}

fn deserialize_with_limit<T: DeserializeOwned, const LIMIT: usize>(
    bytes: &[u8],
) -> Result<T, DecodeError> {
    <serde_wincode::SerdeCompat<T> as wincode::config::Deserialize<_>>::deserialize(
        bytes,
        wincode::config::Configuration::default().with_preallocation_size_limit::<LIMIT>(),
    )
}

fn decode_preallocation_limit(input_len: usize) -> usize {
    input_len
        .saturating_mul(DECODE_PREALLOCATION_FACTOR)
        .clamp(LIMIT_1_MIB, MAX_CODEC_INPUT_BYTES)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct LegacyFixture {
        id: u16,
        name: String,
        values: Vec<u8>,
        optional: Option<u8>,
    }

    #[test]
    fn codec_matches_bincode_v1_default_encoding_for_serde_types() {
        let value = LegacyFixture {
            id: 0x1234,
            name: "hi".to_string(),
            values: vec![5, 6],
            optional: Some(7),
        };
        let bincode_v1_bytes = [
            0x34, 0x12, // id
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // name len
            b'h', b'i', // name
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // values len
            0x05, 0x06, // values
            0x01, // option tag: Some
            0x07, // option payload
        ];

        let encoded = super::serialize(&value).unwrap();
        assert_eq!(encoded, bincode_v1_bytes);

        let decoded: LegacyFixture = super::deserialize(&bincode_v1_bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn decode_rejects_hostile_string_length_without_unbounded_preallocation() {
        let hostile_string = ((super::LIMIT_1_MIB + 1) as u64).to_le_bytes().to_vec();
        assert_eq!(hostile_string.len(), 8);

        let result = super::deserialize::<String>(&hostile_string);
        assert!(matches!(
            result,
            Err(super::DecodeError::PreallocationSizeLimit {
                needed,
                limit: super::LIMIT_1_MIB,
            }) if needed == super::LIMIT_1_MIB + 1
        ));
    }
}
