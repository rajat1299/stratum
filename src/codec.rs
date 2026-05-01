use serde::{Serialize, de::DeserializeOwned};

pub type DecodeError = wincode::error::ReadError;
pub type EncodeError = wincode::error::WriteError;

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, EncodeError> {
    <serde_wincode::SerdeCompat<T> as wincode::config::Serialize<_>>::serialize(
        value,
        codec_config(),
    )
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    <serde_wincode::SerdeCompat<T> as wincode::config::Deserialize<_>>::deserialize(
        bytes,
        codec_config(),
    )
}

fn codec_config() -> impl wincode::config::Config {
    wincode::config::Configuration::default().disable_preallocation_size_limit()
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
}
