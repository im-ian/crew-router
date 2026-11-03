//! Names, account ids and tokens.

use base64::Engine;
use rand::Rng;
use sha2::{Digest, Sha256};

pub const NAME_MAX_CHARS: usize = 32;

/// 1–32 characters, each a letter or digit in any script, `-` or `_`.
pub fn valid_name(name: &str) -> bool {
    let count = name.chars().count();
    (1..=NAME_MAX_CHARS).contains(&count)
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// The key names are unique under: case is ignored.
pub fn name_key(name: &str) -> String {
    name.to_lowercase()
}

/// `acc_` and 20 lowercase letters or digits.
pub fn new_account_id() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let tail: String = (0..20)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect();
    format!("acc_{tail}")
}

/// `crt_` and 32 random bytes in unpadded base64url.
pub fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    format!(
        "crt_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

/// What the database keeps instead of the token.
pub fn token_hash(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(valid_name("minji"));
        assert!(valid_name("민지-bot_2"));
        assert!(valid_name(&"a".repeat(32)));
        assert!(!valid_name(&"a".repeat(33)));
        assert!(valid_name(&"가".repeat(32)));
        assert!(!valid_name(""));
        assert!(!valid_name("a b"));
        assert!(!valid_name("a/b"));
        assert!(!valid_name("@a"));
        assert_eq!(name_key("MinJi"), name_key("minji"));
    }

    #[test]
    fn account_ids_are_ascii() {
        for _ in 0..100 {
            let id = new_account_id();
            let tail = id.strip_prefix("acc_").unwrap();
            assert_eq!(tail.len(), 20);
            assert!(tail
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        }
    }

    #[test]
    fn tokens() {
        let t = new_token();
        assert!(t.starts_with("crt_"));
        assert_eq!(t.len(), 4 + 43);
        assert_ne!(t, new_token());
        assert_eq!(token_hash(&t).len(), 64);
    }
}
