#![cfg(test)]

mod digest_utils {
    use redis::{calculate_value_digest, is_valid_16_bytes_hex_digest};

    const TEST_VALUE: &str = "test_value";
    const VALUE_WITH_LEADING_ZEROES_DIGEST: &str =
        "v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd";

    #[test]
    fn test_calculate_value_digest_basic() {
        // Should return a 16-character hex string (64-bit hash)
        assert!(is_valid_16_bytes_hex_digest(&calculate_value_digest(
            TEST_VALUE
        )));
    }

    #[test]
    fn test_calculate_value_digest_leading_zeroes() {
        let digest = calculate_value_digest(VALUE_WITH_LEADING_ZEROES_DIGEST);
        // Validate that the 1st four bytes are all zeroes and they are not omitted
        assert_eq!(&digest[..4], "0000");
        assert!(is_valid_16_bytes_hex_digest(&digest));
    }

    #[test]
    fn test_calculate_value_digest_consistency() {
        // Same input should always produce the same digest
        assert_eq!(
            calculate_value_digest(TEST_VALUE),
            calculate_value_digest(TEST_VALUE)
        );
    }

    #[test]
    fn test_calculate_value_digest_different_values() {
        // Different inputs should produce different digests
        assert_ne!(
            calculate_value_digest("value1"),
            calculate_value_digest("value2")
        );
    }

    #[test]
    fn test_calculate_value_digest_numeric() {
        // Test with numeric values
        assert!(is_valid_16_bytes_hex_digest(&calculate_value_digest(42i32)));
    }

    #[test]
    fn test_calculate_value_digest_bytes() {
        // Test with byte arrays
        assert!(is_valid_16_bytes_hex_digest(&calculate_value_digest(
            TEST_VALUE.as_bytes()
        )));
    }

    #[test]
    fn test_calculate_value_digest_empty() {
        // Test with empty string
        assert!(is_valid_16_bytes_hex_digest(&calculate_value_digest("")));
    }
}
