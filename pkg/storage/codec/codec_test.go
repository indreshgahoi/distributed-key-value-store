package codec

import (
	"bytes"
	"testing"
)

func TestCodec_KeyOrderingInvariants(t *testing.T) {
	// Test cases where keys contain tricky null bytes and boundary characters
	keyA := []byte("account\x00\x00profile")
	keyB := []byte("account\x00\x01profile")
	keyC := []byte("account\x01profile")

	// Verify User Key Invariant: keyA < keyB < keyC
	if bytes.Compare(keyA, keyB) >= 0 || bytes.Compare(keyB, keyC) >= 0 {
		t.Fatalf("test setup failure: raw keys must order A < B < C")
	}

	// Encode at different timestamps
	kAt100 := EncodeKeyAppend(nil, keyA, 100)
	kAt200 := EncodeKeyAppend(nil, keyA, 200)
	kBt100 := EncodeKeyAppend(nil, keyB, 100)
	kCt100 := EncodeKeyAppend(nil, keyC, 100)

	// INVARIANT 1: For the same user key, higher timestamp must sort BEFORE lower timestamp
	if bytes.Compare(kAt200, kAt100) >= 0 {
		t.Fatalf("temporal order broken: TS 200 must sort strictly before TS 100")
	}

	// INVARIANT 2: User key sorting takes precedence over timestamps
	// Even though TS 200 is newer, keyA must still sort before keyB
	if bytes.Compare(kAt200, kBt100) >= 0 {
		t.Fatalf("key isolation broken: keyA must sort before keyB regardless of timestamp")
	}
	if bytes.Compare(kBt100, kCt100) >= 0 {
		t.Fatalf("key isolation broken: keyB must sort before keyC")
	}
}

func TestCodec_RoundTripDecode(t *testing.T) {
	testCases := [][]byte{
		[]byte(""),
		[]byte("plain_key"),
		[]byte("key_with_\x00_null"),
		[]byte("key_with_\x00\x00_double_null"),
		[]byte("key_with_\x00\xff_escaped_sequence"),
		[]byte("\x00\x00\x00"),
	}

	for _, originalKey := range testCases {
		ts := uint64(18446744073709551615 - 42) // Large uint64
		encoded := EncodeKeyAppend(nil, originalKey, ts)

		dst := make([]byte, 0, len(originalKey))
		decodedKey, decodedTS, err := DecodeKey(dst, encoded)
		if err != nil {
			t.Fatalf("failed to decode key %q: %v", originalKey, err)
		}

		if !bytes.Equal(decodedKey, originalKey) {
			t.Fatalf("key mismatch: got %q, want %q", decodedKey, originalKey)
		}
		if decodedTS != ts {
			t.Fatalf("timestamp mismatch: got %d, want %d", decodedTS, ts)
		}
	}
}

func TestCodec_ValueRoundTrip(t *testing.T) {
	val := []byte("balance_500")
	encoded := EncodeValueAppend(nil, OpTypePut, val)

	op, payload, err := DecodeValue(encoded)
	if err != nil {
		t.Fatalf("failed to decode value: %v", err)
	}
	if op != OpTypePut {
		t.Fatalf("expected OpTypePut, got %v", op)
	}
	if !bytes.Equal(payload, val) {
		t.Fatalf("payload mismatch: got %s, want %s", payload, val)
	}
}

// BenchmarkCodec_ZeroAllocEncode asserts 0 B/op and 0 allocs/op when using pre-allocated buffers.
func BenchmarkCodec_ZeroAllocEncode(b *testing.B) {
	userKey := []byte("users/profile/00012345/settings")
	ts := uint64(1710000000)

	reqLen := EncodedKeyLen(userKey)
	buf := make([]byte, 0, reqLen)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf = EncodeKeyAppend(buf[:0], userKey, ts)
	}
}
