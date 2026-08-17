package edge

import (
	"bytes"
	"testing"
)

// The read path's entire value is that an unchanged poll returns 304 with no body. A conditional
// request we fail to recognise costs a full payload — so every form a client might legitimately
// send has to match.
func TestMatchesETag(t *testing.T) {
	cases := []struct {
		name        string
		ifNoneMatch string
		current     string
		want        bool
	}{
		{"exact weak tag", `W/"abc"`, `W/"abc"`, true},
		{"no header", ``, `W/"abc"`, false},
		{"different tag", `W/"xyz"`, `W/"abc"`, false},
		{"wildcard", `*`, `W/"abc"`, true},
		// Some HTTP stacks and proxies strip or add the weak prefix in transit. Treating those as
		// different entities would turn every poll through such a hop into a full body.
		{"strong sent, weak stored", `"abc"`, `W/"abc"`, true},
		{"weak sent, strong stored", `W/"abc"`, `"abc"`, true},
		// A client that has cached several versions sends them all.
		{"list containing a match", `W/"old", W/"abc"`, `W/"abc"`, true},
		{"list without a match", `W/"old", W/"other"`, `W/"abc"`, false},
		{"list with spaces", ` W/"abc" `, `W/"abc"`, true},
		{"nothing stored", `W/"abc"`, ``, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesETag(tc.ifNoneMatch, tc.current); got != tc.want {
				t.Errorf("matchesETag(%q, %q) = %v, want %v", tc.ifNoneMatch, tc.current, got, tc.want)
			}
		})
	}
}

func TestBearer(t *testing.T) {
	cases := []struct{ header, want string }{
		{"Bearer abc123", "abc123"},
		{"bearer abc123", "abc123"}, // scheme is case-insensitive per RFC 7235
		{"BEARER abc123", "abc123"},
		{"Bearer  abc123  ", "abc123"},
		{"Basic abc123", ""},
		{"abc123", ""},
		{"Bearer", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := bearer(tc.header); got != tc.want {
			t.Errorf("bearer(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}

func TestAcceptsGzip(t *testing.T) {
	cases := []struct {
		header string
		want   bool
	}{
		{"gzip", true},
		{"gzip, deflate", true},
		{"deflate, gzip", true},
		{"gzip;q=1.0, identity;q=0.5", true},
		{" gzip ", true},
		{"GZIP", true},
		{"deflate", false},
		{"", false},
		// The bug worth guarding: a naive substring check matches "x-gzip" and other tokens that
		// merely contain the word.
		{"notgzip", false},
	}
	for _, tc := range cases {
		if got := acceptsGzip(tc.header); got != tc.want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
}

// Payloads are stored pre-compressed so the hot path never spends CPU producing identical bytes.
// A client that cannot take gzip still has to get valid JSON.
func TestGzipRoundTrip(t *testing.T) {
	original := []byte(`{"server":{"version":"1.0.0"},"trackedStats":["zulrah","mining"]}`)

	compressed, err := Gzip(original)
	if err != nil {
		t.Fatalf("Gzip: %v", err)
	}
	decoded, err := gunzip(compressed)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !bytes.Equal(decoded, original) {
		t.Errorf("round trip changed the bytes:\n got %s\nwant %s", decoded, original)
	}
}

func TestGzipActuallyShrinksRealisticPayloads(t *testing.T) {
	// A config payload is highly repetitive JSON — dozens of keys, long arrays of tile names. If
	// compression were not paying for itself, storing pre-compressed would be pure complexity.
	var buf bytes.Buffer
	buf.WriteString(`{"trackedKills":[`)
	for i := 0; i < 400; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`{"tileId":1234,"label":"Kill 50 Zulrah","tracked":"zulrah","goal":50}`)
	}
	buf.WriteString(`]}`)

	compressed, err := Gzip(buf.Bytes())
	if err != nil {
		t.Fatalf("Gzip: %v", err)
	}
	ratio := float64(len(compressed)) / float64(buf.Len())
	t.Logf("%d bytes -> %d bytes (%.1f%%)", buf.Len(), len(compressed), ratio*100)
	if ratio > 0.25 {
		t.Errorf("compressed to %.0f%% of original; expected far better on repetitive JSON", ratio*100)
	}
}

// A plugin token is a credential: whoever holds it can push stats as that player. The bindings
// table is the hottest read in the service, so it lands in slow-query logs, EXPLAIN output and
// backups — none of which should be able to hand one out.
func TestHashTokenDoesNotLeakTheToken(t *testing.T) {
	token := "anvil_live_super_secret_token"
	hash := HashToken(token)

	if bytes.Contains(hash, []byte(token)) {
		t.Error("hash contains the raw token")
	}
	if len(hash) != 32 {
		t.Errorf("hash is %d bytes, want 32 (sha256)", len(hash))
	}
	if !bytes.Equal(hash, HashToken(token)) {
		t.Error("hash is not stable for the same token")
	}
	if bytes.Equal(hash, HashToken(token+"x")) {
		t.Error("different tokens collide")
	}
	// Surrounding whitespace from a header must not produce a different identity.
	if !bytes.Equal(hash, HashToken("  "+token+"  ")) {
		t.Error("whitespace changes the hash")
	}
}
