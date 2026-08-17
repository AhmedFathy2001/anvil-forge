package hiscores

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// The differential test that matters: parse a REAL hiscores response and require the result to
// equal, field for field, what Anvil.Site's osrs-json-hiscores parser produces for the same bytes.
//
// This is the one place a port can be wrong without anything failing loudly — a boss key that
// doesn't match just scores zero forever. Regenerate the golden after bumping the lib:
//
//	node scripts/gen-hiscores-names.mjs
//	node scripts/gen-golden.mjs
func TestParseMatchesSiteParser(t *testing.T) {
	rawBytes, err := os.ReadFile("testdata/lynx_titan.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	goldenBytes, err := os.ReadFile("testdata/lynx_titan.golden.json")
	if err != nil {
		t.Fatalf("reading golden: %v", err)
	}

	var raw rawStats
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		t.Fatalf("decoding fixture: %v", err)
	}
	got := parse(&raw)

	// Compare as generic JSON so field order and Go/JS numeric representation don't matter —
	// only the values do.
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshalling parsed: %v", err)
	}
	var gotAny, wantAny any
	if err := json.Unmarshal(gotJSON, &gotAny); err != nil {
		t.Fatalf("re-decoding parsed: %v", err)
	}
	if err := json.Unmarshal(goldenBytes, &wantAny); err != nil {
		t.Fatalf("decoding golden: %v", err)
	}

	if !reflect.DeepEqual(gotAny, wantAny) {
		for _, diff := range diffJSON("", wantAny, gotAny) {
			t.Errorf("%s", diff)
		}
	}
}

func TestParseRealValues(t *testing.T) {
	rawBytes, err := os.ReadFile("testdata/lynx_titan.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	var raw rawStats
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		t.Fatalf("decoding fixture: %v", err)
	}
	snap := parse(&raw)

	// 4.6 billion overall XP is the whole reason XP is int64 — it overflows int32 by 2x. If this
	// ever reads back as a negative or truncated number, the change detector inverts and every
	// maxed account looks like it is losing XP every tick.
	if got := snap.OverallXp(); got != 4_600_000_000 {
		t.Errorf("OverallXp() = %d, want 4600000000", got)
	}
	if e := snap.Skills["attack"]; e.Level != 99 || e.Xp != 200_000_000 {
		t.Errorf("attack = %+v, want level 99 xp 200000000", e)
	}
	// The extra-boss merge: both of these post-date the pinned lib release, so they only appear
	// if extraBosses did its job.
	for _, key := range []string{"maggotKing", "madAngel"} {
		if _, ok := snap.Bosses[key]; !ok {
			t.Errorf("boss %q missing — extraBosses merge did not run", key)
		}
	}
	if len(snap.BountyHunter) != 4 {
		t.Errorf("bountyHunter has %d modes, want 4 (current + legacy)", len(snap.BountyHunter))
	}
}

// An unranked player is the common case for a fresh account, and the -1 sentinel is what stops a
// first kill from scoring as though it were the hundredth.
func TestParseUnrankedIsMinusOne(t *testing.T) {
	var raw rawStats // no skills, no activities
	snap := parse(&raw)

	if e := snap.Skills["attack"]; e != UnrankedSkill {
		t.Errorf("missing skill = %+v, want %+v", e, UnrankedSkill)
	}
	if e := snap.Bosses["zulrah"]; e != Unranked {
		t.Errorf("missing boss = %+v, want %+v", e, Unranked)
	}
	// ...but the clamped accessors present it as zero, so arithmetic never sees the sentinel.
	if xp := snap.OverallXp(); xp != 0 {
		t.Errorf("OverallXp() on unranked = %d, want 0", xp)
	}
	if kc, ranked := snap.BossKc("zulrah"); kc != 0 || ranked {
		t.Errorf("BossKc(zulrah) = (%d, %v), want (0, false)", kc, ranked)
	}
}

// diffJSON walks two decoded JSON values and reports the leaf paths that differ, so a failure
// names the boss key rather than dumping two 6 KB blobs.
func diffJSON(path string, want, got any) []string {
	if reflect.DeepEqual(want, got) {
		return nil
	}
	wantMap, wOK := want.(map[string]any)
	gotMap, gOK := got.(map[string]any)
	if !wOK || !gOK {
		return []string{path + ": want " + render(want) + ", got " + render(got)}
	}

	var out []string
	for key, wantVal := range wantMap {
		gotVal, present := gotMap[key]
		if !present {
			out = append(out, path+"."+key+": missing (want "+render(wantVal)+")")
			continue
		}
		out = append(out, diffJSON(path+"."+key, wantVal, gotVal)...)
	}
	for key, gotVal := range gotMap {
		if _, present := wantMap[key]; !present {
			out = append(out, path+"."+key+": unexpected (got "+render(gotVal)+")")
		}
	}
	return out
}

func render(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unrenderable>"
	}
	return string(b)
}
