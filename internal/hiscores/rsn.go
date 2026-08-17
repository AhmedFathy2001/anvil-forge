package hiscores

import (
	"regexp"
	"strings"
	"unicode"
)

// RSN handling, ported from Anvil.Site's src/lib/auth.ts. Kept together so the three different
// notions of "the same name" stay visibly distinct — mixing them up is the drift bug that splits
// one player into two rows.

var plausibleRsn = regexp.MustCompile(`^[A-Za-z0-9 _-]{1,12}$`)

// SanitizeRsn is display-side cleanup: collapse any Unicode whitespace to a single ASCII space and
// trim the edges, preserving casing.
//
// The non-obvious case is U+00A0. OSRS encodes in-game name spaces as non-breaking spaces to stop
// them line-wrapping, and osrs-json-hiscores' own validator rejects U+00A0 outright — so a name
// that arrived from the game client 404s forever unless it is normalised before the lookup.
func SanitizeRsn(rsn string) string {
	var b strings.Builder
	b.Grow(len(rsn))
	lastWasSpace := false
	for _, r := range rsn {
		if unicode.IsSpace(r) {
			if !lastWasSpace {
				b.WriteRune(' ')
				lastWasSpace = true
			}
			continue
		}
		b.WriteRune(r)
		lastWasSpace = false
	}
	return strings.TrimSpace(b.String())
}

// NormalizeRsn is the identity key: lowercased, with space and underscore collapsed to one space.
//
// OSRS treats space and underscore as the same character in a name — display names render with
// spaces, while logins, hiscores lookups and hand-entered rows use underscores. Without this,
// "GIM_nisbro" and "GIM Nisbro" resolve to two different players, and a roster sync reports one
// person as having both left and joined.
//
// NOTE: this is a stored key. Changing it requires backfilling every normalised column.
func NormalizeRsn(rsn string) string {
	lowered := strings.ToLower(strings.TrimSpace(rsn))
	var b strings.Builder
	b.Grow(len(lowered))
	lastWasSep := false
	for _, r := range lowered {
		if unicode.IsSpace(r) || r == '_' {
			if !lastWasSep {
				b.WriteRune(' ')
				lastWasSep = true
			}
			continue
		}
		b.WriteRune(r)
		lastWasSep = false
	}
	return strings.TrimSpace(b.String())
}

// IsPlausibleRsn reports whether a string could be a real OSRS account name: 1-12 characters of
// letters, digits, space, underscore or hyphen.
//
// RuneLite hands us placeholders for clan members whose name it cannot resolve ("#Player1404").
// Those can never be looked up, so polling them is pure waste — this is what keeps them out of the
// sweep queue entirely rather than burning a request each tick to learn they 404.
func IsPlausibleRsn(rsn string) bool {
	return plausibleRsn.MatchString(rsn)
}
