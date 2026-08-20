package sweep

import (
	"encoding/json"
	"time"

	"github.com/anvilosrs/forge/internal/hiscores"
)

// The live overlay: the plugin's absolute-value pushes, held above the hiscores until the hiscores
// catch up.
//
// A plugin push says "this account's Zulrah KC is now 1250" seconds after the kill, where the
// hiscores lag until logout. Scoring reads max(hiscores, overlay) so a tile fires live. This file is
// the other half of that bargain — pruning the overlay once it stops being ahead, because an
// overlay that only ever grows is an overlay that eventually lies.

// StaleOverlayWindow is how long an un-refreshed overlay key is trusted.
//
// OSRS force-logs-out at ~6h, so the hiscores MUST reflect real XP by then. Anything the overlay
// still holds above the hiscores past this is a bogus or doubled push, and step one below cannot
// heal it — only time can. Slightly over 6h so a full-length session's last push is not clipped.
const StaleOverlayWindow = 6*time.Hour + 30*time.Minute

// ParseOverlay reads the stored overlay blob. A corrupt blob is treated as empty rather than fatal:
// losing one account's live overlay costs a few minutes of latency, and failing the tick costs
// everyone's.
func ParseOverlay(raw []byte) map[string]int64 {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]int64
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// ParseKeyTimes reads the per-key last-rose timestamps.
func ParseKeyTimes(raw []byte) map[string]time.Time {
	if len(raw) == 0 {
		return nil
	}
	var stamps map[string]string
	if err := json.Unmarshal(raw, &stamps); err != nil {
		return nil
	}
	out := make(map[string]time.Time, len(stamps))
	for key, s := range stamps {
		// The Site writes these in two formats depending on vintage; try both rather than dropping
		// a key, because a dropped timestamp reads as "stale" and prunes a live value.
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, s); err == nil {
				out[key] = t
				break
			}
		}
	}
	return out
}

// ReconcileOverlay prunes overlay keys the hiscores have caught up to, and keys that have gone
// stale. Returns the overlay to persist and whether it differs from what was stored.
//
// Two rules, and both are needed:
//
//  1. HISCORES CAUGHT UP (h >= v). The normal case — the value was real, it has landed, the overlay
//     entry is now redundant. Keeping it is harmless but unbounded.
//
//  2. NOT REFRESHED WITHIN THE LOGOUT WINDOW. The backstop for a push that is WRONG — doubled, or
//     from a miscounted event — and therefore stuck ABOVE the hiscores where rule 1 can never
//     retire it. Without this, one bad push pins a tile's progress forever and the only fix is a
//     manual baseline reset.
func ReconcileOverlay(
	overlay map[string]int64,
	keyTimes map[string]time.Time,
	snapshot *hiscores.Snapshot,
	now time.Time,
) (map[string]int64, bool) {
	if len(overlay) == 0 {
		return nil, false
	}

	kept := make(map[string]int64, len(overlay))
	changed := false

	for key, value := range overlay {
		if actual, ranked := lookup(snapshot, key); ranked && actual >= value {
			changed = true // rule 1: the hiscores have it now
			continue
		}
		if rose, ok := keyTimes[key]; ok && now.Sub(rose) > StaleOverlayWindow {
			changed = true // rule 2: stuck above the hiscores past the logout window
			continue
		}
		kept[key] = value
	}

	if len(kept) == 0 {
		return nil, changed
	}
	return kept, changed
}

// lookup finds a key in either half of a snapshot. Overlay keys are flat — "zulrah", "mining" —
// because the plugin pushes one map for both skills and bosses.
func lookup(snapshot *hiscores.Snapshot, key string) (int64, bool) {
	if xp, ok := snapshot.SkillXp(key); ok {
		return xp, true
	}
	return snapshot.BossKc(key)
}
