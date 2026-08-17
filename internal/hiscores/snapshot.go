// Package hiscores reads the official OSRS hiscores and shapes the result exactly as
// Anvil.Site's parseJsonStats does.
//
// Two things here are load-bearing and easy to get wrong in a port:
//
//   - MISSING MEANS -1, NOT 0. The hiscores omit anything a player has never ranked in, and the
//     Site's parser fills those with -1. Scoring code reads that difference: a boss at 0 means
//     "ranked with zero kills", a boss at -1 means "never appeared". Defaulting to 0 here would
//     hand every unranked account a free baseline and let a first kill score as if it were the
//     hundredth.
//
//   - XP IS int64. A maxed account carries ~4.6B total XP, which overflows int32. The Site gets
//     this for free from JS numbers; we have to say it.
package hiscores

import "encoding/json"

// SkillEntry is one skill's hiscores row. Rank/Level/Xp are -1 when the player is unranked in it.
type SkillEntry struct {
	Rank  int64 `json:"rank"`
	Level int64 `json:"level"`
	Xp    int64 `json:"xp"`
}

// ScoreEntry is one activity/boss/clue row. Rank/Score are -1 when the player is unranked in it.
type ScoreEntry struct {
	Rank  int64 `json:"rank"`
	Score int64 `json:"score"`
}

// Unranked is the value the official hiscores omission maps to, matching osrs-json-hiscores.
var Unranked = ScoreEntry{Rank: -1, Score: -1}

// UnrankedSkill is the skill equivalent of Unranked.
var UnrankedSkill = SkillEntry{Rank: -1, Level: -1, Xp: -1}

// BountyHunter is keyed by mode. Four ladders, not two: the current `hunterV2`/`rogueV2` plus the
// `hunter`/`rogue` legacy pair the hiscores still publish. The Site keeps all four, so we do too.

// Snapshot is one player's full hiscores read.
//
// Field order matches the Site's object-literal order so a JSON dump reads the same way side by
// side. Key order WITHIN the maps differs (Go sorts, JS preserves insertion order) — that is fine
// because every consumer parses rather than compares bytes, and Postgres jsonb normalises anyway.
type Snapshot struct {
	Skills            map[string]SkillEntry `json:"skills"`
	LeaguePoints      ScoreEntry            `json:"leaguePoints"`
	DeadmanPoints     ScoreEntry            `json:"deadmanPoints"`
	BountyHunter      map[string]ScoreEntry `json:"bountyHunter"`
	LastManStanding   ScoreEntry            `json:"lastManStanding"`
	PvpArena          ScoreEntry            `json:"pvpArena"`
	SoulWarsZeal      ScoreEntry            `json:"soulWarsZeal"`
	RiftsClosed       ScoreEntry            `json:"riftsClosed"`
	ColosseumGlory    ScoreEntry            `json:"colosseumGlory"`
	CollectionsLogged ScoreEntry            `json:"collectionsLogged"`
	Clues             map[string]ScoreEntry `json:"clues"`
	Bosses            map[string]ScoreEntry `json:"bosses"`
}

// OverallXp is the change detector the whole backoff ladder turns on. Clamped at 0 so an unranked
// account (-1) compares equal to a fresh one rather than looking like a loss.
func (s *Snapshot) OverallXp() int64 {
	if s == nil {
		return 0
	}
	e, ok := s.Skills["overall"]
	if !ok || e.Xp < 0 {
		return 0
	}
	return e.Xp
}

// SkillXp returns a skill's XP clamped at 0, and whether the player is ranked in it.
func (s *Snapshot) SkillXp(key string) (int64, bool) {
	if s == nil {
		return 0, false
	}
	e, ok := s.Skills[key]
	if !ok || e.Xp < 0 {
		return 0, false
	}
	return e.Xp, true
}

// BossKc returns a boss's kill count clamped at 0, and whether the player is ranked in it.
func (s *Snapshot) BossKc(key string) (int64, bool) {
	if s == nil {
		return 0, false
	}
	e, ok := s.Bosses[key]
	if !ok || e.Score < 0 {
		return 0, false
	}
	return e.Score, true
}

// MarshalJSON is the default; declared explicitly only to document that the stored blob is the
// contract with Anvil.Site's TypeScript readers and must not gain or lose top-level fields.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	type alias Snapshot // break the recursion
	return json.Marshal(alias(s))
}

// Deltas is what moved between two snapshots — only metrics that actually changed, matching the
// Site's computeDeltas. Absent keys are the common case and the reason the daily history row stays
// ~150 bytes instead of ~3 KB.
type Deltas struct {
	Skills map[string]int64 `json:"skills,omitempty"`
	Bosses map[string]int64 `json:"bosses,omitempty"`
}

// Empty reports whether nothing moved.
func (d Deltas) Empty() bool { return len(d.Skills) == 0 && len(d.Bosses) == 0 }

// ComputeDeltas reports what rose between two snapshots.
//
// A nil `before` yields nothing on purpose: a player's first-ever snapshot would otherwise book a
// decade of someone else's progress as gains on the day they joined.
func ComputeDeltas(before, after *Snapshot) Deltas {
	var d Deltas
	if after == nil || before == nil {
		return d
	}
	for key, entry := range after.Skills {
		if key == "overall" {
			continue // stored as its own column; repeating it here is noise
		}
		now := clamp0(entry.Xp)
		then := clamp0(before.Skills[key].Xp)
		if now > then {
			if d.Skills == nil {
				d.Skills = map[string]int64{}
			}
			d.Skills[key] = now - then
		}
	}
	for key, entry := range after.Bosses {
		now := clamp0(entry.Score)
		then := clamp0(before.Bosses[key].Score)
		if now > then {
			if d.Bosses == nil {
				d.Bosses = map[string]int64{}
			}
			d.Bosses[key] = now - then
		}
	}
	return d
}

// MergeDeltas adds one tick's deltas onto a day's running total.
//
// This exists because a day is many ticks and each one only reports movement SINCE THE LAST FETCH.
// Overwriting instead of merging collapses a day to its final window, which biases hardest against
// the most active players — they get polled every tick (gaining resets the backoff) and so keep
// only their last slice, while an idle player polled once every two hours banks a whole session in
// one delta. That inverts the leaderboard it feeds.
func MergeDeltas(before Deltas, add Deltas) Deltas {
	out := Deltas{}
	out.Skills = mergeCounts(before.Skills, add.Skills)
	out.Bosses = mergeCounts(before.Bosses, add.Bosses)
	return out
}

func mergeCounts(before, add map[string]int64) map[string]int64 {
	if len(before) == 0 && len(add) == 0 {
		return nil
	}
	out := make(map[string]int64, len(before)+len(add))
	for k, v := range before {
		out[k] = v
	}
	for k, v := range add {
		out[k] += v
	}
	return out
}

func clamp0(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
