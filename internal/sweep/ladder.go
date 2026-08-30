// Package sweep schedules and runs the hiscores poll.
package sweep

import "time"

// The backoff ladder: the PRIORITY ORDER in which players are polled.
//
// READ docs/RATE_BUDGET.md BEFORE CHANGING ANYTHING HERE. The short version: a large community
// hiscores tracker covers a big fraction of the entire OSRS playerbase at FOUR requests per second, globally, with
// no periodic all-player sweep whatsoever. That is the real-world budget, and it is roughly 20x
// smaller than a first pass at this problem assumes.
//
// THE INTERVALS BELOW ARE DESIRES, NOT PROMISES. At 20k clans the enrolled population is ~1.2M
// accounts. A 5 req/s budget is 432,000 polls a day — about one poll per account every three days.
// There is no polite rate at which 1.2M accounts can be continuously polled on a competition
// cadence, so the scheduler's actual job is ALLOCATION: given far less budget than demand, decide
// who gets served first. `next_poll_at` is a priority key, and backlog is the expected steady
// state rather than a fault.
//
// What carries the load instead:
//
//   - THE PLUGIN. Push costs zero hiscores requests and arrives in seconds. Every point of plugin
//     adoption directly buys budget for the players who do not have it.
//   - BOUNDARIES. A mandatory poll at enrolment (the frozen baseline) and a mandatory sweep at
//     event end (closing the books exactly). These are never negotiable and they are cheap,
//     because they happen twice per event rather than continuously.
//   - ON DEMAND. A refresh button with a per-player cooldown, so anyone who cares that their
//     number is stale can fix it for one request.
//
// The ladder then spends what remains on whoever is most likely to have moved.
//
// ENROLMENT BUYS ELIGIBILITY, NOT SPEED. Clans bulk-enrol their whole roster and OSRS rosters are
// full of people who quit years ago. Granting those a fast cadence because someone ticked a box is
// the single most expensive thing the system could do, so the fast tier has to be earned by
// evidence that the account is alive.
//
// WHY BACKING OFF IS SAFE. Baselines are frozen at enrolment and gains are computed as
// (current − baseline), so a player who goes dark on day 1 and returns on day 20 still receives
// full credit for everything they did. Slow polling costs DETECTION LATENCY, never CORRECTNESS.
// That is the property that makes allocation under scarcity acceptable at all; without frozen
// baselines, a starved queue would be silently dropping points.

// Tier is a coarse polling band. Stored alongside the streak so the distribution is queryable —
// "what fraction of enrolled players are dormant?" is the number that tells a clan its roster
// needs trimming, and the number that predicts our bill.
type Tier int16

const (
	TierHot     Tier = 0 // gained on the last poll, or currently logged in
	TierWarm    Tier = 1
	TierCool    Tier = 2
	TierIdle    Tier = 3
	TierDormant Tier = 4 // enrolled but showing no sign of life — where quit accounts land
)

func (t Tier) String() string {
	switch t {
	case TierHot:
		return "hot"
	case TierWarm:
		return "warm"
	case TierCool:
		return "cool"
	case TierIdle:
		return "idle"
	default:
		return "dormant"
	}
}

// Interval is the DESIRED gap before a player on this tier is polled again — the position they are
// given in the queue, not a guarantee they will be served then. Under a saturated budget every tier
// stretches proportionally, which is the correct degradation: the ordering between tiers is what
// matters, and it survives.
//
// The top matches what a live competition needs to feel responsive. The bottom is set by
// arithmetic: dormant accounts at 24h contribute a few percent of demand instead of most of it,
// which is what makes bulk-enrolled dead rosters a non-problem without quotas or enforcement.
func (t Tier) Interval() time.Duration {
	switch t {
	case TierHot:
		return 15 * time.Minute
	case TierWarm:
		return 30 * time.Minute
	case TierCool:
		return 2 * time.Hour
	case TierIdle:
		return 6 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// TierFor maps a consecutive-miss count onto a band.
//
// The steps widen rather than doubling smoothly, because the thing being modelled is not a decay
// curve but a question with three answers: are they playing right now, did they play recently, or
// have they stopped? A player who quits reaches dormant in about a day and a half of real time,
// which is fast enough to matter for cost and slow enough that a weekend break does not look like
// quitting.
func TierFor(missStreak int) Tier {
	switch {
	case missStreak <= 0:
		return TierHot
	case missStreak == 1:
		return TierWarm
	case missStreak <= 3:
		return TierCool
	case missStreak <= 7:
		return TierIdle
	default:
		return TierDormant
	}
}

// LiveWindow is how recently a plugin heartbeat counts as "logged in right now".
//
// Generous on purpose: the cost of believing a stale heartbeat is one extra poll, while the cost
// of disbelieving a real one is a visibly lagging leaderboard for someone who is actively playing
// and watching it.
const LiveWindow = 30 * time.Minute

// Decision is the outcome of scheduling one player.
type Decision struct {
	Tier        Tier
	MissStreak  int
	NextPollAt  time.Time
	ErrorStreak int
	// Reason names which rule fired, for the run log. Worth carrying: when the queue misbehaves
	// the question is always "why is this player being polled this often", and a tier alone does
	// not answer it.
	Reason string
}

// Input is everything the ladder needs to know about one player.
type Input struct {
	// MissStreak before this poll.
	MissStreak int
	// ErrorStreak before this poll.
	ErrorStreak int
	// Changed reports whether this poll found new stats. Ignored unless Outcome is OK.
	Changed bool
	// Outcome of the fetch: "ok", "unranked", or "transient".
	Outcome string
	// LiveSeenAt is the last plugin heartbeat, zero if never.
	LiveSeenAt time.Time
	// Now is the poll time.
	Now time.Time
}

// Next computes where a player lands after a poll.
func Next(in Input) Decision {
	switch in.Outcome {
	case "transient":
		// Our failure, not their inactivity — so the miss ladder is left untouched and a separate,
		// steeper backoff applies. Conflating the two would park an actively-playing account on the
		// dormant tier because of a network blip on our side.
		streak := in.ErrorStreak + 1
		return Decision{
			Tier:        TierFor(in.MissStreak),
			MissStreak:  in.MissStreak,
			ErrorStreak: streak,
			NextPollAt:  in.Now.Add(errorBackoff(streak)),
			Reason:      "transient-error",
		}

	case "unranked":
		// Terminal for now: the player is taken out of the sweep entirely (players.status flips)
		// and a re-probe job decides when to try again. Push the queue position out so that if the
		// status flip fails for any reason, we still are not hammering a 404 every tick.
		return Decision{
			Tier:        TierDormant,
			MissStreak:  in.MissStreak + 1,
			ErrorStreak: 0,
			NextPollAt:  in.Now.Add(TierDormant.Interval()),
			Reason:      "unranked",
		}
	}

	// A heartbeat outranks everything. They are logged in, so whatever the ladder had built up
	// while they were away is stale information — this is the rule that means a player running the
	// plugin never sits on a slow tier during a session.
	if !in.LiveSeenAt.IsZero() && in.Now.Sub(in.LiveSeenAt) <= LiveWindow {
		return Decision{
			Tier:       TierHot,
			MissStreak: 0,
			NextPollAt: in.Now.Add(TierHot.Interval()),
			Reason:     "live-heartbeat",
		}
	}

	if in.Changed {
		return Decision{
			Tier:       TierHot,
			MissStreak: 0,
			NextPollAt: in.Now.Add(TierHot.Interval()),
			Reason:     "gained",
		}
	}

	miss := in.MissStreak + 1
	tier := TierFor(miss)
	return Decision{
		Tier:       tier,
		MissStreak: miss,
		NextPollAt: in.Now.Add(tier.Interval()),
		Reason:     "no-change",
	}
}

// errorBackoff grows fast and caps at an hour. Unlike the miss ladder this is about protecting the
// other side: if the hiscores are down or rate limiting us, polling harder makes it worse.
func errorBackoff(streak int) time.Duration {
	switch {
	case streak <= 1:
		return 5 * time.Minute
	case streak == 2:
		return 15 * time.Minute
	case streak == 3:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}

// MinPollInterval is the floor between two polls of the SAME player, whatever else happens.
//
// Without it the promotion path is an amplifier: a refresh button, a burst of plugin heartbeats and
// a submission arriving together would each pull the same player to the front of the queue, and one
// enthusiastic user could spend a visible share of a 5 req/s budget on themselves. This 60-second
// minimum-poll guard at this value is the established practice.
const MinPollInterval = 60 * time.Second

// PromoteNow is the "earned fast tier" side of the ladder: any evidence that a player is alive
// pulls them back to the front of the queue, whatever tier they had decayed to — but never nearer
// than MinPollInterval since their last poll.
//
// The signals, all of which the Site pushes:
//   - a plugin heartbeat
//   - a manual submission or proof upload in a live event
//   - the player opening the site or their team page
//   - enrolment in a new competition (which also needs a baseline immediately)
//   - an event ending (the mandatory final sweep that closes the books)
//
// This is what makes a slow dormant cadence acceptable: anyone who cares that their numbers are
// stale has several ways to fix it that cost one request, instead of us paying a permanent floor
// for everyone on the chance that someone might care.
func PromoteNow(now time.Time, lastPolledAt time.Time) Decision {
	at := now
	if !lastPolledAt.IsZero() {
		if earliest := lastPolledAt.Add(MinPollInterval); earliest.After(at) {
			at = earliest
		}
	}
	return Decision{
		Tier:       TierHot,
		MissStreak: 0,
		NextPollAt: at,
		Reason:     "promoted",
	}
}
