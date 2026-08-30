package sweep

import (
	"testing"
	"time"
)

var now = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func TestGainKeepsPlayerHot(t *testing.T) {
	// However far a player has decayed, one detected gain puts them straight back to the top. This
	// is the self-healing case: a returning account fixes its own cadence on the first poll that
	// sees it, without anyone having to notice.
	for _, missStreak := range []int{0, 1, 5, 40} {
		d := Next(Input{MissStreak: missStreak, Changed: true, Outcome: "ok", Now: now})
		if d.Tier != TierHot {
			t.Errorf("missStreak=%d changed: tier = %v, want hot", missStreak, d.Tier)
		}
		if d.MissStreak != 0 {
			t.Errorf("missStreak=%d changed: streak = %d, want 0", missStreak, d.MissStreak)
		}
		if got := d.NextPollAt.Sub(now); got != 15*time.Minute {
			t.Errorf("missStreak=%d changed: next in %v, want 15m", missStreak, got)
		}
	}
}

// walkQuitAccount simulates an account that stops playing, returning when it is first classified
// dormant. `at` is the elapsed time OF that poll — not of the next one it schedules — because the
// question being asked is "how long does a quit account keep costing us fast-tier requests", and
// the 24-hour wait it then books is already the cheap state.
func walkQuitAccount(t *testing.T) (polls int, at time.Duration) {
	t.Helper()
	miss := 0
	for {
		d := Next(Input{MissStreak: miss, Outcome: "ok", Changed: false, Now: now.Add(at)})
		polls++
		miss = d.MissStreak
		if d.Tier == TierDormant {
			return polls, at
		}
		at = d.NextPollAt.Sub(now)
		if polls > 100 {
			t.Fatal("ladder never reaches dormant")
		}
	}
}

func TestLadderDecaysMonotonically(t *testing.T) {
	// Each idle poll must push the next one further out, never nearer. A non-monotonic ladder
	// would let an account oscillate between tiers and poll more often the longer it stayed idle.
	var prev time.Duration
	at := time.Duration(0)
	miss := 0
	for poll := 0; poll < 12; poll++ {
		d := Next(Input{MissStreak: miss, Outcome: "ok", Changed: false, Now: now.Add(at)})
		gap := d.NextPollAt.Sub(now.Add(at))
		if gap < prev {
			t.Errorf("poll %d: gap %v shrank from %v", poll, gap, prev)
		}
		prev = gap
		miss = d.MissStreak
		at = d.NextPollAt.Sub(now)
	}
	if TierFor(miss) != TierDormant {
		t.Errorf("after 12 idle polls: tier = %v, want dormant", TierFor(miss))
	}
}

func TestTimeToDormant(t *testing.T) {
	// The number that sets how long a quit account keeps costing the fast-tier rate before it
	// settles. Wants to be short enough to matter for cost, and long enough that a weekend away
	// does not read as quitting.
	polls, at := walkQuitAccount(t)
	t.Logf("quit account classified dormant after %d polls / %v", polls, at)

	if at > 48*time.Hour {
		t.Errorf("takes %v to reach dormant, want under 48h", at)
	}
	if at < 12*time.Hour {
		t.Errorf("reaches dormant in %v — too eager; a weekend off would look like quitting", at)
	}
	// Total requests spent learning that one account has stopped playing. This multiplies by
	// however many accounts quit, so it is worth keeping in single digits.
	if polls > 10 {
		t.Errorf("spent %d requests to classify one quit account, want <= 10", polls)
	}
}

func TestHeartbeatOverridesBackoff(t *testing.T) {
	// A plugin push means they are logged in RIGHT NOW. Even with no XP gain since the last poll
	// (they might be questing, or between kills), that outranks a decayed tier — otherwise a
	// player running the plugin could sit on a 6-hour cadence during an active session.
	d := Next(Input{
		MissStreak: 9, // deep in dormant
		Outcome:    "ok",
		Changed:    false,
		LiveSeenAt: now.Add(-5 * time.Minute),
		Now:        now,
	})
	if d.Tier != TierHot {
		t.Errorf("tier = %v, want hot", d.Tier)
	}
	if d.Reason != "live-heartbeat" {
		t.Errorf("reason = %q, want live-heartbeat", d.Reason)
	}
}

func TestStaleHeartbeatDoesNotOverride(t *testing.T) {
	// A heartbeat from hours ago is not evidence of anything; they logged off. Honouring it would
	// pin every player who ever installed the plugin to the hot tier permanently.
	d := Next(Input{
		MissStreak: 4,
		Outcome:    "ok",
		Changed:    false,
		LiveSeenAt: now.Add(-3 * time.Hour),
		Now:        now,
	})
	if d.Tier == TierHot {
		t.Errorf("stale heartbeat promoted to hot; want decay to continue (got %v)", d.Tier)
	}
	if d.MissStreak != 5 {
		t.Errorf("missStreak = %d, want 5", d.MissStreak)
	}
}

func TestTransientErrorDoesNotTouchMissLadder(t *testing.T) {
	// The distinction this protects: an active player behind a flaky fetch must not decay toward
	// dormant. Their miss streak is about THEIR activity; our network failures are our problem and
	// get their own, steeper backoff.
	d := Next(Input{MissStreak: 0, ErrorStreak: 0, Outcome: "transient", Now: now})
	if d.MissStreak != 0 {
		t.Errorf("missStreak = %d, want unchanged at 0", d.MissStreak)
	}
	if d.ErrorStreak != 1 {
		t.Errorf("errorStreak = %d, want 1", d.ErrorStreak)
	}
	if d.Tier != TierHot {
		t.Errorf("tier = %v, want unchanged at hot", d.Tier)
	}
}

func TestErrorBackoffCaps(t *testing.T) {
	// If the hiscores are down or rate limiting us, polling harder makes it worse. Grows fast,
	// caps at an hour so recovery is still prompt once they come back.
	prev := time.Duration(0)
	for streak := 1; streak <= 8; streak++ {
		got := errorBackoff(streak)
		if got < prev {
			t.Errorf("streak %d: backoff %v shrank from %v", streak, got, prev)
		}
		if got > time.Hour {
			t.Errorf("streak %d: backoff %v exceeds the 1h cap", streak, got)
		}
		prev = got
	}
}

func TestUnrankedParksThePlayer(t *testing.T) {
	// A 404 is terminal until a re-probe lifts them. The long next-poll is a backstop: if the
	// status flip fails, we still are not spending a request every tick to be told the same thing.
	d := Next(Input{MissStreak: 0, Outcome: "unranked", Now: now})
	if d.Tier != TierDormant {
		t.Errorf("tier = %v, want dormant", d.Tier)
	}
	if got := d.NextPollAt.Sub(now); got != 24*time.Hour {
		t.Errorf("next poll in %v, want 24h", got)
	}
}

// A plausible steady state at 20k clans: ~1.2M enrolled accounts, half without the plugin, and the
// non-plugin half dominated by accounts that no longer play.
var targetPopulation = map[Tier]int{
	TierHot:     30_000, // actually online right now
	TierWarm:    40_000,
	TierCool:    130_000,
	TierIdle:    200_000,
	TierDormant: 400_000, // enrolled but quit — the case that breaks a flat floor
}

// ForgeBudgetRPS mirrors the FORGE_HISCORES_RPS default. A large community tracker runs the real thing at 4.
const ForgeBudgetRPS = 5.0

// This is the honest arithmetic, and it is uncomfortable on purpose.
//
// The earlier version of this test asserted the ladder "fits" a 150 req/s budget and passed at
// 87 req/s. That budget was invented. A large community tracker covers a big fraction of the entire OSRS
// playerbase at FOUR requests per second with no periodic sweep at all — so the real conclusion is
// that continuous polling cannot cover this population, and the scheduler's job is allocation
// under scarcity rather than hitting cadences. See docs/RATE_BUDGET.md.
func TestDemandVastlyExceedsBudget(t *testing.T) {
	var demandRPS float64
	for tier, count := range targetPopulation {
		rps := float64(count) / tier.Interval().Seconds()
		demandRPS += rps
		t.Logf("%-8s %7d players @ %-5v = %6.1f req/s desired", tier, count, tier.Interval(), rps)
	}

	enrolled := 0
	for _, n := range targetPopulation {
		enrolled += n
	}

	pollsPerDay := ForgeBudgetRPS * 86_400
	perAccountDays := float64(enrolled) / pollsPerDay

	t.Logf("desired: %.1f req/s", demandRPS)
	t.Logf("budget:  %.1f req/s (%.0f polls/day across %d enrolled accounts)", ForgeBudgetRPS, pollsPerDay, enrolled)
	t.Logf("=> oversubscribed %.0fx; one poll per account every %.1f days if spread evenly",
		demandRPS/ForgeBudgetRPS, perAccountDays)

	// The point of the test: demand MUST exceed budget at this scale. If it ever does not, either
	// the population estimate has been quietly shrunk or the intervals have been widened into
	// uselessness — both worth noticing.
	if demandRPS <= ForgeBudgetRPS {
		t.Errorf("desired %.1f req/s no longer exceeds the %.1f req/s budget — the population "+
			"model or the intervals have drifted", demandRPS, ForgeBudgetRPS)
	}

	// And the consequence that must stay true: continuous polling alone cannot serve a competition
	// cadence, so plugin push and boundary sweeps are load-bearing, not optional.
	if perAccountDays < 1 {
		t.Errorf("budget now affords a poll per account every %.2f days — if that is real, the "+
			"boundary/on-demand design could be simplified", perAccountDays)
	}
}

// Given that the budget is fixed and scarce, what the ladder must still get right is the ORDER:
// the scarce requests have to go to the players most likely to have moved.
func TestLadderPrioritisesTheLikelyMovers(t *testing.T) {
	var demandRPS float64
	share := map[Tier]float64{}
	for tier, count := range targetPopulation {
		rps := float64(count) / tier.Interval().Seconds()
		share[tier] = rps
		demandRPS += rps
	}

	// Dormant accounts are 33% of the population but must be a small minority of the requests.
	// That is the entire answer to bulk-enrolled dead rosters, and it is what makes enrolment
	// quotas and caps unnecessary.
	dormantShare := share[TierDormant] / demandRPS
	t.Logf("dormant: %.0f%% of population, %.0f%% of requests",
		100*float64(targetPopulation[TierDormant])/1_200_000, 100*dormantShare)
	if dormantShare > 0.10 {
		t.Errorf("dormant players claim %.0f%% of requests, want under 10%%", dormantShare*100)
	}

	// Hot players are 2.5% of the population and should claim the largest single share, because
	// they are the ones whose numbers are actually changing.
	hotShare := share[TierHot] / demandRPS
	if hotShare < 0.25 {
		t.Errorf("hot players claim only %.0f%% of requests, want at least 25%%", hotShare*100)
	}

	// And the comparison that justifies the ladder existing at all: a flat floor for everyone
	// enrolled spends most of the budget on accounts that have not logged in for years.
	var flat float64
	for _, count := range targetPopulation {
		flat += float64(count) / (30 * time.Minute).Seconds()
	}
	t.Logf("flat 30m floor for all enrolled: %.1f req/s (%.1fx the ladder's demand)", flat, flat/demandRPS)
	if flat < demandRPS {
		t.Error("flat floor should be strictly more expensive than the ladder")
	}
}

func TestPromoteRespectsPerPlayerFloor(t *testing.T) {
	// Without the floor, promotion is an amplifier: a refresh press, a heartbeat and a submission
	// arriving together each pull the same player forward, and one enthusiastic user can spend a
	// visible share of a 5 req/s budget on themselves.
	justPolled := now.Add(-10 * time.Second)
	d := PromoteNow(now, justPolled)
	if !d.NextPollAt.After(now) {
		t.Errorf("next poll at %v, want deferred past %v by the floor", d.NextPollAt, now)
	}
	if got := d.NextPollAt.Sub(justPolled); got != MinPollInterval {
		t.Errorf("gap from last poll = %v, want %v", got, MinPollInterval)
	}

	// A player polled long ago goes immediately, which is the whole point of a refresh button.
	d = PromoteNow(now, now.Add(-2*time.Hour))
	if !d.NextPollAt.Equal(now) {
		t.Errorf("next poll at %v, want immediate (%v)", d.NextPollAt, now)
	}

	// Never polled: also immediate — they need a baseline before anything can be scored.
	d = PromoteNow(now, time.Time{})
	if !d.NextPollAt.Equal(now) {
		t.Errorf("never-polled player deferred to %v, want immediate", d.NextPollAt)
	}
}
