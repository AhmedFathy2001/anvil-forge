# The polling budget

The single most important number in Forge is how many requests per second it makes to the official
OSRS hiscores. This document records where that number comes from, because it is the constraint the
whole scheduler is shaped around and it is much smaller than it first appears.

## What a large community tracker actually does

A large community hiscores tracker covers a big fraction of the OSRS playerbase, and its production
configuration is publicly documented rather than guessable:

```ts
// handlers/update-player.job.ts
options: {
  backoff: 30_000,
  maxConcurrent: 8,
  rateLimiter: { max: 1, duration: 250 }   // 1 request per 250ms
}
```

That limiter is BullMQ's, backed by Redis, and applies to the whole `UPDATE_PLAYER` queue across
every worker. So:

**A tracker at that scale polls the hiscores at 4 requests per second. Globally. In production.**

Not 4 per clan, not 4 per worker. Four.

Everything else in their design follows from that being the budget:

| Mechanism | What it does |
|---|---|
| No periodic all-player sweep | There is no cron job that walks every tracked player. None. |
| Boundary updates | Competitions update participants at `started`, `ending-12h`, `ending-2h` — not continuously. |
| The gained-only filter | At `ending-2h`, only participants **who have already gained** are refreshed. Accounts that never moved are not re-polled at the busiest moment. |
| Staleness gate | The `ending-12h` sweep only touches players whose data is over 24h old. |
| On-demand | Most updates come from a human pressing "update" on the site. |
| Patron auto-updates | Even paying supporters get at most **one automatic update per day**. |
| Per-player cooldown | 60 seconds minimum between updates of the same player. |
| Terminal cooldown | Opted-out / not-found / flagged players get a 24h Redis cooldown. |
| Even spreading | Group score updates are spread across the day: `delay = i * (24h / groupCount)`. No thundering herd. |

## What that means for us

The arithmetic is unforgiving. At 20k clans we project ~1.2M enrolled accounts. At that 4 req/s:

```
4 req/s x 86,400 s = 345,600 polls/day
345,600 / 1,200,000 accounts = 0.29 polls per account per day
```

**One poll per account every three and a half days.** Even at 20 req/s it is one poll every 17
hours. There is no rate, within the realm of politeness, at which 1.2M accounts can be continuously
polled on a competition-relevant cadence.

An earlier version of this design sized the ladder against an ~87 req/s budget and treated that as
"roughly what services of that class do". That was wrong by more than an order of magnitude, and it
would have shipped a service that got the box's IP blocked — taking tracking down for every clan at
once, not just the one that caused it.

## The correction

The ladder is **not** a way to achieve target cadences. It is a way to **allocate a fixed, scarce
budget by priority**. Three consequences:

1. **Backlog is normal, not a bug.** `next_poll_at` is a priority key, not a promise. When more
   players are due than the budget serves, the most-overdue go first and the rest wait. The
   `backlog` column in `sweep_runs` measures this; it is expected to be non-zero at scale.

2. **The plugin is the primary data path, not a nice-to-have.** Push costs us zero hiscores
   requests and arrives in seconds. Every percentage point of plugin adoption directly buys polling
   budget for the players who do not have it. This reframes plugin adoption from a product
   nice-to-have into the core scaling lever.

3. **Boundaries and demand carry the load, continuous polling fills the gaps.** Following that model:
   - **event start** — mandatory poll of every participant; this is the frozen baseline, and it is
     the one poll that is never negotiable
   - **event end** — mandatory sweep to close the books exactly
   - **during** — only players showing evidence of life, drawn from whatever budget remains
   - **on demand** — a "refresh" button with a 60s per-player cooldown, so anyone who cares that
     their number is stale can fix it themselves for one request

   The gained-only filter at event end is worth copying verbatim: an account that has not moved all
   event will not have moved in the last two hours either, and re-polling it at the exact moment
   every competition is closing is the worst possible time to spend the budget.

## The number we ship

`FORGE_HISCORES_RPS` defaults to **5**, marginally above that 4 because we additionally need to
detect tile completions mid-event for players without the plugin.

Raising it is a decision about someone else's infrastructure that we do not pay for and cannot be
granted more of by asking. Before raising it, exhaust in this order:

1. Increase plugin adoption (free budget).
2. Widen the ladder (costs latency for idle players, which costs nothing real).
3. Move more work to boundaries and on-demand.
4. Only then, raise the rate — and say so in the changelog.
