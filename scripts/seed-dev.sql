-- Development seed: real OSRS accounts so a local run exercises the actual hiscores, including the
-- failure paths. Some of these names WILL 404 (people rename); that is deliberate — the unranked
-- path is the one most likely to rot unnoticed, so the dev seed should hit it every time.
--
--   psql "$DATABASE_URL" -f scripts/seed-dev.sql

INSERT INTO forge_players (rsn, rsn_normalized) VALUES
  ('Lynx Titan',   'lynx titan'),
  ('Zezima',       'zezima'),
  ('Woox',         'woox'),
  ('B0aty',        'b0aty'),
  ('Odablock',     'odablock'),
  ('Framed',       'framed'),
  ('Torvesta',     'torvesta'),
  ('Faux',         'faux'),
  ('Settled',      'settled'),
  ('Mmorpg',       'mmorpg'),
  -- Almost certainly not a real account: exercises the 404 → unranked path.
  ('Notarealrsn1', 'notarealrsn1')
ON CONFLICT (rsn_normalized) DO NOTHING;

-- Everyone enrolled and due immediately, so a local run has work the moment it starts.
INSERT INTO forge_sweep_state (player_id, enrolled, next_poll_at)
SELECT id, true, now() FROM forge_players
ON CONFLICT (player_id) DO UPDATE SET enrolled = true, next_poll_at = now();

SELECT count(*) AS seeded_players FROM forge_players;
SELECT count(*) AS due_now FROM forge_sweep_state WHERE enrolled AND next_poll_at <= now();
