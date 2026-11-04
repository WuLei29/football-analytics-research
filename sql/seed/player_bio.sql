-- player_bio.sql: date of birth and height for the players with a published page.
--
-- Neither field is in the provider's squad feed. `date_of_birth` and `height_cm`
-- are hand-filled in silver.players and the squad upsert never overwrites them
-- (src/silver/squads/db.py). Read from each player's Transfermarkt profile on
-- 3 Nov 2026. NULL height: the profile lists none.
--
-- Idempotent: a second run updates 0 rows. A new player with a page gets a new
-- row here.

UPDATE silver.players AS p
SET date_of_birth = v.date_of_birth,
    height_cm     = v.height_cm
FROM (VALUES
    (2073, DATE '2001-03-20', 189),
    (2130, DATE '2001-09-09', 164),
    (2339, DATE '1997-01-30', 186),
    (2345, DATE '2002-09-05', 180),
    (2346, DATE '1992-09-23', 180),
    (2347, DATE '2001-10-05', 183),
    (2348, DATE '2000-10-18', 181),
    (2349, DATE '1995-09-14', 184),
    (2350, DATE '2000-09-30', 178),
    (2352, DATE '2004-09-15', 175),
    (2353, DATE '1989-11-25', 186),
    (2354, DATE '2001-02-04', 185),
    (2355, DATE '2004-01-30', 175),
    (2356, DATE '2003-07-19', 186),
    (2357, DATE '2001-06-17', 175),
    (2358, DATE '2001-10-29', 174),
    (2359, DATE '2002-07-03', 186),
    (2360, DATE '1991-06-17', 190),
    (2361, DATE '1998-03-11', 191),
    (2362, DATE '1999-10-06', 176),
    (2363, DATE '2003-09-12', 183),
    (2366, DATE '1996-08-01', 178),
    (2367, DATE '2006-03-24', 175),
    (2369, DATE '1997-05-15', 187),
    (2370, DATE '2000-05-26', 180),
    (2372, DATE '2005-01-30', 183),
    (2373, DATE '1992-01-24', 189),
    (2374, DATE '2001-12-28', 170),
    (2375, DATE '2005-02-21', 180),
    (2376, DATE '2003-05-17', 176),
    (2377, DATE '1998-05-25', 177),
    (3273, DATE '2006-09-19', 164),
    (4777, DATE '2001-11-14', 183),
    (4785, DATE '2000-06-22', 175),
    (4790, DATE '1999-10-30', 186),
    (4794, DATE '2005-09-28', 185),
    (5387, DATE '2007-04-11', 175),
    (5410, DATE '2007-05-26', NULL::integer)
) AS v (player_id, date_of_birth, height_cm)
WHERE p.player_id = v.player_id
  AND (p.date_of_birth IS DISTINCT FROM v.date_of_birth
       OR p.height_cm IS DISTINCT FROM v.height_cm);
