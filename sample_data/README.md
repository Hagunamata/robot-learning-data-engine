# sample_data/

A **handful** of committed LeRobot-format episodes, kept as a schema reference and so
`make seed` / the smoke tests have something to run against without any download.

**Empty at M0 by design.** Real sample episodes are added in **M3** once the
canonicalizer exists and the LeRobot feature keys are verified against the real spec.
Only a few MB total — anything larger stays gitignored under `data/` (committing more
than a few MB is a DECISION to clear with the human first).

**License:** each committed sample carries the license of its upstream source (DROID
= CC-BY 4.0). Record it here alongside the episode when added.
