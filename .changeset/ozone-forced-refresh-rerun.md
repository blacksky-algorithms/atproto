---
'@atproto/ozone': patch
---

A forced run of a periodic background task now waits for the run already in flight and runs once more, so a refresh requested after new events reflects them.
