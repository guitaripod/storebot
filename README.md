# storebot

Telegram bot that tells Marcus the moment a sale lands, and writes the local
activity feed for rc-tui.

**Live sales** — every App Store record sends its Server Notifications to the
Midgar ledger, which queues each sale; storebot drains the queue every 300 s
and pings per-sale detail: product, price, buyer's country, plus tags:

- `🆕` new subscriber / one-time purchase, `🔁` renewal, `🌱` trial → paid
- `🐣 first-ever` the app's first-ever sale, `🌅 1st today` first of the day

```
🆕 🐣 first-ever 🌅 1st today 🇫🇮 App of the Dead · Greek Guides: +€2.99
🌱 🔁 🇺🇸 Helia · Pro (Monthly): +$9.99
```

A sale is pinged once per App Store transaction id. The first drain
acknowledges the queued backlog without pinging. Which apps have ever sold is
seeded once from Apple's SALES reports (yearly back to 2019, then monthly and
daily) and keyed by short app name, so "Flaccy – Lossless FLAC Player" and
"Flaccy: FLAC Music Player" are one app. Refunds are tracked silently.

**Next-day sales**: the App Store SALES report is checked once a day (default
09:00, retried until Apple publishes it) and pings what the ledger could not:
paid downloads, and in-app purchases of any app whose notifications do not
reach the ledger.

**App reviews**: App Store Connect customerReviews polled every 30 min; every
new written review pings (any star level). Stars-only ratings are invisible
there, so the same tick also diffs per-storefront rating counts (public
iTunes lookup — the ASC API exposes none) and pings the remainder, with the
star count inferred from the storefront average when exactly one new rating
landed. First run baselines silently — history never replays. The lookup's
count wobbles (stale caches, Apple re-tallies), so counts are diffed against
a high-water baseline: a lower read is absorbed and logged, never re-pinged.
On upgrade from a pre-watermark state, each watermark seeds provisionally:
the first read above the old count is held one sweep (unchanged read
confirms it) so history never replays. Stars-only ratings now wait two review
cycles plus five minutes; a written review that surfaces in that window
cancels one pending rating instead of arriving after it.

**No double pings**: sales are collapsed by App Store transaction id, an app
the ledger reports live is skipped by the daily sweep, and ambiguous Telegram
send timeouts are never retried. A second storebot process refuses to start.

**Coverage audit**: every 6h storebot lists the App Store apps that can take
money (any IAP or subscription group) but whose notifications do not reach the
ledger, and pings when that list changes. `/coverage` runs it on demand.

**Activity feed**: writes `~/.local/state/storebot/feed.json` (mode 600) and a
60-second `feed.alive` heartbeat for rc-tui. It contains the same accepted sales,
reviews and rating-only events, including entries whose Telegram delivery failed;
`/off` mutes Telegram but does not stop the feed. On first creation it seeds all
available written reviews, one all-time rating total per app, 14 days of paid
App Store sales rows as `backfilled` history. Stars-only ratings are aggregate storefront counts, not
individual Apple events, so they appear as an app total. rc-tui reads this file
locally and makes no additional API calls.

Commands: `/status`, `/coverage`, `/on`, `/off`, `/help`.

## Install

```sh
./install.sh                                  # symlink service + template secrets.env
# fill ~/.config/storebot/secrets.env
systemctl --user enable --now storebot
```

Config is read from `~/.config/storebot/secrets.env` (bot-specific:
`STOREBOT_SALES_SECONDS`, `STOREBOT_REVIEW_SECONDS`, `STOREBOT_LEDGER_URL`,
`STOREBOT_LEDGER_SECRET`, `STOREBOT_LEDGER_APP_ID`, `STOREBOT_DIGEST_HOUR`,
`STOREBOT_VENDOR`) and `~/.config/midgar/credentials.env` (the App Store
Connect key: `ASC_KEY_ID`, `ASC_ISSUER_ID`, `ASC_PRIVATE_KEY_PATH`). Both files are
parsed by the bot itself, so `export KEY=value`, quotes and inline comments
all work.

## Checks

```sh
python3 storebot.py --selftest     # validate the App Store Connect and ledger data layer
python3 storebot.py --send "test"  # send one raw message
```

## License

GPL-3.0
