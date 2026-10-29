# storebot

Telegram bot that tells Marcus the moment a sale lands, and writes the local
activity feed for rc-tui.

**RevenueCat sales** — per-sale detail via the RC webhook relay
(`storebot-hook`, a Cloudflare Worker): product, price, buyer's country, plus
tags:

- `🆕` new subscriber / one-time purchase, `🔁` renewal, `🌱` trial → paid
- `🐣 first-ever` the project's first-ever sale, `🌅 1st today` first of the day

```
🆕 🐣 first-ever 🌅 1st today 🇫🇮 App of the Dead · Greek Guides: +€2.99
🌱 🔁 🇺🇸 Helia: Solar Coach · Pro (Monthly): +$9.99
```

A 60s revenue-metric poll stays as the fallback for projects that have never
delivered a webhook. Once a project is proven live, app-level deltas are
absorbed silently; after a fallback ping, matching hook pings are suppressed
for 30 min so the first sale cannot ping twice. Refunds are tracked silently.

**Paid-app sales** (invisible to RevenueCat, e.g. SForesight): App Store sales
report checked once a day (default 09:00); pings only when a paid download
actually landed.

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

**App Store sales without RevenueCat** (Master of Flags, Master of Inventory,
PixiePocket, Tailscode): Apple's own App Store Server Notifications V2, posted
to the same relay at `/asn?k=<STOREBOT_HOOK_SECRET>` and drained on the same
300s tick (configurable), so they ping live like everything else. Set each app's **Production Server URL** to that address (App Store Connect → App Information → App Store
Server Notifications, version 2) — there is no API for it. Product names come
from the app's ASC in-app purchases, and once an app has delivered one
notification the daily sweep stops reporting its sales a day later.

**No double pings**: RevenueCat and Apple notifications are collapsed by App
Store transaction id, the revenue fallback defers to proven webhooks, and
ambiguous Telegram send timeouts are never retried. A second storebot process
refuses to start.

**Coverage audit**: every 6h storebot compares its RevenueCat projects
against the App Store apps that actually sell something (any IAP or
subscription group) and pings the gaps — apps with no RC project, and RC
projects that earn but whose webhook has never fired. It re-pings only when
the gap set changes. `/coverage` runs it on demand.

**Activity feed**: writes `~/.local/state/storebot/feed.json` (mode 600) and a
60-second `feed.alive` heartbeat for rc-tui. It contains the same accepted sales,
reviews and rating-only events, including entries whose Telegram delivery failed;
`/off` mutes Telegram but does not stop the feed. On first creation it seeds all
available written reviews, one all-time rating total per app, 14 days of paid
App Store sales rows, and the current two days of hook-log purchases as
`backfilled` history. Stars-only ratings are aggregate storefront counts, not
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
`STOREBOT_POLL_SECONDS`, `STOREBOT_HOOK_SECONDS`, `STOREBOT_REVIEW_SECONDS`,
`STOREBOT_HOOK_URL`, `STOREBOT_HOOK_SECRET`, `STOREBOT_DIGEST_HOUR`,
`STOREBOT_VENDOR`) and `~/.config/midgar/credentials.env` (ASC + RevenueCat
secrets; every `RC_SECRET_*` project is auto-discovered). Both files are
parsed by the bot itself, so `export KEY=value`, quotes and inline comments
all work.

## Checks

```sh
python3 storebot.py --selftest     # validate ASC + RC + relay data layer
python3 storebot.py --replay       # one-off: send all real history events as pings
python3 storebot.py --send "test"  # send one raw message
```

## License

GPL-3.0
