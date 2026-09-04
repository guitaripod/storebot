# storebot

Telegram bot that tells Marcus the moment a sale lands. Nothing else.

**RevenueCat sales** — per-sale detail via the RC webhook relay
(`storebot-hook`, a Cloudflare Worker): product, price, buyer's country, plus
tags:

- `🆕` new subscriber / one-time purchase, `🔁` renewal, `🌱` trial → paid
- `🐣 first-ever` the project's first-ever sale, `🌅 1st today` first of the day

```
🆕 🐣 first-ever 🌅 1st today 🇫🇮 App of the Dead · Greek Guides: +€2.99
🌱 🔁 🇺🇸 Helia: Solar Coach · Pro (Monthly): +$9.99
```

A 60s revenue-metric poll stays as fallback (app-level deltas only, no
detail) and suppresses itself for 30 min around webhook deliveries so nothing
double-pings. Refunds are tracked silently.

**Paid-app sales** (invisible to RevenueCat, e.g. SForesight): App Store sales
report checked once a day (default 09:00); pings only when a paid download
actually landed.

**App reviews**: App Store Connect customerReviews polled every 30 min; new
5★ and 1★ reviews ping. First run baselines silently — history never
replays.

**App Store sales without RevenueCat** (Master of Flags, Master of Inventory,
PixiePocket, Tailscode): Apple's own App Store Server Notifications V2, posted
to the same relay at `/asn?k=<STOREBOT_HOOK_SECRET>` and drained on the same
30s tick, so they ping live like everything else. Set each app's **Production
Server URL** to that address (App Store Connect → App Information → App Store
Server Notifications, version 2) — there is no API for it. Product names come
from the app's ASC in-app purchases, and once an app has delivered one
notification the daily sweep stops reporting its sales a day later.

**Coverage audit**: every 6h storebot compares its RevenueCat projects
against the App Store apps that actually sell something (any IAP or
subscription group) and pings the gaps — apps with no RC project, and RC
projects that earn but whose webhook has never fired. It re-pings only when
the gap set changes. `/coverage` runs it on demand.

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
