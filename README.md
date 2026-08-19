# storebot

Telegram bot reporting Midgar's **App Store sales** and **RevenueCat** revenue.
Long-polls a dedicated bot (@midgar_storebot), answers on-demand commands and
sends a scheduled daily digest (default 09:00 local).

## Commands

| Command                  | What it does                                              |
| ------------------------ | --------------------------------------------------------- |
| `/sales [n\|week\|month\|date]` | App Store downloads + proceeds (default: yesterday) |
| `/countries [date]`      | top download countries for a day                          |
| `/revenue [range] [type]`| RevenueCat revenue (proceeds / gross / net)               |
| `/mrr`                   | active subscriptions, trials, MRR, 28d revenue            |
| `/apps`                  | one-line subscription snapshot per app                    |
| `/digest`                | send the daily digest now                                 |
| `/status`                | health, uptime, next digest                               |
| `/on` / `/off`           | toggle the daily digest                                   |

## Data sources

- **App Store Connect** `salesReports` (SALES/SUMMARY per day, gzip TSV) with a
  locally signed ES256 JWT — needs `ASC_KEY_ID` / `ASC_ISSUER_ID` /
  `ASC_PRIVATE_KEY_PATH` + vendor number. Days with no sales come back 404 and
  count as zero.
- **RevenueCat v2** — every `RC_SECRET_*` key in the Midgar credentials vault
  is auto-discovered via `GET /v2/projects`, then queried through
  `GET /v2/projects/{id}/metrics/overview` and `/metrics/revenue`. Adding a new
  app's key to `credentials.env` is picked up automatically.
- Day-over-day deltas in the digest come from snapshots persisted in
  `~/.local/state/storebot/state.json` (kept for 14 days).

## Install

```sh
./install.sh                                  # symlink service + template secrets.env
# fill ~/.config/storebot/secrets.env (token from @BotFather, chat id, vendor)
systemctl --user enable --now storebot
```

Config is read from `~/.config/storebot/secrets.env` (bot-specific) and
`~/.config/midgar/credentials.env` (ASC + RevenueCat secrets). Both are parsed
by the bot itself, so `export KEY=value`, quotes and inline comments all work.

## Checks

```sh
python3 storebot.py --selftest     # validate ASC + RevenueCat data layer
python3 storebot.py --digest-now   # send the digest once
python3 storebot.py --send "test"  # send one raw message
```

## License

GPL-3.0
