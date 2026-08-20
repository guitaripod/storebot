# storebot

Telegram bot that tells Marcus the moment a sale lands. Nothing else.

**RevenueCat sales** (IAP + subscriptions): polls the realtime revenue metric
every ~60s per project and pings on any increase — emoji per app/amount, no
punchlines:

```
☀️ Solar Beam: +$6.00
```

**Paid-app sales** (invisible to RevenueCat, e.g. SForesight): checks the App
Store sales report once a day (default 09:00) and pings only when a paid
download actually landed:

```
📦 SForesight: +$9.99 USD
```

Refunds are tracked silently. Daily revenue milestones ride along with the
pings. Commands: `/status`, `/on`, `/off`, `/help`.

## Install

```sh
./install.sh                                  # symlink service + template secrets.env
# fill ~/.config/storebot/secrets.env (token from @BotFather, chat id, vendor)
systemctl --user enable --now storebot
```

Config is read from `~/.config/storebot/secrets.env` (bot-specific;
`STOREBOT_POLL_SECONDS` tunes ping cadence, default 60;
`STOREBOT_DIGEST_HOUR` = App Store check hour, default 9) and
`~/.config/midgar/credentials.env` (ASC + RevenueCat secrets; every
`RC_SECRET_*` project is auto-discovered). Both are parsed by the bot itself,
so `export KEY=value`, quotes and inline comments all work.

## Checks

```sh
python3 storebot.py --selftest     # validate ASC + RevenueCat data layer
python3 storebot.py --replay       # one-off: send all real history events as pings
python3 storebot.py --send "test"  # send one raw message
```

## License

GPL-3.0
