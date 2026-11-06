#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"

mkdir -p ~/.config/storebot ~/.local/state/storebot
if [ ! -f ~/.config/storebot/secrets.env ]; then
  cat > ~/.config/storebot/secrets.env <<'EOF'
STOREBOT_TOKEN=
STOREBOT_CHAT_IDS=
STOREBOT_DIGEST_HOUR=9
STOREBOT_VENDOR=
STOREBOT_LEDGER_URL=
STOREBOT_LEDGER_SECRET=
STOREBOT_LEDGER_APP_ID=
EOF
  chmod 600 ~/.config/storebot/secrets.env
  echo "created ~/.config/storebot/secrets.env — fill STOREBOT_TOKEN (@BotFather), STOREBOT_CHAT_IDS and STOREBOT_VENDOR"
fi

mkdir -p ~/.config/systemd/user
ln -sf "$here/storebot.service" ~/.config/systemd/user/storebot.service
systemctl --user daemon-reload
echo "installed. next: fill STOREBOT_TOKEN, then: systemctl --user enable --now storebot"
