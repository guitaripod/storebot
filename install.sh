#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"

mkdir -p ~/.config/storebot ~/.local/state/storebot
if [ ! -f ~/.config/storebot/secrets.env ]; then
  cat > ~/.config/storebot/secrets.env <<'EOF'
STOREBOT_TOKEN=
STOREBOT_CHAT_IDS=
STOREBOT_DIGEST_HOUR=9
STOREBOT_VENDOR=93803823
EOF
  chmod 600 ~/.config/storebot/secrets.env
  echo "created ~/.config/storebot/secrets.env — fill STOREBOT_TOKEN (@BotFather) and STOREBOT_CHAT_IDS"
fi

mkdir -p ~/.config/systemd/user
ln -sf "$here/storebot.service" ~/.config/systemd/user/storebot.service
systemctl --user daemon-reload
echo "installed. next: fill STOREBOT_TOKEN, then: systemctl --user enable --now storebot"
