#!/usr/bin/env python3
"""storebot — Telegram bot reporting Midgar's App Store sales and RevenueCat revenue.

Long-polls a dedicated Telegram bot. Answers on-demand commands and sends a
scheduled daily digest (default 09:00 local):

  /sales [n|week|month|date]   App Store downloads + proceeds (default: yesterday)
  /countries [date]            top download countries for a single day
  /revenue [range] [type]      RevenueCat revenue for a range (proceeds|gross|net)
  /mrr                         active subscriptions, trials, MRR, 28d revenue
  /apps                        one-line snapshot per tracked app
  /digest                      send the daily digest now
  /status                      health, uptime, next digest
  /on  /off                    toggle the daily digest

Data sources: App Store Connect salesReports (SALES/SUMMARY per day, gzip TSV,
ES256 JWT signed locally) and the RevenueCat v2 API. Every RC_SECRET_* key in
the Midgar credentials vault is discovered via /v2/projects (name + id), then
queried via /metrics/overview and /metrics/revenue — new RC projects are picked
up automatically when a key is added to credentials.env.

Config (both files parsed by the bot itself; sourceable `export KEY=value` and
plain KEY=value lines accepted):
  ~/.config/storebot/secrets.env   STOREBOT_TOKEN, STOREBOT_CHAT_IDS,
                                   STOREBOT_DIGEST_HOUR (default 9),
                                   STOREBOT_VENDOR (ASC vendor number),
                                   STOREBOT_API_BASE (default https://api.telegram.org)
  ~/.config/midgar/credentials.env ASC_KEY_ID, ASC_ISSUER_ID,
                                   ASC_PRIVATE_KEY_PATH, RC_SECRET_* keys

CLI:
  --selftest     validate the data layer (ASC + RevenueCat) and exit
  --digest-now   send the daily digest and exit
  --send TEXT    send one raw message and exit
"""

import base64
import csv
import gzip
import io
import json
import os
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

RC_BASE = "https://api.revenuecat.com/v2"
ASC_SALES_URL = "https://api.appstoreconnect.apple.com/v1/salesReports"
HOME = Path.home()
CONFIG_FILE = HOME / ".config/storebot/secrets.env"
MIDGAR_ENV = HOME / ".config/midgar/credentials.env"
STATE_FILE = HOME / ".local/state/storebot/state.json"
LOG_DIR = HOME / ".local/state/storebot"

TG_MAX_LEN = 4000


def log(msg, level="INFO"):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}"
    print(line, file=sys.stderr if level in ("ERROR", "WARN") else sys.stdout, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / f"run-{datetime.now().strftime('%Y-%m-%d')}.log").open("a") as f:
        f.write(line + "\n")


def parse_env_file(path):
    """Parse a shell-style env file; tolerates `export`, quotes, inline
    comments and $VAR/$HOME references."""
    if not path.exists():
        return {}
    out = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if value[:1] in ('"', "'"):
            quote = value[0]
            end = value.find(quote, 1)
            value = value[1:end if end != -1 else None]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        out[key] = os.path.expanduser(os.path.expandvars(value))
    return out


def load_config():
    cfg = {}
    cfg.update(parse_env_file(MIDGAR_ENV))
    cfg.update(parse_env_file(CONFIG_FILE))
    cfg.update({k: v for k, v in os.environ.items() if k.startswith("STOREBOT_") or k.startswith("ASC_") or k.startswith("RC_")})
    cfg["STOREBOT_CHAT_IDS"] = [
        c.strip() for c in cfg.get("STOREBOT_CHAT_IDS", cfg.get("STOREBOT_CHAT_ID", "")).split(",")
        if c.strip()
    ]
    try:
        cfg["STOREBOT_DIGEST_HOUR"] = int(cfg.get("STOREBOT_DIGEST_HOUR", "9"))
    except ValueError:
        cfg["STOREBOT_DIGEST_HOUR"] = 9
    return cfg


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError) as e:
            log(f"state.json unreadable, starting fresh: {e}", "WARN")
    return {}


_STATE_LOCK = threading.Lock()


def save_state(state):
    with _STATE_LOCK:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=1))
        tmp.replace(STATE_FILE)


def _b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class StoreBot:
    def __init__(self):
        self.cfg = load_config()
        self.token = self.cfg.get("STOREBOT_TOKEN", "")
        self.chat_ids = self.cfg["STOREBOT_CHAT_IDS"]
        self.api_base = self.cfg.get("STOREBOT_API_BASE", "https://api.telegram.org").rstrip("/")
        self.digest_hour = self.cfg["STOREBOT_DIGEST_HOUR"]
        self.vendor = self.cfg.get("STOREBOT_VENDOR", "")
        self.state = load_state()
        self.started = time.time()
        self._jwt = ""
        self._jwt_exp = 0.0
        self._rc_projects = None
        self.last_status = {"asc": "", "rc": ""}

    def run(self):
        if "--selftest" in sys.argv:
            sys.exit(self.selftest())
        if not self.token:
            print("error: STOREBOT_TOKEN not set (see ~/.config/storebot/secrets.env)", file=sys.stderr)
            return 2
        if "--send" in sys.argv:
            text = sys.argv[sys.argv.index("--send") + 1]
            self.send(text)
            print("sent")
            return 0
        if "--digest-now" in sys.argv:
            self._send_digest()
            return 0
        self.run_forever()
        return 0

    # ---------------------------------------------------------------- telegram

    def tg_api(self, method, payload=None, timeout=60):
        url = f"{self.api_base}/bot{self.token}/{method}"
        body = json.dumps(payload or {}).encode()
        last_err = None
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read())
            except urllib.error.HTTPError as e:
                try:
                    data = json.loads(e.read())
                except (ValueError, OSError):
                    data = None
                if data is None or e.code >= 500:
                    last_err = e
                    time.sleep(min(2 ** attempt, 30))
                    continue
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
                last_err = e
                time.sleep(min(2 ** attempt, 30))
                continue
            if data.get("ok"):
                return data.get("result")
            retry_after = (data.get("parameters") or {}).get("retry_after")
            if retry_after:
                time.sleep(retry_after + 1)
                continue
            raise RuntimeError(f"telegram {method}: {data.get('description', 'unknown')}")
        raise RuntimeError(f"telegram {method} failed after retries: {last_err}")

    def send(self, text, silent=False, chat_id=None):
        chat_id = chat_id or self.chat_ids[0] if self.chat_ids else None
        if not chat_id:
            raise RuntimeError("no chat id configured")
        for chunk in self._chunks(text):
            self.tg_api("sendMessage", {"chat_id": chat_id, "text": chunk,
                                        "disable_notification": silent})

    @staticmethod
    def _chunks(text):
        if len(text) <= TG_MAX_LEN:
            return [text]
        parts, buf = [], []
        size = 0
        for line in text.splitlines(keepends=True):
            if size + len(line) > TG_MAX_LEN and buf:
                parts.append("".join(buf))
                buf, size = [], 0
            buf.append(line)
            size += len(line)
        if buf:
            parts.append("".join(buf))
        return parts

    # -------------------------------------------------------------------- asc

    def asc_jwt(self):
        if self._jwt and time.time() < self._jwt_exp - 300:
            return self._jwt
        key_id = self.cfg["ASC_KEY_ID"]
        issuer = self.cfg["ASC_ISSUER_ID"]
        pem = Path(self.cfg["ASC_PRIVATE_KEY_PATH"]).read_text()
        key = serialization.load_pem_private_key(pem.encode(), password=None)
        header = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
        payload = {"iss": issuer, "iat": int(time.time()),
                   "exp": int(time.time()) + 1200, "aud": "appstoreconnect-v1"}
        signing_input = (_b64(json.dumps(header, separators=(",", ":")).encode()) + "."
                         + _b64(json.dumps(payload, separators=(",", ":")).encode()))
        der = key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256()))
        r, s = decode_dss_signature(der)
        self._jwt = signing_input + "." + _b64(r.to_bytes(32, "big") + s.to_bytes(32, "big"))
        self._jwt_exp = payload["exp"]
        return self._jwt

    def asc_sales(self, day, frequency="DAILY"):
        """Fetch one day's SALES/SUMMARY report as a list of row dicts, or None
        when Apple reports no sales (404) or the report is not generated yet."""
        params = {
            "filter[reportType]": "SALES",
            "filter[reportSubType]": "SUMMARY",
            "filter[frequency]": frequency,
            "filter[reportDate]": str(day),
            "filter[vendorNumber]": self.vendor,
        }
        r = requests.get(ASC_SALES_URL, params=params, timeout=60,
                         headers={"Authorization": f"Bearer {self.asc_jwt()}",
                                  "Accept": "application/a-gzip"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        rows = []
        for row in csv.DictReader(io.StringIO(gzip.decompress(r.content).decode("utf-8", "replace")),
                                  delimiter="\t"):
            rows.append(row)
        return rows

    @staticmethod
    def aggregate(rows):
        apps = {}
        for row in rows:
            title = row.get("Title", "").strip() or "?"
            units = int(row.get("Units") or 0)
            currency = (row.get("Currency of Proceeds") or "").strip()
            try:
                proceeds = float(row.get("Developer Proceeds") or 0)
            except ValueError:
                proceeds = 0.0
            a = apps.setdefault(title, {"units": 0, "proceeds": {}, "countries": Counter()})
            a["units"] += units
            if currency and proceeds:
                a["proceeds"][currency] = round(a["proceeds"].get(currency, 0) + proceeds, 2)
            country = (row.get("Country Code") or "").strip()
            if country:
                a["countries"][country] += units
        return apps

    def sales_span(self, days):
        """Aggregate downloads into per-app totals plus a per-day units list.

        Days without a report count as zero. `days <= 0` means today only."""
        today = date.today()
        if days <= 0:
            days = 1
            end = today
        else:
            end = today - timedelta(days=1)
        merged = {}
        per_day = []
        for i in range(days):
            day = end - timedelta(days=days - 1 - i)
            rows = self.asc_sales(day)
            units = sum(int(r.get("Units") or 0) for r in rows) if rows else 0
            per_day.append((day, units))
            if rows:
                for title, a in self.aggregate(rows).items():
                    m = merged.setdefault(title, {"units": 0, "proceeds": {}, "countries": Counter()})
                    m["units"] += a["units"]
                    for cur, val in a["proceeds"].items():
                        m["proceeds"][cur] = round(m["proceeds"].get(cur, 0) + val, 2)
                    m["countries"].update(a["countries"])
        return merged, per_day

    # -------------------------------------------------------------------- rc

    def rc_get(self, secret, path, params=None, timeout=45):
        r = requests.get(RC_BASE + path, params=params, timeout=timeout,
                         headers={"Authorization": f"Bearer {secret}"})
        r.raise_for_status()
        return r.json()

    def rc_projects(self):
        """Discover the RC project behind every RC_SECRET_* env key."""
        if self._rc_projects is not None:
            return self._rc_projects
        projects = []
        for key, secret in sorted(self.cfg.items()):
            if not key.startswith("RC_SECRET_") or not secret:
                continue
            try:
                data = self.rc_get(secret, "/projects")
                items = data.get("items") or []
                if not items:
                    continue
                projects.append({"env": key, "secret": secret,
                                 "project_id": items[0]["id"], "name": items[0]["name"]})
            except requests.HTTPError as e:
                log(f"rc discover {key}: http {e.response.status_code}", "WARN")
                projects.append({"env": key, "secret": secret, "error":
                                 f"http {e.response.status_code}"})
            except requests.RequestException as e:
                log(f"rc discover {key}: {e}", "WARN")
                projects.append({"env": key, "secret": secret, "error": "unreachable"})
        self._rc_projects = projects
        return projects

    def rc_metrics(self, project):
        """(project) -> dict of overview metric id -> value, or None on failure."""
        try:
            data = self.rc_get(project["secret"],
                               f"/projects/{project['project_id']}/metrics/overview",
                               params={"currency": "USD"})
            return {m["id"]: m["value"] for m in data.get("metrics", [])}
        except (requests.HTTPError, requests.RequestException, KeyError) as e:
            project["error"] = str(e)[:120]
            log(f"rc overview {project['name']}: {e}", "WARN")
            return None

    def rc_revenue(self, project, start, end, revenue_type="proceeds"):
        try:
            data = self.rc_get(project["secret"],
                               f"/projects/{project['project_id']}/metrics/revenue",
                               params={"start_date": str(start), "end_date": str(end),
                                       "currency": "USD", "revenue_type": revenue_type})
            return float(data.get("value") or 0)
        except (requests.HTTPError, requests.RequestException) as e:
            log(f"rc revenue {project['name']}: {e}", "WARN")
            return None

    # ------------------------------------------------------------------ texts

    @staticmethod
    def _fmt_money(v):
        return f"${v:,.2f}"

    @staticmethod
    def _units(n):
        return f"{n} unit" if n == 1 else f"{n} units"

    def _projects_ok(self):
        return [p for p in self.rc_projects() if "project_id" in p]

    def _sales_text(self, days):
        merged, per_day = self.sales_span(days)
        today = date.today()
        if days <= 0:
            day = today
            if not merged:
                return (f"📥 App Store — {day.strftime('%a %Y-%m-%d')}\n"
                        f"No sales reported for today yet (report is generated next day).")
            lines = self._apps_lines(merged, show_units=True)
            head = f"📥 App Store — {day.strftime('%a %Y-%m-%d')} (partial)"
            total_units = sum(a["units"] for a in merged.values())
            tail = self._proceeds_tail(merged)
            return "\n".join([head, f"Total: {self._units(total_units)}" + tail, "", *lines])
        if days == 1:
            day = today - timedelta(days=1)
            if not merged:
                return (f"📥 App Store — {day.strftime('%a %Y-%m-%d')}\n"
                        f"No sales reported for this date (yet).")
            lines = self._apps_lines(merged, show_units=True)
            head = f"📥 App Store — {day.strftime('%a %Y-%m-%d')}"
            total_units = sum(a["units"] for a in merged.values())
            tail = self._proceeds_tail(merged)
            return "\n".join([head, f"Total: {self._units(total_units)}" + tail, "", *lines])
        if not merged and not sum(u for _, u in per_day):
            return "📥 App Store — no sales in the selected range."
        start = today - timedelta(days=days)
        end = today - timedelta(days=1)
        total_units = sum(a["units"] for a in merged.values())
        trend = "  ".join(f"{d.strftime('%d')}:{u}" for d, u in per_day)
        lines = self._apps_lines(merged, show_units=True)
        tail = self._proceeds_tail(merged)
        return "\n".join([
            f"📥 App Store — {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}",
            f"Total: {self._units(total_units)}" + tail,
            f"Per day: {trend}", "", *lines,
        ])

    @staticmethod
    def _apps_lines(merged, show_units=True, limit=15):
        ranked = sorted(merged.items(), key=lambda kv: kv[1]["units"], reverse=True)[:limit]
        return [f"{i + 1}. {title} — {self._units(a['units'])}" for i, (title, a) in enumerate(ranked)]

    @staticmethod
    def _proceeds_tail(merged):
        totals = Counter()
        for a in merged.values():
            for cur, val in a["proceeds"].items():
                totals[cur] = round(totals[cur] + val, 2)
        if not totals:
            return ""
        parts = " + ".join(f"{val:,.2f} {cur}" for cur, val in sorted(totals.items()))
        return f" · Proceeds: {parts}"

    def _countries_text(self, day):
        rows = self.asc_sales(day)
        if not rows:
            return f"🌍 No sales reported for {day} yet."
        merged = self.aggregate(rows)
        countries = Counter()
        for a in merged.values():
            countries.update(a["countries"])
        top = countries.most_common(12)
        lines = [f"{i + 1}. {code} — {units}" for i, (code, units) in enumerate(top)]
        return "\n".join([f"🌍 Top countries — {day} ({countries.total()} units total)", *lines])

    def _revenue_text(self, days, revenue_type, label):
        start = date.today() - timedelta(days=days)
        end = date.today() - timedelta(days=1)
        lines, total = [], 0.0
        errors = []
        for p in self._projects_ok():
            v = self.rc_revenue(p, start, end, revenue_type)
            if v is None:
                errors.append(p["name"])
                continue
            total += v
            if v:
                lines.append(f"{p['name']} — {self._fmt_money(v)}")
        if not lines:
            lines.append("$0.00 across all projects")
        if errors:
            lines.append(f"⚠️ no data: {', '.join(errors)}")
        return "\n".join([
            f"💰 RevenueCat {label} — {start} → {end}",
            f"Total: {self._fmt_money(total)}",
            "", *lines,
        ])

    def _mrr_text(self):
        lines, errors = [], []
        active = trials = mrr = rev28 = new_customers = 0
        for p in self._projects_ok():
            m = self.rc_metrics(p)
            if m is None:
                errors.append(p["name"])
                continue
            active += m.get("active_subscriptions", 0)
            trials += m.get("active_trials", 0)
            mrr += m.get("mrr", 0)
            rev28 += m.get("revenue", 0)
            new_customers += m.get("new_customers", 0)
            lines.append(f"{p['name']} — {int(m.get('active_subscriptions', 0))} active"
                         f" · {self._fmt_money(m.get('mrr', 0))}")
        head = "\n".join([
            "📈 Subscriptions (now)",
            f"Active: {int(active)} · Trials: {int(trials)} · MRR: {self._fmt_money(mrr)}",
            f"28d revenue: {self._fmt_money(rev28)} · 28d new customers: {int(new_customers)}",
        ])
        if errors:
            head += f"\n⚠️ no data: {', '.join(errors)}"
        return "\n".join([head, "", *lines])

    def _apps_text(self):
        lines, errors = [], []
        for p in self._projects_ok():
            m = self.rc_metrics(p)
            if m is None:
                errors.append(p["name"])
                continue
            lines.append(f"{p['name']} — {int(m.get('active_subscriptions', 0))} active"
                         f" · {self._fmt_money(m.get('mrr', 0))}"
                         f" · {int(m.get('active_trials', 0))} trials")
        if errors:
            lines.append(f"⚠️ no data: {', '.join(errors)}")
        return "\n".join(["🗂 Apps", *lines])

    def _digest_text(self):
        yesterday = date.today() - timedelta(days=1)
        merged, per_day = self.sales_span(1)
        units = sum(a["units"] for a in merged.values())
        top = sorted(merged.items(), key=lambda kv: kv[1]["units"], reverse=True)[:5]
        top_line = " · ".join(f"{t} {a['units']}" for t, a in top) or "—"
        asc_tail = self._proceeds_tail(merged)

        rc_lines, rc_errors = [], []
        rc_total = 0.0
        active = trials = mrr = rev28 = 0
        for p in self._projects_ok():
            m = self.rc_metrics(p)
            if m is None:
                rc_errors.append(p["name"])
                continue
            v = self.rc_revenue(p, yesterday, yesterday, "proceeds")
            active += m.get("active_subscriptions", 0)
            trials += m.get("active_trials", 0)
            mrr += m.get("mrr", 0)
            rev28 += m.get("revenue", 0)
            if v is not None:
                rc_total += v
                if v:
                    rc_lines.append(f"{p['name']} {self._fmt_money(v)}")
        if not rc_lines:
            rc_lines.append("$0.00 across all projects")

        delta_line = self._delta_line(active, mrr)
        parts = [
            f"📊 Midgar daily — {date.today().strftime('%a %Y-%m-%d')}",
            f"📥 Downloads ({yesterday.strftime('%b %d')}): {self._units(units)}{asc_tail}",
            f"   {top_line}",
            f"💰 RevenueCat ({yesterday.strftime('%b %d')}, proceeds): {self._fmt_money(rc_total)}",
            "   " + " · ".join(rc_lines),
            f"📈 Active {int(active)} · Trials {int(trials)} · MRR {self._fmt_money(mrr)} · 28d rev {self._fmt_money(rev28)}",
        ]
        if delta_line:
            parts.append(delta_line)
        if rc_errors:
            parts.append(f"⚠️ no data: {', '.join(rc_errors)}")
        return "\n".join(parts)

    def _delta_line(self, active, mrr):
        snaps = self.state.get("snapshots") or {}
        prev = sorted((d for d in snaps if d != date.today().isoformat()), reverse=True)
        if not prev:
            return ""
        snap = snaps[prev[0]]
        prev_active = sum(s.get("active", 0) for s in snap.values())
        prev_mrr = sum(s.get("mrr", 0) for s in snap.values())
        d_active = int(active) - int(prev_active)
        d_mrr = mrr - prev_mrr
        bits = []
        if d_active:
            bits.append(f"active {'+' if d_active > 0 else ''}{d_active}")
        if abs(d_mrr) >= 0.005:
            bits.append(f"MRR {'+' if d_mrr > 0 else ''}{self._fmt_money(d_mrr)}")
        if not bits:
            return f"Δ vs {prev[0]}: no change"
        return f"Δ vs {prev[0]}: " + " · ".join(bits)

    def _snapshot_now(self):
        snap = {}
        for p in self._projects_ok():
            m = self.rc_metrics(p)
            if m is None:
                continue
            snap[p["name"]] = {"active": m.get("active_subscriptions", 0), "mrr": m.get("mrr", 0)}
        snaps = self.state.get("snapshots") or {}
        snaps[date.today().isoformat()] = snap
        for old in sorted(snaps)[:-14]:
            del snaps[old]
        self.state["snapshots"] = snaps

    def _status_text(self):
        up = time.time() - self.started
        d = datetime.now()
        enabled = self.state.get("digest_enabled", True)
        last = self.state.get("last_digest") or "never"
        today = date.today().isoformat()
        if last == today:
            nxt = f"tomorrow {self.digest_hour:02d}:00"
        elif d.hour < self.digest_hour:
            nxt = f"today {self.digest_hour:02d}:00"
        else:
            nxt = "due now"
        if not enabled:
            nxt = "off"
        lines = [
            f"🟢 storebot up {int(up // 3600)}h{int(up % 3600 // 60):02d}m · digest {'ON' if enabled else 'OFF'}",
            f"Last digest: {last} · Next: {nxt}",
            f"ASC vendor {self.vendor} · RC projects: {len(self._projects_ok())}",
        ]
        if self.last_status["asc"]:
            lines.append(f"ASC: {self.last_status['asc']}")
        if self.last_status["rc"]:
            lines.append(f"RC: {self.last_status['rc']}")
        return "\n".join(lines)

    # ------------------------------------------------------------------ loop

    def run_forever(self):
        try:
            self.tg_api("setMyCommands", {"commands": [{"command": c, "description": d} for c, d in COMMANDS]})
        except RuntimeError as e:
            log(f"setMyCommands: {e}", "WARN")
        if "update_offset" not in self.state:
            self._drain_backlog()
        log("storebot online")
        while True:
            self._tick_digest()
            try:
                updates = self.tg_api("getUpdates", {
                    "offset": self.state.get("update_offset", 0), "timeout": 25,
                    "allowed_updates": ["message"]}, timeout=40)
            except RuntimeError as e:
                log(f"getUpdates: {e}", "WARN")
                time.sleep(5)
                continue
            for u in updates or []:
                self.state["update_offset"] = u["update_id"] + 1
                save_state(self.state)
                msg = u.get("message") or {}
                chat_id = str((msg.get("chat") or {}).get("id", ""))
                text = (msg.get("text") or "").strip()
                if not text or chat_id not in self.chat_ids:
                    continue
                try:
                    self.handle(text)
                except Exception as e:
                    log(f"handler error for {text!r}: {e}", "ERROR")
                    try:
                        self.send(f"⚠️ error handling that: {e}")
                    except RuntimeError:
                        pass

    def _drain_backlog(self):
        try:
            updates = self.tg_api("getUpdates", {"offset": -1, "timeout": 0}, timeout=15) or []
            self.state["update_offset"] = updates[-1]["update_id"] + 1 if updates else 0
        except RuntimeError as e:
            log(f"backlog drain: {e}", "WARN")
            self.state["update_offset"] = 0
        save_state(self.state)

    def _tick_digest(self):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if not self.state.get("digest_enabled", True):
            return
        if self.state.get("last_digest") == today:
            return
        retry_at = self.state.get("digest_retry_at", 0)
        if now.hour < self.digest_hour or time.time() < retry_at:
            return
        try:
            self._send_digest()
            self.state["last_digest"] = today
            self.state.pop("digest_retry_at", None)
            save_state(self.state)
        except Exception as e:
            log(f"digest failed, retrying in 30min: {e}", "ERROR")
            self.state["digest_retry_at"] = time.time() + 1800
            save_state(self.state)

    def _send_digest(self):
        text = self._digest_text()
        self._snapshot_now()
        for chat_id in self.chat_ids:
            self.send(text, chat_id=chat_id)
        self.last_status["rc"] = "digest sent"
        log("digest sent")

    # --------------------------------------------------------------- handlers

    def handle(self, text):
        cmd, _, rest = text.partition(" ")
        cmd = cmd.split("@")[0].lower()
        rest = rest.strip()
        if cmd in ("/help", "/start"):
            self.send("Commands:\n" + "\n".join(f"/{c} — {d}" for c, d in COMMANDS))
        elif cmd == "/sales":
            self.send(self._sales_text(self._parse_days(rest)))
        elif cmd == "/countries":
            day = self._parse_date(rest)
            self.send(self._countries_text(day))
        elif cmd == "/revenue":
            days, api_type, label = self._parse_revenue(rest)
            self.send(self._revenue_text(days, api_type, label))
        elif cmd == "/mrr":
            self.send(self._mrr_text())
        elif cmd == "/apps":
            self.send(self._apps_text())
        elif cmd == "/digest":
            self.send("⏳ building digest…")
            self._send_digest()
        elif cmd == "/status":
            self.send(self._status_text())
        elif cmd == "/on":
            self.state["digest_enabled"] = True
            save_state(self.state)
            self.send(f"Daily digest ON — sends at {self.digest_hour:02d}:00 daily.")
        elif cmd == "/off":
            self.state["digest_enabled"] = False
            save_state(self.state)
            self.send("Daily digest OFF. /on to re-enable.")
        elif cmd.startswith("/"):
            self.send("Unknown command. /help")
        else:
            self.send("Use /help for commands.")

    def _parse_days(self, rest):
        today = date.today()
        if not rest:
            return 1
        arg = rest.lower()
        if arg == "week":
            return 7
        if arg == "month":
            return 30
        if arg == "today":
            return 0
        if arg.isdigit():
            return max(1, min(int(arg), 90))
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", arg)
        if m:
            d = date(*map(int, m.groups()))
            return max(0, (today - d).days)
        return 1

    def _parse_date(self, rest):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", rest)
        if m:
            return date(*map(int, m.groups()))
        return date.today() - timedelta(days=1)

    def _parse_revenue(self, rest):
        aliases = {"proceeds": ("proceeds", "proceeds"),
                   "gross": ("revenue", "gross revenue"),
                   "net": ("revenue_net_of_taxes", "net of taxes")}
        parts = rest.split()
        days = self._parse_days(parts[0]) if parts else 1
        api_type, label = aliases["proceeds"]
        if len(parts) > 1 and parts[-1] in aliases:
            api_type, label = aliases[parts[-1]]
        return days, api_type, label

    # --------------------------------------------------------------- selftest

    def selftest(self):
        ok = True

        def check(label, fn):
            nonlocal ok
            try:
                value = fn()
                print(f"  ok  {label}: {value}")
                return True
            except Exception as e:
                ok = False
                print(f"FAIL  {label}: {e}")
                return False

        print(f"config: token={'set' if self.token else 'MISSING'} "
              f"chats={self.chat_ids} vendor={self.vendor or 'MISSING'}")
        if not self.vendor:
            ok = False

        def _asc():
            yesterday = date.today() - timedelta(days=1)
            rows = self.asc_sales(yesterday)
            units = sum(int(r.get("Units") or 0) for r in rows) if rows else 0
            return f"yesterday {yesterday}: {units} units across {len(rows or [])} rows"
        check("asc jwt + sales report", _asc)

        def _rc():
            projects = self._projects_ok()
            details = []
            for p in projects:
                m = self.rc_metrics(p)
                if m is None:
                    details.append(f"{p['name']}: ERROR")
                    continue
                details.append(f"{p['name']}: {int(m.get('active_subscriptions', 0))} active, "
                               f"MRR {m.get('mrr', 0)}, 28d rev {m.get('revenue', 0)}")
            return f"{len(projects)} projects: " + "; ".join(details)
        check("revenuecat discovery + metrics", _rc)

        def _rev():
            p = self._projects_ok()[0]
            yesterday = date.today() - timedelta(days=1)
            return f"{p['name']} proceeds {yesterday}: {self.rc_revenue(p, yesterday, yesterday)}"
        check("revenuecat revenue range", _rev)

        print("SELFTEST " + ("PASS" if ok else "FAIL"))
        return 0 if ok else 1


COMMANDS = [
    ("sales", "App Store downloads & proceeds"),
    ("countries", "top download countries for a day"),
    ("revenue", "RevenueCat revenue for a range (proceeds default)"),
    ("mrr", "active subs, trials, MRR, 28d revenue"),
    ("apps", "per-app subscription snapshot"),
    ("digest", "send the daily digest now"),
    ("status", "bot health"),
    ("on", "enable daily digest"),
    ("off", "disable daily digest"),
    ("help", "list commands"),
]


def main():
    bot = StoreBot()

    def on_sigterm(signum, frame):
        save_state(bot.state)
        sys.exit(0)

    signal.signal(signal.SIGTERM, on_sigterm)
    return bot.run()


if __name__ == "__main__":
    sys.exit(main())
