#!/usr/bin/env python3
"""storebot — Telegram bot that tells Marcus the moment a sale lands.

Long-polls a dedicated Telegram bot. One job: pings the chat when money lands.

  - RevenueCat sales: two paths. Primary is the RC webhook relay (a Cloudflare
    Worker: storebot-hook), which delivers per-sale detail — product, price,
    buyer's country, new vs renewal vs trial conversion, first-of-day and
    first-ever-sale markers:

      🆕 🇫🇮 Solar Beam · Premium (Annual): +€39.99
      🌱 🔁 Helia: Solar Coach · Pro (Monthly): +$9.99
      🐣 First-ever sale — PayDay · Pro (Annual): +€59.99

    The 60s revenue-metric poll stays as fallback (pings app-level deltas only
    when a project has no webhook or the relay is down) and suppresses itself
    around webhook deliveries so nothing double-pings.

  - Everything else Apple-side (paid-app downloads, invisible to RevenueCat,
    plus one-time IAPs / subscriptions of apps with no RC project): checks
    the App Store SALES report once a day (default 09:00) and pings every
    revenue row. Rows for RC-covered apps are suppressed so nothing
    double-pings. Apple publishes DAILY reports with ~1 day lag and retries
    until the report lands, so these pings arrive next morning.

  - App reviews: polls App Store Connect customerReviews and pings every new
    written review (default every 30 min), then sweeps per-storefront rating
    counts (public iTunes lookup — the ASC API exposes none) so stars-only
    ratings ping too; a written review is subtracted from its storefront's
    count delta, so nothing double-pings.

Refunds are tracked silently. Daily revenue milestones ride along with pings.
Control commands: /status, /on, /off, /help.

Config (both files parsed by the bot itself; sourceable `export KEY=value` and
plain KEY=value lines accepted):
  ~/.config/storebot/secrets.env   STOREBOT_TOKEN, STOREBOT_CHAT_IDS,
                                   STOREBOT_DIGEST_HOUR (default 9, App Store check hour),
                                   STOREBOT_POLL_SECONDS (default 60),
                                   STOREBOT_HOOK_SECONDS (default 30),
                                   STOREBOT_REVIEW_SECONDS (default 1800),
                                   STOREBOT_HOOK_URL (webhook relay worker),
                                   STOREBOT_HOOK_SECRET (relay + RC webhook auth),
                                   STOREBOT_VENDOR (ASC vendor number),
                                   STOREBOT_API_BASE (default https://api.telegram.org)
  ~/.config/midgar/credentials.env ASC_KEY_ID, ASC_ISSUER_ID,
                                   ASC_PRIVATE_KEY_PATH, RC_SECRET_* keys

CLI:
  --selftest     validate the data layer (ASC + RevenueCat) and exit
  --replay       walk every project's purchase + renewal history and send the
                 real revenue events as celebratory lines (one-off, ~10 min)
  --send TEXT    send one raw message and exit
"""

import base64
import csv
import gzip
import io
import json
import os
import random
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
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

APP_EMOJI = {
    "solar beam": ["☀️", "🔆"],
    "living dex": ["🦎", "🦋"],
    "psybeam": ["⚡", "🗣️"],
    "psywave": ["🌊", "🧠"],
    "dream": ["😴", "💭"],
    "dead": ["💀", "🪦"],
    "kick": ["🥋", "🦵"],
    "helia": ["🌞", "🌤️"],
    "pay": ["💵", "🤑"],
}

TIER_EMOJI = {
    (0, 1): ["🪙", "💫"],
    (1, 5): ["☕", "🎯"],
    (5, 15): ["🍕", "🚀"],
    (15, 40): ["🎉", "🥂"],
    (40, float("inf")): ["🐳", "👑"],
}

MILESTONES = [
    (10, "🎊 Today crossed $10"),
    (25, "🥳 $25 today"),
    (50, "🚀 $50 today"),
    (100, "💯 $100 today"),
    (250, "👑 $250 today"),
    (500, "🐐 $500 today"),
]

CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CNY": "¥", "KRW": "₩",
    "SEK": "kr", "NOK": "kr", "DKK": "kr", "PLN": "zł", "CAD": "C$",
    "AUD": "A$", "NZD": "NZ$", "CHF": "Fr.", "CZK": "Kč", "BRL": "R$",
    "INR": "₹", "MXN": "MX$", "TRY": "₺", "HKD": "HK$", "SGD": "S$",
    "RUB": "₽", "ZAR": "R", "RON": "lei", "UAH": "₴", "MYR": "RM", "COP": "CO$",
}

MONEY_EVENT_TYPES = {"INITIAL_PURCHASE", "RENEWAL", "NON_RENEWING_PURCHASE"}

TERRITORY_ALPHA2 = {
    "AFG": "af", "AGO": "ao", "AIA": "ai", "ALB": "al", "ARE": "ae",
    "ARG": "ar", "ARM": "am", "ATG": "ag", "AUS": "au", "AUT": "at",
    "AZE": "az", "BEL": "be", "BEN": "bj", "BFA": "bf", "BGR": "bg",
    "BHR": "bh", "BHS": "bs", "BIH": "ba", "BLR": "by", "BLZ": "bz",
    "BMU": "bm", "BOL": "bo", "BRA": "br", "BRB": "bb", "BRN": "bn",
    "BTN": "bt", "BWA": "bw", "CAN": "ca", "CHE": "ch", "CHL": "cl",
    "CHN": "cn", "CIV": "ci", "CMR": "cm", "COD": "cd", "COG": "cg",
    "COL": "co", "CPV": "cv", "CRI": "cr", "CYM": "ky", "CYP": "cy",
    "CZE": "cz", "DEU": "de", "DMA": "dm", "DNK": "dk", "DOM": "do",
    "DZA": "dz", "ECU": "ec", "EGY": "eg", "ESP": "es", "EST": "ee",
    "FIN": "fi", "FJI": "fj", "FRA": "fr", "FSM": "fm", "GAB": "ga",
    "GBR": "gb", "GEO": "ge", "GHA": "gh", "GMB": "gm", "GNB": "gw",
    "GRC": "gr", "GRD": "gd", "GTM": "gt", "GUY": "gy", "HKG": "hk",
    "HND": "hn", "HRV": "hr", "HUN": "hu", "IDN": "id", "IND": "in",
    "IRL": "ie", "IRQ": "iq", "ISL": "is", "ISR": "il", "ITA": "it",
    "JAM": "jm", "JOR": "jo", "JPN": "jp", "KAZ": "kz", "KEN": "ke",
    "KGZ": "kg", "KHM": "kh", "KNA": "kn", "KOR": "kr", "KWT": "kw",
    "LAO": "la", "LBN": "lb", "LBR": "lr", "LBY": "ly", "LCA": "lc",
    "LKA": "lk", "LTU": "lt", "LUX": "lu", "LVA": "lv", "MAC": "mo",
    "MAR": "ma", "MDA": "md", "MDG": "mg", "MDV": "mv", "MEX": "mx",
    "MKD": "mk", "MLI": "ml", "MLT": "mt", "MMR": "mm", "MNE": "me",
    "MNG": "mn", "MOZ": "mz", "MRT": "mr", "MSR": "ms", "MUS": "mu",
    "MWI": "mw", "MYS": "my", "NAM": "na", "NER": "ne", "NGA": "ng",
    "NIC": "ni", "NLD": "nl", "NOR": "no", "NPL": "np", "NRU": "nr",
    "NZL": "nz", "OMN": "om", "PAK": "pk", "PAN": "pa", "PER": "pe",
    "PHL": "ph", "PLW": "pw", "PNG": "pg", "POL": "pl", "PRT": "pt",
    "PRY": "py", "QAT": "qa", "ROU": "ro", "RUS": "ru", "RWA": "rw",
    "SAU": "sa", "SEN": "sn", "SGP": "sg", "SLB": "sb", "SLE": "sl",
    "SLV": "sv", "SRB": "rs", "STP": "st", "SUR": "sr", "SVK": "sk",
    "SVN": "si", "SWE": "se", "SWZ": "sz", "SYC": "sc", "TCA": "tc",
    "TCD": "td", "THA": "th", "TJK": "tj", "TKM": "tm", "TON": "to",
    "TTO": "tt", "TUN": "tn", "TUR": "tr", "TWN": "tw", "TZA": "tz",
    "UGA": "ug", "UKR": "ua", "URY": "uy", "USA": "us", "UZB": "uz",
    "VCT": "vc", "VEN": "ve", "VGB": "vg", "VNM": "vn", "VUT": "vu",
    "XKS": "xk", "YEM": "ye", "ZAF": "za", "ZMB": "zm", "ZWE": "zw",
}


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
    try:
        cfg["STOREBOT_POLL_SECONDS"] = int(cfg.get("STOREBOT_POLL_SECONDS", "60"))
    except ValueError:
        cfg["STOREBOT_POLL_SECONDS"] = 60
    try:
        cfg["STOREBOT_HOOK_SECONDS"] = int(cfg.get("STOREBOT_HOOK_SECONDS", "30"))
    except ValueError:
        cfg["STOREBOT_HOOK_SECONDS"] = 30
    try:
        cfg["STOREBOT_REVIEW_SECONDS"] = int(cfg.get("STOREBOT_REVIEW_SECONDS", "1800"))
    except ValueError:
        cfg["STOREBOT_REVIEW_SECONDS"] = 1800
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
        self.poll_seconds = self.cfg["STOREBOT_POLL_SECONDS"]
        self.hook_seconds = self.cfg["STOREBOT_HOOK_SECONDS"]
        self.review_seconds = self.cfg["STOREBOT_REVIEW_SECONDS"]
        self.hook_url = self.cfg.get("STOREBOT_HOOK_URL", "").rstrip("/")
        self.hook_secret = self.cfg.get("STOREBOT_HOOK_SECRET", "")
        self.vendor = self.cfg.get("STOREBOT_VENDOR", "")
        self.state = load_state()
        if "digest_enabled" in self.state and "alerts_enabled" not in self.state:
            self.state["alerts_enabled"] = self.state.pop("digest_enabled")
        self.started = time.time()
        self._jwt = ""
        self._jwt_exp = 0.0
        self._rc_projects = None
        self._last_poll = 0.0
        self._last_hook = 0.0
        self._last_review = 0.0
        self._products_cache = {}
        self._apps_cache = None
        self._ever_sold_seeded = False
        self._last_coverage = 0.0

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
        if "--replay" in sys.argv:
            self._replay()
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

    def asc_get(self, path):
        r = requests.get("https://api.appstoreconnect.apple.com" + path, timeout=60,
                         headers={"Authorization": f"Bearer {self.asc_jwt()}"})
        r.raise_for_status()
        return r.json()

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

    def rc_get(self, secret, path, params=None, timeout=45):
        url = path if path.startswith("http") else RC_BASE + path
        r = requests.get(url, params=params, timeout=timeout,
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

    def _rc_get_paced(self, secret, path, params=None):
        """rc_get with a throttle + 429 backoff, for full-history walks that
        would otherwise blow the customer-information rate limit."""
        for attempt in range(4):
            time.sleep(0.13)
            try:
                return self.rc_get(secret, path, params)
            except requests.HTTPError as e:
                if e.response.status_code != 429 or attempt == 3:
                    raise
                time.sleep(3 * (attempt + 1))
        return None

    def _history_events(self, project):
        """All real (production, non-refunded, non-zero) revenue events for one
        RC project: one-time + subscription purchases plus every renewal.

        Returns [(timestamp_ms, proceeds_usd, product_display_name), ...]."""
        products = {}
        pnext = f"/projects/{project['project_id']}/products?limit=200"
        while pnext:
            pdata = self._rc_get_paced(project["secret"], pnext)
            for prod in (pdata or {}).get("items") or []:
                name = (prod.get("display_name") or "").strip()
                if name:
                    products[prod.get("id")] = name
                    if prod.get("store_identifier"):
                        products[prod["store_identifier"]] = name
            pnext = (pdata or {}).get("next_page")
        events = []
        initial_txn_ids = set()
        cnext = f"/projects/{project['project_id']}/customers?limit=200"
        while cnext:
            cdata = self._rc_get_paced(project["secret"], cnext)
            for c in (cdata or {}).get("items") or []:
                cid = c["id"]
                sub_next = (f"/projects/{project['project_id']}/customers/{cid}"
                            f"/subscriptions?environment=production&limit=200")
                while sub_next:
                    sdata = self._rc_get_paced(project["secret"], sub_next)
                    for sub in (sdata or {}).get("items") or []:
                        sid = sub["id"]
                        if sub.get("store") not in ("app_store", "play_store", "mac_app_store"):
                            total = (sub.get("total_revenue_in_usd") or {}).get("proceeds") or 0
                            if total > 0.004:
                                events.append((sub.get("starts_at") or 0, total, ""))
                            continue
                        tnext = (f"/projects/{project['project_id']}/subscriptions/{sid}"
                                 f"/transactions?sort=purchased_at&direction=asc&limit=200")
                        while tnext:
                            tdata = self._rc_get_paced(project["secret"], tnext)
                            txns = (tdata or {}).get("items") or []
                            for i, txn in enumerate(txns):
                                if i == 0:
                                    initial_txn_ids.add(txn.get("id"))
                                rev = (txn.get("revenue_in_usd") or {}).get("proceeds") or 0
                                if rev <= 0.004:
                                    continue
                                sku = txn.get("product_store_identifier") or ""
                                events.append((txn.get("purchased_at") or 0, rev,
                                               products.get(sku, "")))
                            tnext = (tdata or {}).get("next_page")
                    sub_next = (sdata or {}).get("next_page")
                buy_next = (f"/projects/{project['project_id']}/customers/{cid}"
                            f"/purchases?environment=production&limit=200")
                while buy_next:
                    bdata = self._rc_get_paced(project["secret"], buy_next)
                    for purch in (bdata or {}).get("items") or []:
                        if purch.get("status") == "refunded":
                            continue
                        if purch.get("store_purchase_identifier") in initial_txn_ids:
                            continue
                        rev = (purch.get("revenue_in_usd") or {}).get("proceeds") or 0
                        if rev <= 0.004:
                            continue
                        events.append((purch.get("purchased_at") or 0, rev,
                                       products.get(purch.get("product_id"), "")))
                    buy_next = (bdata or {}).get("next_page")
            cnext = (cdata or {}).get("next_page")
        events.sort()
        return events

    def _replay(self):
        for p in self._projects_ok():
            try:
                events = self._history_events(p)
            except (requests.HTTPError, requests.RequestException) as e:
                log(f"history {p['name']}: {e}", "WARN")
                continue
            if not events:
                log(f"history {p['name']}: no events")
                continue
            lines = []
            for ts, rev, product in events:
                label = p["name"]
                if product and p["name"].lower() in product.lower():
                    label = product[:40]
                elif product:
                    label = f"{p['name']} · {product[:24]}"
                lines.append(self._celebrate(label, rev))
            for chunk in self._chunks("\n".join(lines)):
                for chat_id in self.chat_ids:
                    self.send(chunk, chat_id=chat_id)
            log(f"history {p['name']}: sent {len(events)} events")

    # ------------------------------------------------------------------ texts

    @staticmethod
    def _fmt_money(v):
        return f"${v:,.2f}"


    def _celebrate(self, name, delta):
        """One-line celebration for a revenue bump: per-app emoji, else a
        size-tiered emoji."""
        name_l = name.lower()
        for key, emojis in APP_EMOJI.items():
            if key in name_l:
                return f"{random.choice(emojis)} {name}: +{self._fmt_money(delta)}"
        for (lo, hi), emojis in sorted(TIER_EMOJI.items()):
            if lo <= delta < hi:
                return f"{random.choice(emojis)} {name}: +{self._fmt_money(delta)}"
        return f"💰 {name} +{self._fmt_money(delta)}"

    def _projects_ok(self):
        return [p for p in self.rc_projects() if "project_id" in p]

    APP_FIELDS = ("name,bundleId,sku,subscriptionStatusUrl,subscriptionStatusUrlVersion,"
                  "subscriptionStatusUrlForSandbox,subscriptionStatusUrlVersionForSandbox")

    def _ensure_apps(self):
        """Refresh the cached ASC apps list daily (shared by reviews + sales).

        The notification URLs are not in the default field set and have to be
        asked for by name, so a plain list silently reports every app as having
        none."""
        if self._apps_cache is None or time.time() - self._apps_cache["at"] > 86400:
            data = self.asc_get(f"/v1/apps?limit=200&fields[apps]={self.APP_FIELDS}")
            self._apps_cache = {"at": time.time(), "apps": data.get("data") or []}

    @staticmethod
    def _name_key(name):
        """A name reduced to its letters and digits, for containment tests."""
        return re.sub(r"[^a-z0-9]+", "", (name or "").lower())

    @staticmethod
    def _brand(name):
        """The leading word of an app or project name — the part the two sides
        still share when a store title and its RevenueCat project are worded
        differently ("Helia: UV Index" against "Helia: Solar Coach")."""
        m = re.search(r"[a-z0-9]+", (name or "").lower())
        return m.group(0) if m else ""

    def _ambiguous_brands(self):
        """Leading words shared by more App Store apps than RevenueCat projects,
        which are therefore too weak to attribute a project by — "Master of
        Flags" must not claim "Master of Inventory". A brand carried by as many
        projects as apps is a product family (both Flaccy SKUs) and still
        matches."""
        apps = Counter(self._brand(a["attributes"].get("name", ""))
                       for a in (self._apps_cache or {}).get("apps") or [])
        projects = Counter(self._brand(p["name"]) for p in self._projects_ok())
        return {b for b, n in apps.items() if b and n > projects.get(b, 0)}

    def _is_rc_covered(self, app_title):
        """True when an ASC app title belongs to a RevenueCat-watched project
        (its IAP/sub rows are already pinged live by the webhook path)."""
        t = self._name_key(app_title)
        brand = self._brand(app_title)
        weak = self._ambiguous_brands()
        for p in self._projects_ok():
            pn = self._name_key(p["name"])
            if (pn and pn in t) or (t and t in pn):
                return True
            if len(brand) >= 4 and brand not in weak and brand == self._brand(p["name"]):
                return True
        return False

    def _notifies_storebot(self, app):
        """True when an app's App Store Server Notification URL points at the
        relay, which is what makes its purchases arrive live."""
        target = f"{self.hook_url}/asn"
        attr = app.get("attributes") or {}
        url = attr.get("subscriptionStatusUrl") or ""
        return bool(self.hook_url) and url.startswith(target)

    def _is_live_covered(self, app_title):
        """True when an app's IAP rows already ping the moment they happen —
        through RevenueCat, or through Apple's own server notifications."""
        if self._is_rc_covered(app_title) or app_title in self.state.get("asn_apps", {}):
            return True
        for app in (self._apps_cache or {}).get("apps") or []:
            if (app["attributes"].get("name") == app_title
                    and self._notifies_storebot(app)):
                return True
        return False

    def _asc_money_pings(self, rows):
        """Ping lines for every revenue row in a SALES report that storebot's
        RevenueCat path cannot see: all paid-app downloads plus IAP/subscription
        rows of apps without an RC project. IAP rows are attributed to their
        app via Parent Identifier (the app's SKU)."""
        sku_map = {}
        for a in self._apps_cache["apps"]:
            attr = a["attributes"]
            for key in (attr.get("sku"), attr.get("bundleId")):
                if key:
                    sku_map[key] = attr.get("name", "?")
        pings = set()
        for row in rows:
            try:
                proceeds = float(row.get("Developer Proceeds") or 0)
            except ValueError:
                continue
            if proceeds <= 0:
                continue
            title = (row.get("Title") or "").strip()
            ptype = (row.get("Product Type Identifier") or "").strip()
            parent = (row.get("Parent Identifier") or "").strip()
            app_name = sku_map.get(parent) if parent else None
            is_iap = app_name is not None or ptype.startswith("IA")
            app_name = app_name or title
            if is_iap and self._is_live_covered(app_name):
                continue
            label = f"{app_name} · {title}" if is_iap and title != app_name else app_name
            flag = self._flag(row.get("Country Code"))
            cur = (row.get("Currency of Proceeds") or "").strip() or "$"
            pings.add(f"📦 {label}: +{proceeds:,.2f} {cur}" + (f" {flag}" if flag else ""))
        return sorted(pings)

    def _status_text(self):
        up = time.time() - self.started
        enabled = self.state.get("alerts_enabled", True)
        return "\n".join([
            f"🟢 storebot up {int(up // 3600)}h{int(up % 3600 // 60):02d}m · alerts {'ON' if enabled else 'OFF'}",
            f"Webhooks every {self.hook_seconds}s · revenue fallback {self.poll_seconds}s · reviews {self.review_seconds}s · App Store check daily {self.digest_hour:02d}:00",
            f"RC projects: {len(self._projects_ok())}",
        ])

    # ------------------------------------------------------------------ hook

    @staticmethod
    def _flag(country):
        code = (country or "").strip().upper()
        if len(code) == 3 and code.isalpha():
            code = ((TERRITORY_ALPHA2.get(code) or code[:2]).upper())
        if len(code) != 2 or not code.isalpha():
            return ""
        return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code)

    @staticmethod
    def _money(amount, currency):
        code = (currency or "USD").upper()
        symbol = CURRENCY_SYMBOLS.get(code)
        if symbol:
            return f"+{symbol}{amount:,.2f}"
        return f"+{code} {amount:,.2f}"

    def _products(self, project):
        """{product key -> display name} for one project, cached 24h."""
        if "project_id" not in project:
            return self._asc_products(project)
        entry = self._products_cache.get(project["project_id"])
        if entry and time.time() - entry["at"] < 86400:
            return entry["map"]
        products = {}
        pnext = f"/projects/{project['project_id']}/products?limit=200"
        try:
            while pnext:
                data = self.rc_get(project["secret"], pnext)
                for prod in (data or {}).get("items") or []:
                    name = (prod.get("display_name") or "").strip()
                    if not name:
                        continue
                    products[prod.get("id")] = name
                    if prod.get("store_identifier"):
                        products[prod["store_identifier"]] = name
                pnext = (data or {}).get("next_page")
            self._products_cache[project["project_id"]] = {"at": time.time(), "map": products}
        except requests.RequestException as e:
            log(f"products {project['name']}: {e}", "WARN")
        return products

    def _asc_products(self, project):
        """{product id -> display name} for an App Store app storebot watches
        through Apple's notifications rather than RevenueCat, cached 24h."""
        app_id = project.get("app_id")
        if not app_id:
            return {}
        entry = self._products_cache.get(app_id)
        if entry and time.time() - entry["at"] < 86400:
            return entry["map"]
        names = {}
        try:
            for item in (self.asc_get(f"/v1/apps/{app_id}/inAppPurchasesV2?limit=200")
                         .get("data") or []):
                attr = item.get("attributes") or {}
                if attr.get("productId") and attr.get("name"):
                    names[attr["productId"]] = attr["name"]
                    names[attr["productId"].lower()] = attr["name"]
            for group in (self.asc_get(f"/v1/apps/{app_id}/subscriptionGroups?limit=200")
                          .get("data") or []):
                subs = self.asc_get(f"/v1/subscriptionGroups/{group['id']}"
                                    f"/subscriptions?limit=200")
                for sub in subs.get("data") or []:
                    attr = sub.get("attributes") or {}
                    if attr.get("productId") and attr.get("name"):
                        names[attr["productId"]] = attr["name"]
                        names[attr["productId"].lower()] = attr["name"]
        except requests.RequestException as e:
            log(f"asc products {app_id}: {e}", "WARN")
            return names
        self._products_cache[app_id] = {"at": time.time(), "map": names}
        return names

    def _apple_project(self, bundle_id):
        """The App Store app behind a bundle id from Apple's notifications,
        shaped like an RC project so one ping path serves both. Seeing one at
        all proves the app's notification URL is live, which is recorded so the
        daily sweep stops reporting the same sale a day later."""
        try:
            self._ensure_apps()
        except requests.RequestException as e:
            log(f"apps list: {e}", "WARN")
            return None
        for app in self._apps_cache["apps"]:
            attr = app["attributes"]
            if attr.get("bundleId") != bundle_id:
                continue
            name = attr.get("name", bundle_id)
            live = self.state.setdefault("asn_apps", {})
            if name not in live:
                live[name] = time.time()
                save_state(self.state)
            return {"name": name, "app_id": app["id"]}
        log(f"apple event for unknown bundle {bundle_id!r}", "WARN")
        return None

    def _seed_ever_sold(self):
        """Mark projects that already have revenue so their next sale is not
        mistaken for a first-ever one."""
        ever = self.state.setdefault("ever_sold", {})
        for p in self._projects_ok():
            if ever.get(p["name"]):
                continue
            m = self.rc_metrics(p)
            if m and m.get("revenue", 0) > 0:
                ever[p["name"]] = True
        save_state(self.state)

    def _hook_line(self, project, event):
        ev_type = event.get("type") or ""
        if ev_type not in MONEY_EVENT_TYPES:
            return None
        if (event.get("environment") or "").upper() != "PRODUCTION":
            return None
        price = event.get("price_in_purchased_currency") or event.get("price") or 0
        if price <= 0:
            return None
        if ev_type == "RENEWAL" and event.get("is_trial_conversion"):
            tag = "🌱"
        elif ev_type == "RENEWAL":
            tag = "🔁"
        else:
            tag = "🆕"
        ever = self.state.setdefault("ever_sold", {})
        if not ever.get(project["name"]):
            ever[project["name"]] = True
            tag += " 🐣 first-ever"
        days = self.state.setdefault("sale_days", {})
        day = datetime.fromtimestamp((event.get("event_timestamp_ms") or 0) / 1000,
                                     tz=timezone.utc).date().isoformat()
        if days.get(project["name"]) != day:
            days[project["name"]] = day
            tag += " 🌅 1st today"
        products = self._products(project)
        pid = event.get("product_id") or ""
        product = products.get(pid) or products.get(pid.lower(), "")
        label = project["name"]
        if product and project["name"].lower() in product.lower():
            label = product[:40]
        elif product:
            label = f"{project['name']} · {product[:28]}"
        flag = self._flag(event.get("country_code"))
        return f"{tag} {flag} {label}: {self._money(price, event.get('currency'))}".strip()

    def _tick_hook(self):
        """Drain the webhook relay queue and turn each sale into a ping."""
        now = time.time()
        if now - self._last_hook < self.hook_seconds or not self.hook_url:
            return
        self._last_hook = now
        if not self.state.get("alerts_enabled", True):
            return
        if not self._ever_sold_seeded:
            self._seed_ever_sold()
            self._ever_sold_seeded = True
        try:
            r = requests.get(f"{self.hook_url}/events", timeout=30,
                             headers={"X-Hook-Secret": self.hook_secret})
            r.raise_for_status()
            events = (r.json() or {}).get("events") or []
        except requests.RequestException as e:
            log(f"hook fetch: {e}", "WARN")
            return
        if not events:
            return
        processed = self.state.setdefault("seen_events", [])
        seen = set(processed)
        pings = []
        affected = set()
        for entry in events:
            event_id = entry.get("event_id") or ""
            project_name = (entry.get("project") or "").strip()
            if event_id in seen:
                continue
            seen.add(event_id)
            if project_name.startswith("apple:"):
                project = self._apple_project(project_name[len("apple:"):])
            else:
                slug = project_name.lower()
                project = next((p for p in self._projects_ok()
                                if p["env"].replace("RC_SECRET_", "").lower() == slug), None)
            if project is None:
                log(f"hook event for unknown project {project_name!r}", "WARN")
                continue
            try:
                event = json.loads(entry.get("payload") or "{}")
            except (json.JSONDecodeError, TypeError):
                continue
            self.state.setdefault("hook_seen", {})[project["name"]] = time.time()
            line = self._hook_line(project, event)
            if line:
                pings.append(line)
                affected.add(project["name"])
        self.state["seen_events"] = (processed + list(seen - set(processed)))[-1000:]
        save_state(self.state)
        if affected:
            suppress = self.state.setdefault("hook_suppress", {})
            for name in affected:
                suppress[name] = time.time() + 1800
            save_state(self.state)
        if pings:
            text = "\n".join(pings)
            for chat_id in self.chat_ids:
                self.send(text, chat_id=chat_id)
            log(f"hook pings: {'; '.join(pings)}")
        try:
            ids = [e["id"] for e in events]
            requests.post(f"{self.hook_url}/ack", timeout=30, json={"ids": ids},
                          headers={"X-Hook-Secret": self.hook_secret})
        except requests.RequestException as e:
            log(f"hook ack: {e}", "WARN")

    # ----------------------------------------------------------------- reviews

    def _tick_reviews(self):
        """Poll App Store Connect customerReviews and ping every new written
        review at any star level, then sweep storefront rating counts so
        stars-only ratings — invisible to customerReviews — ping too.

        Storefront counts lag and include written reviews, so each written
        ping banks a per-territory "pending" count that the sweep consumes
        when the storefront count catches up; the remainder is the
        stars-only delta. First run baselines both silently so history
        never replays."""
        now = time.time()
        if now - self._last_review < self.review_seconds:
            return
        self._last_review = now
        if not self.state.get("alerts_enabled", True):
            return
        try:
            self._ensure_apps()
        except requests.RequestException as e:
            log(f"apps list: {e}", "WARN")
            return
        seen = self.state.setdefault("reviews_seen", {})
        pending = self.state.setdefault("review_pending", {})
        pings = []
        dirty = False
        for app in self._apps_cache["apps"]:
            app_id = app["id"]
            name = app["attributes"].get("name", "?")
            try:
                data = self.asc_get(
                    f"/v1/apps/{app_id}/customerReviews?limit=50&sort=-createdDate")
            except requests.RequestException as e:
                log(f"reviews {name}: {e}", "WARN")
                continue
            reviews = data.get("data") or []
            known = set(seen.get(app_id) or [])
            fresh = [rv for rv in reviews if rv["id"] not in known]
            if app_id not in seen:
                seen[app_id] = [rv["id"] for rv in reviews][-200:]
                dirty = True
                continue
            if not fresh:
                continue
            seen[app_id] = (seen[app_id] + [rv["id"] for rv in fresh])[-200:]
            dirty = True
            pend = pending.setdefault(app_id, {})
            for rv in fresh:
                attrs = rv["attributes"]
                terr = (attrs.get("territory") or "").upper()
                rating = attrs.get("rating") or 1
                banked = pend.get(terr) or [0, 0]
                pend[terr] = [min(banked[0] + 1, 50), min(banked[1] + rating, 250)]
                stars = "⭐" * (attrs.get("rating") or 1)
                title = (attrs.get("title") or "").strip()
                body = (attrs.get("body") or "").strip()
                text = (title or body).replace("\n", " ")[:120]
                nick = (attrs.get("reviewerNickname") or "user").strip()
                flag = self._flag(terr)
                if text:
                    pings.append(f"{stars} {name} — “{text}” — {nick} {flag}".strip())
                else:
                    pings.append(f"{stars} {name} — {nick} {flag}".strip())
        if dirty:
            save_state(self.state)
        if pings:
            for chat_id in self.chat_ids:
                for chunk in self._chunks("\n".join(pings)):
                    self.send(chunk, chat_id=chat_id)
            log(f"review pings: {len(pings)}")
        self._sweep_ratings()

    def _sweep_ratings(self):
        """Diff per-storefront userRatingCount against the persisted baseline
        and ping stars-only ratings: the count delta minus the written reviews
        already pinged (banked per territory by _tick_reviews).

        The ASC API has no rating-count endpoint, so the public iTunes lookup
        is the only source; one request per storefront returns every app at
        once. When exactly one rating is new, the old and new storefront
        averages solve for its star value; otherwise the ping carries the
        count and the average move. First observation of a storefront
        baselines silently."""
        apps = self._apps_cache["apps"]
        if not apps:
            return
        ids = ",".join(a["id"] for a in apps)
        names = {a["id"]: a["attributes"].get("name", "?") for a in apps}
        base = self.state.setdefault("rating_counts", {})
        avgs = self.state.setdefault("rating_avgs", {})
        pend_all = self.state.setdefault("review_pending", {})
        terr_of = {v: k for k, v in TERRITORY_ALPHA2.items()}
        dirty = False
        pings = []
        def lookup(country):
            try:
                r = requests.get("https://itunes.apple.com/lookup", timeout=30,
                                 params={"id": ids, "country": country})
                r.raise_for_status()
                return country, r.json().get("results") or []
            except (requests.RequestException, ValueError) as e:
                log(f"ratings lookup {country}: {e}", "WARN")
                return country, None

        with ThreadPoolExecutor(8) as pool:
            fetched = pool.map(lookup, sorted(set(TERRITORY_ALPHA2.values())))
            for country, app_results in fetched:
                if app_results is None:
                    continue
                for res in app_results:
                    app_id = str(res.get("trackId") or "")
                    if app_id not in names:
                        continue
                    n = res.get("userRatingCount") or 0
                    avg = res.get("averageUserRating") or 0.0
                    old_n = (base.get(app_id) or {}).get(country)
                    if old_n is None:
                        base.setdefault(app_id, {})[country] = n
                        avgs.setdefault(app_id, {})[country] = avg
                        dirty = True
                        continue
                    if n == old_n:
                        continue
                    old_avg = avgs.get(app_id, {}).get(country, 0.0)
                    base[app_id][country] = n
                    avgs[app_id][country] = avg
                    dirty = True
                    if n < old_n:
                        continue
                    delta = n - old_n
                    terr = terr_of.get(country, "")
                    pend = pend_all.get(app_id) or {}
                    banked = pend.get(terr) or [0, 0]
                    consumed = min(delta, banked[0])
                    written_sum = (banked[1] if consumed == banked[0]
                                   else round(banked[1] * consumed / banked[0]))
                    if consumed:
                        left = banked[0] - consumed
                        if left:
                            pend[terr] = [left, banked[1] - written_sum]
                        else:
                            pend.pop(terr, None)
                        if pend:
                            pend_all[app_id] = pend
                        else:
                            pend_all.pop(app_id, None)
                    stars_only = delta - consumed
                    if stars_only <= 0:
                        continue
                    new_sum = avg * n - old_avg * old_n
                    name = names[app_id]
                    flag = self._flag(country)
                    if stars_only == 1:
                        star = new_sum - written_sum
                        if abs(star - round(star)) <= 0.25 and 1 <= round(star) <= 5:
                            stars = "⭐" * round(star)
                            pings.append(f"{stars} {name} — rating only {flag}".strip())
                        else:
                            pings.append(f"{name} — new rating, avg {avg:.1f} {flag}".strip())
                    else:
                        pings.append(f"{name} — {stars_only} new ratings "
                                     f"(avg {old_avg:.1f}→{avg:.1f}) {flag}".strip())
        if dirty:
            save_state(self.state)
        if pings:
            for chat_id in self.chat_ids:
                for chunk in self._chunks("\n".join(pings)):
                    self.send(chunk, chat_id=chat_id)
            log(f"rating pings: {len(pings)}")

    # ------------------------------------------------------------------ loop

    def _tick_revenue(self):
        """Poll RC's realtime revenue metric and ping on new revenue/refunds.

        Baseline is persisted in state, keyed by UTC day, so a restart does not
        swallow revenue that arrived while the bot was down."""
        now = time.time()
        if now - self._last_poll < self.poll_seconds:
            return
        self._last_poll = now
        if not self.state.get("alerts_enabled", True):
            return
        utc_today = datetime.now(timezone.utc).date()
        baseline = self.state.setdefault("revenue_baseline", {})
        pings = []
        dirty = False
        total_today = 0.0
        for p in self._projects_ok():
            v = self.rc_revenue(p, utc_today, utc_today, "proceeds")
            if v is None:
                continue
            total_today += v
            b = baseline.get(p["name"])
            if not b or b.get("day") != utc_today.isoformat():
                baseline[p["name"]] = {"day": utc_today.isoformat(), "value": v}
                dirty = True
                continue
            delta = v - b["value"]
            suppress = (self.state.get("hook_suppress") or {}).get(p["name"], 0)
            if suppress > time.time():
                b["value"] = v
                dirty = True
            elif delta > 0.004:
                pings.append(self._celebrate(p["name"], delta))
                b["value"] = v
                dirty = True
            elif delta < -0.004:
                b["value"] = v
                dirty = True
        milestones = self.state.get("milestones")
        if not milestones or milestones.get("day") != utc_today.isoformat():
            milestones = {"day": utc_today.isoformat(), "crossed": []}
            self.state["milestones"] = milestones
            dirty = True
        for threshold, line in MILESTONES:
            if total_today >= threshold and threshold not in milestones["crossed"]:
                milestones["crossed"].append(threshold)
                pings.append(line)
                dirty = True
        if dirty:
            save_state(self.state)
        if pings:
            text = "\n".join(pings)
            for chat_id in self.chat_ids:
                self.send(text, chat_id=chat_id)
            log(f"revenue ping: {'; '.join(pings)}")

    def _app_monetizes(self, app_id):
        """True when an ASC app has any in-app purchase or subscription group,
        i.e. it can earn money that RevenueCat would be the one to report."""
        for path in (f"/v1/apps/{app_id}/inAppPurchasesV2?limit=1",
                     f"/v1/apps/{app_id}/subscriptionGroups?limit=1"):
            try:
                if self.asc_get(path).get("data"):
                    return True
            except requests.RequestException as e:
                log(f"coverage iap {app_id}: {e}", "WARN")
        return False

    def _coverage_gaps(self):
        """(apps that can take money but have no RC project, RC projects that
        earn but have never delivered a webhook) — every sale that would land
        late or not at all."""
        self._ensure_apps()
        unwired = []
        for app in self._apps_cache["apps"]:
            name = app["attributes"].get("name", "?")
            if self._is_live_covered(name):
                continue
            if self._app_monetizes(app["id"]):
                unwired.append(name)
        proven = set(self.state.get("hook_seen", {})) | set(self.state.get("hook_suppress", {}))
        silent = []
        for p in self._projects_ok():
            if p["name"] in proven:
                continue
            m = self.rc_metrics(p)
            if m and m.get("revenue", 0) > 0:
                silent.append(p["name"])
        return sorted(unwired), sorted(silent)

    def _coverage_text(self, unwired, silent):
        if not unwired and not silent:
            return "\u2705 every earning app is wired to the webhook path."
        lines = ["\u26a0\ufe0f sales that land next-day via the App Store sweep, not live:"]
        if unwired:
            lines.append("no RC project: " + ", ".join(unwired))
        if silent:
            lines.append("RC webhook never delivered: " + ", ".join(silent))
        return "\n".join(lines)

    def _tick_coverage(self):
        """Every 6h, ping when an app that can take money is not wired to the
        live path — no RC project at all, or an RC project whose webhook has
        never fired despite revenue. Only a change in the gap set re-pings."""
        now = time.time()
        if now - self._last_coverage < 21600:
            return
        self._last_coverage = now
        if not self.state.get("alerts_enabled", True):
            return
        unwired, silent = self._coverage_gaps()
        signature = json.dumps([unwired, silent], sort_keys=True)
        if signature == self.state.get("coverage_sig"):
            return
        self.state["coverage_sig"] = signature
        save_state(self.state)
        if not unwired and not silent:
            return
        text = self._coverage_text(unwired, silent)
        for chat_id in self.chat_ids:
            self.send(text, chat_id=chat_id)
        log(f"coverage gaps: {text}")

    def run_forever(self):
        try:
            self.tg_api("setMyCommands", {"commands": [{"command": c, "description": d} for c, d in COMMANDS]})
        except RuntimeError as e:
            log(f"setMyCommands: {e}", "WARN")
        if "update_offset" not in self.state:
            self._drain_backlog()
        log("storebot online")
        while True:
            for tick in (self._tick_hook, self._tick_revenue,
                         self._tick_appstore, self._tick_reviews,
                         self._tick_coverage):
                try:
                    tick()
                except Exception as e:
                    log(f"{tick.__name__}: {e}", "ERROR")
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

    def _tick_appstore(self):
        """Daily sweep of Apple's SALES report — catches every revenue row the
        RevenueCat path can't see (paid downloads + IAPs/subs of apps without
        an RC project). Apple publishes DAILY reports with ~1 day lag and an
        unpredictable hour, so it retries every 10 min all day; RC-covered
        apps are suppressed so a sale never pings twice."""
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if not self.state.get("alerts_enabled", True) or self.state.get("last_as_check") == today:
            return
        if time.time() - getattr(self, "_last_as_try", 0.0) < 600:
            return
        self._last_as_try = time.time()
        try:
            rows = self.asc_sales(date.today() - timedelta(days=1))
            if rows is None:
                log("appstore: yesterday's SALES report not published yet, will retry")
                return
            self._ensure_apps()
        except requests.RequestException as e:
            log(f"appstore check failed: {e}", "WARN")
            return
        pings = self._asc_money_pings(rows)
        self.state["last_as_check"] = today
        save_state(self.state)
        if pings:
            text = "\n".join(pings)
            for chat_id in self.chat_ids:
                self.send(text, chat_id=chat_id)
            log(f"appstore ping: {'; '.join(pings)}")

    def _drain_backlog(self):
        try:
            updates = self.tg_api("getUpdates", {"offset": -1, "timeout": 0}, timeout=15) or []
            self.state["update_offset"] = updates[-1]["update_id"] + 1 if updates else 0
        except RuntimeError as e:
            log(f"backlog drain: {e}", "WARN")
            self.state["update_offset"] = 0
        save_state(self.state)

    def handle(self, text):
        cmd, _, rest = text.partition(" ")
        cmd = cmd.split("@")[0].lower()
        if cmd in ("/help", "/start"):
            self.send("Commands:\n" + "\n".join(f"/{c} — {d}" for c, d in COMMANDS))
        elif cmd == "/status":
            self.send(self._status_text())
        elif cmd == "/coverage":
            self.send(self._coverage_text(*self._coverage_gaps()))
        elif cmd == "/on":
            self.state["alerts_enabled"] = True
            save_state(self.state)
            self.send("Alerts ON.")
        elif cmd == "/off":
            self.state["alerts_enabled"] = False
            save_state(self.state)
            self.send("Alerts OFF. /on to re-enable.")
        elif cmd.startswith("/"):
            self.send("Unknown command. /help")
        else:
            self.send("Use /help for commands.")

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

        def _hook():
            if not self.hook_url:
                return "not configured"
            r = requests.get(f"{self.hook_url}/events", timeout=30,
                             headers={"X-Hook-Secret": self.hook_secret})
            r.raise_for_status()
            n = len((r.json() or {}).get("events") or [])
            return f"relay ok, {n} queued events"
        check("webhook relay", _hook)

        print("SELFTEST " + ("PASS" if ok else "FAIL"))
        return 0 if ok else 1


COMMANDS = [
    ("status", "bot health"),
    ("coverage", "apps not wired to live sale pings"),
    ("on", "enable alerts"),
    ("off", "disable alerts"),
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
