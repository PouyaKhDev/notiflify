#!/usr/bin/env python3
"""Binance Perpetuals Blow-Off Top / Parabolic Short Scanner.

Completely isolated scanner that detects parabolic blow-off tops on Binance USDT-M Perpetuals:
- Universe filtering: Binance USDT perpetuals with >= 15% 24h gain and >= $10M 24h quote volume.
- Climactic Volume: Identifies volume spikes (>= 3.0x 20-period volume SMA) on 1H or 15M.
- RSI Exhaustion: Confirms overextended momentum (RSI >= 70) on 1H or 15M.
- Upper Wick Calculation: Measures upper rejection wick (% of candle range) for manual monitoring.
- Funding Rate & Snapshot: Real-time funding rate and countdown to next settlement snapshot.
- Execution: Continuous 5-minute loop (clock-aligned) or one-shot scan (--once).
- Delivery: Rich formatted terminal table + optional Telegram alerts.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import http.server
import json
import os
import socketserver
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# Terminal ANSI color constants
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"

DEFAULT_CONFIG_FILE = "blowoff.conf.env"
RESTRICTED_LOCATION_MSG = (
    "Binance returned HTTP 451 (Restricted Location).\n"
    "Your current IP or active proxy is recognized by Binance as a restricted jurisdiction (e.g. US/UK).\n"
    "To resolve this, please:\n"
    "  1. Switch your VPN or proxy exit node to an allowed country (e.g., Germany, Netherlands, Singapore, Turkey).\n"
    "  2. Or configure a non-US proxy in blowoff.conf.env (HTTP_PROXY / HTTPS_PROXY)."
)


def load_blowoff_config(config_path: str = DEFAULT_CONFIG_FILE) -> Dict[str, Any]:
    """Loads configuration key-value pairs from blowoff.conf.env with sensible defaults."""
    candidate_paths = [
        config_path,
        os.path.join(os.getcwd(), config_path),
        os.path.join(os.path.dirname(__file__), "..", config_path),
        os.path.join(os.path.dirname(__file__), config_path),
    ]
    resolved_path = None
    for p in candidate_paths:
        if p and os.path.exists(p):
            resolved_path = os.path.abspath(p)
            break

    raw_cfg: Dict[str, str] = {}
    if resolved_path and os.path.exists(resolved_path):
        try:
            with open(resolved_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        raw_cfg[k.strip()] = v.strip()
        except Exception as e:
            print(f"Warning: Failed to read config file {resolved_path}: {e}", file=sys.stderr)

    def _get_float(key: str, default: float) -> float:
        val = os.getenv(key, raw_cfg.get(key, ""))
        try:
            return float(val) if val else default
        except ValueError:
            return default

    def _get_int(key: str, default: int) -> int:
        val = os.getenv(key, raw_cfg.get(key, ""))
        try:
            return int(val) if val else default
        except ValueError:
            return default

    def _get_str(key: str, default: str) -> str:
        env_val = os.getenv(key)
        if env_val is not None and env_val.strip() != "":
            return env_val.strip()
        return raw_cfg.get(key, default).strip()

    def _get_bool(key: str, default: bool) -> bool:
        val = os.getenv(key, raw_cfg.get(key, "")).strip().lower()
        if not val:
            return default
        return val in ("true", "1", "yes", "on")

    stables_raw = _get_str("EXCLUDE_STABLES", "USDC,FDUSD,TUSD,EUR,BUSD,DAI,USDP,AEUR")
    stables_set = {s.strip().upper() for s in stables_raw.split(",") if s.strip()}

    return {
        "binance_base_url": _get_str("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/"),
        "http_proxy": _get_str("HTTP_PROXY", ""),
        "https_proxy": _get_str("HTTPS_PROXY", ""),
        "request_timeout": _get_int("REQUEST_TIMEOUT", 10),
        "workers": _get_int("WORKERS", 8),
        "min_pump_pct": _get_float("MIN_PUMP_PCT", 15.0),
        "min_turnover": _get_float("MIN_TURNOVER", 10000000.0),
        "exclude_stables": stables_set,
        "vol_climax_mult": _get_float("VOL_CLIMAX_MULT", 3.0),
        "rsi_threshold": _get_float("RSI_THRESHOLD", 70.0),
        "wick_rejection_threshold": _get_float("WICK_REJECTION_THRESHOLD", 40.0),
        "scan_interval_sec": _get_int("SCAN_INTERVAL_SEC", 300),
        "telegram_enabled": _get_bool("TELEGRAM_ENABLED", False),
        "telegram_bot_token": _get_str("TELEGRAM_BOT_TOKEN", ""),
        "telegram_chat_id": _get_str("TELEGRAM_CHAT_ID", ""),
    }


def create_session(config: Dict[str, Any]) -> requests.Session:
    """Builds a configured requests session with proxies and headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    proxies = {}
    if config.get("http_proxy"):
        proxies["http"] = config["http_proxy"]
    if config.get("https_proxy"):
        proxies["https"] = config["https_proxy"]
    if proxies:
        session.proxies.update(proxies)
    return session


def calculate_rsi(closes: List[float], period: int = 14) -> float:
    """Computes Wilder's Smoothed RSI matching TradingView/Binance bit-for-bit."""
    if len(closes) <= period:
        return 50.0

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0.0, d) for d in deltas]
    losses = [max(0.0, -d) for d in deltas]

    alpha = 1.0 / period
    avg_gain = gains[0]
    avg_loss = losses[0]
    for i in range(1, len(gains)):
        avg_gain = (1.0 - alpha) * avg_gain + alpha * gains[i]
        avg_loss = (1.0 - alpha) * avg_loss + alpha * losses[i]

    if avg_loss == 0.0:
        return 100.0 if avg_gain > 0 else 50.0
    if avg_gain == 0.0:
        return 0.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def extract_candle_metrics(candles: List[Any], vol_period: int = 20, rsi_period: int = 14) -> Optional[Dict[str, float]]:
    """Calculates indicators on historical OHLCV data for the most recently CLOSED candle.

    Accepts list of raw candles: [open_time, open, high, low, close, volume, ...] or dicts.
    Uses index -2 (the finalized closed candle).
    """
    if not candles or len(candles) < max(vol_period, rsi_period) + 2:
        return None

    parsed = []
    for c in candles:
        if isinstance(c, dict):
            parsed.append((float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]), float(c["volume"])))
        else:
            parsed.append((float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])))

    # Closed candle is index -2 (index -1 is active candle)
    c_open, c_high, c_low, c_close, c_volume = parsed[-2]

    candle_range = c_high - c_low
    upper_wick = c_high - max(c_open, c_close)
    wick_ratio = (upper_wick / candle_range * 100.0) if candle_range > 0 else 0.0

    # 20 closed candles up to and including index -2
    vol_window = [c[4] for c in parsed[-(vol_period + 2):-1]]
    vol_sma = (sum(vol_window) / len(vol_window)) if vol_window else 0.0
    vol_spike = (c_volume / vol_sma) if vol_sma > 0 else 1.0

    # Closes up to index -2
    closed_closes = [c[3] for c in parsed[:-1]]
    rsi_val = calculate_rsi(closed_closes, period=rsi_period)

    return {
        "price": c_close,
        "high": c_high,
        "low": c_low,
        "volume": c_volume,
        "vol_sma": vol_sma,
        "vol_spike": vol_spike,
        "rsi": rsi_val,
        "wick_pct": wick_ratio,
    }


def fetch_24h_tickers(session: requests.Session, base_url: str, timeout: int = 10) -> List[Dict[str, Any]]:
    """Fetches all 24h ticker data from Binance Futures."""
    url = f"{base_url}/fapi/v1/ticker/24hr"
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code == 451:
            raise PermissionError(RESTRICTED_LOCATION_MSG)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and data.get("code"):
            raise ValueError(f"Binance API error: {data.get('msg')} (code: {data.get('code')})")
        return []
    except requests.RequestException as e:
        if getattr(e.response, "status_code", None) == 451:
            raise PermissionError(RESTRICTED_LOCATION_MSG) from e
        raise


def fetch_premium_index(session: requests.Session, base_url: str, timeout: int = 10) -> Dict[str, Dict[str, Any]]:
    """Fetches real-time funding rates and settlement countdowns for all symbols."""
    url = f"{base_url}/fapi/v1/premiumIndex"
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code == 451:
            raise PermissionError(RESTRICTED_LOCATION_MSG)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            return {}

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        funding_map = {}
        for item in data:
            sym = item.get("symbol")
            if not sym:
                continue
            try:
                rate_float = float(item.get("lastFundingRate", 0.0)) * 100.0  # percentage
            except (ValueError, TypeError):
                rate_float = 0.0

            next_time = item.get("nextFundingTime", 0)
            diff_sec = max(0, int((next_time - now_ms) / 1000)) if next_time else 0
            hours = diff_sec // 3600
            minutes = (diff_sec % 3600) // 60
            seconds = diff_sec % 60
            countdown_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            funding_map[sym] = {
                "funding_rate_pct": rate_float,
                "next_funding_time": next_time,
                "countdown": countdown_str,
            }
        return funding_map
    except Exception:
        return {}


def filter_top_gainers(
    tickers: List[Dict[str, Any]],
    min_pump_pct: float,
    min_turnover: float,
    exclude_stables: Set[str],
) -> List[Dict[str, Any]]:
    """Filters candidate USDT perpetuals up >= min_pump_pct with turnover >= min_turnover."""
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue

        base = sym[:-4].upper()
        if base in exclude_stables:
            continue

        try:
            chg = float(t.get("priceChangePercent", 0.0))
            quote_vol = float(t.get("quoteVolume", 0.0))
            price = float(t.get("lastPrice", 0.0))
        except (ValueError, TypeError):
            continue

        if chg >= min_pump_pct and quote_vol >= min_turnover:
            candidates.append({
                "symbol": sym,
                "base": base,
                "price": price,
                "change_24h": chg,
                "quote_volume": quote_vol,
            })

    candidates.sort(key=lambda x: x["change_24h"], reverse=True)
    return candidates


def fetch_klines(
    session: requests.Session,
    base_url: str,
    symbol: str,
    interval: str,
    limit: int = 35,
    timeout: int = 10,
) -> Optional[List[List[Any]]]:
    """Fetches OHLCV klines for a single symbol and timeframe."""
    url = f"{base_url}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        resp = session.get(url, params=params, timeout=timeout)
        if resp.status_code == 451:
            raise PermissionError(RESTRICTED_LOCATION_MSG)
        resp.raise_for_status()
        raw = resp.json()
        if not isinstance(raw, list) or len(raw) == 0:
            return None
        return raw
    except Exception:
        return None


def evaluate_single_coin(
    session: requests.Session,
    base_url: str,
    candidate: Dict[str, Any],
    funding_info: Dict[str, Any],
    vol_climax_mult: float,
    rsi_threshold: float,
    timeout: int = 10,
) -> Optional[Dict[str, Any]]:
    """Fetches 1H and 15M klines and evaluates blow-off top conditions."""
    sym = candidate["symbol"]

    # 1. Fetch 1H and 15M candles
    df_1h = fetch_klines(session, base_url, sym, "1h", limit=35, timeout=timeout)
    df_15m = fetch_klines(session, base_url, sym, "15m", limit=35, timeout=timeout)

    metrics_1h = extract_candle_metrics(df_1h)
    metrics_15m = extract_candle_metrics(df_15m)

    if not metrics_1h or not metrics_15m:
        return None

    # Condition 1: Climactic Volume on either 1H OR 15M (>= vol_climax_mult)
    vol_1h_spike = metrics_1h["vol_spike"]
    vol_15m_spike = metrics_15m["vol_spike"]
    has_vol_climax = (vol_1h_spike >= vol_climax_mult) or (vol_15m_spike >= vol_climax_mult)

    # Condition 2: High RSI on either 1H OR 15M (>= rsi_threshold)
    rsi_1h = metrics_1h["rsi"]
    rsi_15m = metrics_15m["rsi"]
    has_high_rsi = (rsi_1h >= rsi_threshold) or (rsi_15m >= rsi_threshold)

    # Qualification check
    if not (has_vol_climax and has_high_rsi):
        return None

    # Funding rate info
    fund_data = funding_info.get(sym, {"funding_rate_pct": 0.0, "countdown": "N/A"})

    return {
        "symbol": sym,
        "price": candidate["price"],
        "change_24h": candidate["change_24h"],
        "quote_volume": candidate["quote_volume"],
        "vol_1h_spike": vol_1h_spike,
        "vol_15m_spike": vol_15m_spike,
        "rsi_1h": rsi_1h,
        "rsi_15m": rsi_15m,
        "wick_1h_pct": metrics_1h["wick_pct"],
        "wick_15m_pct": metrics_15m["wick_pct"],
        "funding_rate_pct": fund_data["funding_rate_pct"],
        "countdown": fund_data["countdown"],
    }


def format_money(val: float) -> str:
    """Formats large dollar amounts nicely."""
    if val >= 1_000_000_000:
        return f"${val / 1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"${val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val / 1_000:.2f}K"
    return f"${val:.2f}"


def format_price(price: float) -> str:
    """Formats instrument price with appropriate precision."""
    if price >= 100:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.001:
        return f"{price:.5f}"
    return f"{price:.7f}"


def format_terminal_table(results: List[Dict[str, Any]], cfg: Dict[str, Any]) -> str:
    """Renders the detected blow-off top setups into an ANSI colored table."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = []
    lines.append(f"{BOLD}{MAGENTA}===================================================================================================={RESET}")
    lines.append(f"{BOLD}{WHITE}🚨 BINANCE FUTURES: PARABOLIC BLOW-OFF TOP SCANNER{RESET}  |  {CYAN}{now_utc}{RESET}")
    lines.append(
        f"{DIM}Filters: 24h Pump >= +{cfg['min_pump_pct']:.1f}% | Vol Spike >= {cfg['vol_climax_mult']:.1f}x (1H or 15M) | "
        f"RSI >= {cfg['rsi_threshold']:.1f} (1H or 15M){RESET}"
    )
    lines.append(f"{BOLD}{MAGENTA}===================================================================================================={RESET}")

    if not results:
        lines.append(f"{YELLOW}No parabolic blow-off setups detected at this time.{RESET}")
        lines.append(f"{DIM}Waiting for next scanning cycle...{RESET}")
        lines.append(f"{BOLD}{MAGENTA}===================================================================================================={RESET}")
        return "\n".join(lines)

    header = (
        f"{BOLD}{WHITE}{'SYMBOL':<12} {'PRICE':<12} {'24H CHG':<10} {'24H VOL':<10} "
        f"{'1H VOL':<9} {'15M VOL':<9} {'1H RSI':<8} {'15M RSI':<8} "
        f"{'1H WICK':<9} {'15M WICK':<9} {'FUNDING':<10} {'SNAPSHOT':<9}{RESET}"
    )
    lines.append(header)
    lines.append(f"{DIM}{'-' * 115}{RESET}")

    for r in results:
        sym = r["symbol"]
        price_str = format_price(r["price"])
        chg_str = f"+{r['change_24h']:.1f}%"
        vol_str = format_money(r["quote_volume"])

        # 1H Vol Spike Color
        v1h = r["vol_1h_spike"]
        v1h_color = RED if v1h >= 5.0 else (YELLOW if v1h >= cfg["vol_climax_mult"] else WHITE)
        v1h_str = f"{v1h_color}{v1h:.1f}x{RESET}"

        # 15M Vol Spike Color
        v15 = r["vol_15m_spike"]
        v15_color = RED if v15 >= 5.0 else (YELLOW if v15 >= cfg["vol_climax_mult"] else WHITE)
        v15_str = f"{v15_color}{v15:.1f}x{RESET}"

        # RSI Colors
        r1h = r["rsi_1h"]
        r1h_color = RED if r1h >= 80.0 else (YELLOW if r1h >= cfg["rsi_threshold"] else WHITE)
        r1h_str = f"{r1h_color}{r1h:.1f}{RESET}"

        r15 = r["rsi_15m"]
        r15_color = RED if r15 >= 80.0 else (YELLOW if r15 >= cfg["rsi_threshold"] else WHITE)
        r15_str = f"{r15_color}{r15:.1f}{RESET}"

        # Upper Wick Rejections
        w1h = r["wick_1h_pct"]
        w1h_color = GREEN if w1h >= cfg["wick_rejection_threshold"] else WHITE
        w1h_str = f"{w1h_color}{w1h:.1f}%{RESET}"

        w15 = r["wick_15m_pct"]
        w15_color = GREEN if w15 >= cfg["wick_rejection_threshold"] else WHITE
        w15_str = f"{w15_color}{w15:.1f}%{RESET}"

        # Funding Rate
        fr = r["funding_rate_pct"]
        fr_color = RED if fr >= 0.05 else (YELLOW if fr > 0.01 else (GREEN if fr < 0 else WHITE))
        fr_str = f"{fr_color}{fr:+.4f}%{RESET}"

        countdown_str = r["countdown"]

        line = (
            f"{BOLD}{CYAN}{sym:<12}{RESET} "
            f"{price_str:<12} "
            f"{GREEN}{chg_str:<10}{RESET} "
            f"{vol_str:<10} "
            f"{v1h_str:<18} "
            f"{v15_str:<18} "
            f"{r1h_str:<17} "
            f"{r15_str:<17} "
            f"{w1h_str:<18} "
            f"{w15_str:<18} "
            f"{fr_str:<19} "
            f"{countdown_str:<9}"
        )
        lines.append(line)

    lines.append(f"{DIM}{'-' * 115}{RESET}")
    lines.append(
        f"{DIM}Legend: Vol Spike >= {cfg['vol_climax_mult']}x | RSI >= {cfg['rsi_threshold']} | Wick >= {cfg['wick_rejection_threshold']}% (rejection confirmed){RESET}"
    )
    lines.append(f"{BOLD}{MAGENTA}===================================================================================================={RESET}")
    return "\n".join(lines)


def send_telegram_alert(
    results: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    session: Optional[requests.Session] = None,
) -> None:
    """Dispatches detected setups to Telegram if configured."""
    bot_token = cfg.get("telegram_bot_token", "").strip()
    chat_id = cfg.get("telegram_chat_id", "").strip()
    enabled = cfg.get("telegram_enabled", False)

    # Enable if explicitly enabled OR if both bot_token and chat_id are provided
    if not (enabled or (bot_token and chat_id)) or not results:
        return
    if not bot_token or not chat_id:
        return

    sess = session or requests.Session()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    text_lines = [
        f"🚨 <b>BINANCE BLOW-OFF TOP SHORT SETUPS</b>",
        f"🕒 <code>{now_utc}</code>",
        f"─────────────────────────────",
    ]

    for r in results:
        sym = r["symbol"]
        price = format_price(r["price"])
        chg = f"+{r['change_24h']:.1f}%"
        v1h = f"{r['vol_1h_spike']:.1f}x"
        v15 = f"{r['vol_15m_spike']:.1f}x"
        r1h = f"{r['rsi_1h']:.1f}"
        r15 = f"{r['rsi_15m']:.1f}"
        w1h = f"{r['wick_1h_pct']:.1f}%"
        w15 = f"{r['wick_15m_pct']:.1f}%"
        fund = f"{r['funding_rate_pct']:+.4f}%"
        snap = r["countdown"]

        text_lines.append(
            f"⚡ <b>#{sym}</b> | <code>${price}</code> (<b>{chg}</b>)\n"
            f"• <b>Vol Spike:</b> 1H: <code>{v1h}</code> | 15M: <code>{v15}</code>\n"
            f"• <b>RSI:</b> 1H: <code>{r1h}</code> | 15M: <code>{r15}</code>\n"
            f"• <b>Upper Wick:</b> 1H: <code>{w1h}</code> | 15M: <code>{w15}</code>\n"
            f"• <b>Funding:</b> <code>{fund}</code> (Snap in: <code>{snap}</code>)\n"
        )

    text_lines.append(f"─────────────────────────────")
    text_lines.append(f"⚠️ <i>Monitor structure on lower timeframe before entry.</i>")
    message = "\n".join(text_lines)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        sess.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Warning: Failed to send Telegram alert: {e}", file=sys.stderr)


# Global state dictionary accessible by the health check server
SCANNER_STATE: Dict[str, Any] = {
    "status": "starting",
    "cycles_completed": 0,
    "last_scan_utc": "none",
    "next_scan_utc": "pending",
    "setups_detected": 0,
}


def start_health_server(port: int) -> None:
    """Runs a minimal HTTP server responding with 200 OK for Render free-tier health checks."""
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            body = (
                f"OK: Binance Blow-Off Top Scanner is running.\n"
                f"Status: {SCANNER_STATE['status']}\n"
                f"Cycles Completed: {SCANNER_STATE['cycles_completed']}\n"
                f"Last Scan (UTC): {SCANNER_STATE['last_scan_utc']}\n"
                f"Next Scan (UTC): {SCANNER_STATE['next_scan_utc']}\n"
                f"Setups in Last Scan: {SCANNER_STATE['setups_detected']}\n"
            )
            self.wfile.write(body.encode("utf-8"))

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()

        def log_message(self, format, *args):
            pass  # Silence routine health check logs

    try:
        with socketserver.TCPServer(("", port), HealthHandler) as httpd:
            print(f"{GREEN}Render health check HTTP server listening on port {port}{RESET}", flush=True)
            httpd.serve_forever()
    except Exception as e:
        print(f"Warning: Health check server failed on port {port}: {e}", file=sys.stderr, flush=True)


def run_scan(config: Dict[str, Any], session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """Executes a single end-to-end market scan."""
    sess = session or create_session(config)
    base_url = config["binance_base_url"]
    timeout = config["request_timeout"]

    # 1. Fetch 24h tickers
    tickers = fetch_24h_tickers(sess, base_url, timeout=timeout)

    # 2. Filter top gainers (>15% 24h change & >$10M turnover)
    candidates = filter_top_gainers(
        tickers,
        min_pump_pct=config["min_pump_pct"],
        min_turnover=config["min_turnover"],
        exclude_stables=config["exclude_stables"],
    )

    if not candidates:
        return []

    # 3. Batch fetch real-time funding rates
    funding_info = fetch_premium_index(sess, base_url, timeout=timeout)

    # 4. Concurrently evaluate candidates (fetch 1H and 15M candles)
    results: List[Dict[str, Any]] = []
    workers = min(config["workers"], max(1, len(candidates)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                evaluate_single_coin,
                sess,
                base_url,
                cand,
                funding_info,
                config["vol_climax_mult"],
                config["rsi_threshold"],
                timeout,
            ): cand["symbol"]
            for cand in candidates
        }

        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    # Sort results by 24h gain descending
    results.sort(key=lambda x: x["change_24h"], reverse=True)
    return results


def main() -> int:
    """CLI Entry point for the scanner."""
    parser = argparse.ArgumentParser(
        description="Binance Futures Blow-Off Top & Parabolic Short Scanner"
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_FILE,
        help="Path to configuration file (default: blowoff.conf.env)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single scan cycle and exit immediately (default is continuous 5m loop)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Interval in seconds for continuous loop mode (default: 300 / 5 minutes)",
    )
    parser.add_argument(
        "--min-pump",
        type=float,
        default=None,
        help="Minimum 24h pump percentage override (e.g. 15.0)",
    )
    parser.add_argument(
        "--min-vol",
        type=float,
        default=None,
        help="Minimum 24h turnover in USDT override (e.g. 10000000)",
    )
    parser.add_argument(
        "--vol-spike",
        type=float,
        default=None,
        help="Climactic volume spike multiplier override (e.g. 3.0)",
    )
    parser.add_argument(
        "--rsi",
        type=float,
        default=None,
        help="RSI exhaustion threshold override (e.g. 70.0)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw JSON results instead of colored table",
    )

    args = parser.parse_args()

    # Load configuration and apply CLI overrides
    cfg = load_blowoff_config(args.config)
    if args.interval is not None:
        cfg["scan_interval_sec"] = args.interval
    if args.min_pump is not None:
        cfg["min_pump_pct"] = args.min_pump
    if args.min_vol is not None:
        cfg["min_turnover"] = args.min_vol
    if args.vol_spike is not None:
        cfg["vol_climax_mult"] = args.vol_spike
    if args.rsi is not None:
        cfg["rsi_threshold"] = args.rsi

    session = create_session(cfg)

    # Single-run mode (--once)
    if args.once:
        try:
            results = run_scan(cfg, session)
            if args.json:
                print(json.dumps(results, indent=2))
            else:
                print(format_terminal_table(results, cfg))

            # Dispatch Telegram notifications if configured
            send_telegram_alert(results, cfg, session)
            return 0
        except PermissionError as pe:
            print(f"\n{RED}{BOLD}ERROR:{RESET} {pe}\n", file=sys.stderr)
            return 2
        except Exception as e:
            print(f"\n{RED}{BOLD}SCANNER ERROR:{RESET} {e}\n", file=sys.stderr)
            return 1

    # Continuous loop mode (default)
    interval = cfg["scan_interval_sec"]
    print(
        f"{BOLD}{GREEN}Starting Binance Blow-Off Top Scanner in continuous loop mode (every {interval}s)...{RESET}"
    )
    # Start background health check HTTP server if PORT is set (for Render Free-tier Web Service)
    port_env = os.getenv("PORT")
    if port_env:
        try:
            port = int(port_env)
            t = threading.Thread(target=start_health_server, args=(port,), daemon=True)
            t.start()
        except ValueError:
            pass

    cycle_count = 0
    try:
        while True:
            cycle_count += 1
            cycle_start = time.time()
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            SCANNER_STATE["status"] = "scanning"

            try:
                results = run_scan(cfg, session)
                if args.json:
                    print(json.dumps(results, indent=2), flush=True)
                else:
                    print(format_terminal_table(results, cfg), flush=True)

                SCANNER_STATE["setups_detected"] = len(results)
                # Dispatch Telegram notifications if configured
                send_telegram_alert(results, cfg, session)

            except PermissionError as pe:
                print(f"\n{RED}{BOLD}ERROR:{RESET} {pe}\n", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"\n{RED}{BOLD}SCANNER ERROR during cycle:{RESET} {e}\n", file=sys.stderr, flush=True)

            SCANNER_STATE["cycles_completed"] = cycle_count
            SCANNER_STATE["last_scan_utc"] = now_str
            SCANNER_STATE["status"] = "sleeping"

            # Sleep until the next cycle
            elapsed = time.time() - cycle_start
            sleep_duration = max(1.0, interval - elapsed)

            # Align with 5-minute clock boundary if interval is 300
            if interval == 300:
                now_sec = time.time()
                next_5m = ((int(now_sec) // 300) + 1) * 300
                sleep_duration = max(5.0, next_5m - now_sec)

            next_scan_time = datetime.fromtimestamp(time.time() + sleep_duration, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            SCANNER_STATE["next_scan_utc"] = next_scan_time
            print(f"{DIM}[Cycle #{cycle_count}] Scan finished. Sleeping {sleep_duration:.1f}s until {next_scan_time}...{RESET}\n", flush=True)

            time.sleep(sleep_duration)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}Scanner stopped by user.{RESET}")
        return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
