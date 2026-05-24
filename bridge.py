import asyncio
import csv
import json
import random
import re
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests
import websockets

try:
    from neo_api_client import NeoAPI
except Exception:
    NeoAPI = None


HTTP_HOST = "127.0.0.1"
HTTP_PORT = 8765
WS_HOST = "127.0.0.1"
WS_PORT = 8766
APP_VERSION = "0.1.0"
FO_MASTER_PATH = Path(__file__).parent / "neo_api_client" / "api" / "nse_fo.csv"
CM_MASTER_PATH = Path(__file__).parent / "neo_api_client" / "api" / "nse_cm.csv"

KNOWN_BASE_PRICES: Dict[str, float] = {
    "NIFTY": 23134.0,
    "BANKNIFTY": 48620.0,
    "FINNIFTY": 22108.0,
    "MIDCPNIFTY": 11340.0,
    "SENSEX": 76192.0,
    "RELIANCE": 1280.0,
    "TCS": 3820.0,
    "INFY": 1720.0,
    "HDFCBANK": 1700.0,
    "ICICIBANK": 1300.0,
    "SBIN": 820.0,
    "AXISBANK": 1180.0,
    "BAJFINANCE": 7100.0,
    "TATAMOTORS": 720.0,
    "WIPRO": 315.0,
    "HCLTECH": 1720.0,
    "ADANIENT": 2460.0,
    "KOTAKBANK": 2180.0,
    "BHARTIARTL": 1720.0,
    "LT": 3520.0,
    "ITC": 480.0,
    "MARUTI": 12800.0,
    "HINDUNILVR": 2350.0,
    "TATASTEEL": 145.0,
    "NTPC": 375.0,
    "POWERGRID": 335.0,
    "ONGC": 285.0,
    "M&M": 3100.0,
    "SUNPHARMA": 1870.0,
    "BAJAJFINSV": 1680.0,
    "HINDALCO": 680.0,
    "JSWSTEEL": 1000.0,
    "TITAN": 3420.0,
    "COALINDIA": 480.0,
    "BPCL": 310.0,
    "INDUSINDBK": 1050.0,
    "DRREDDY": 6800.0,
    "CIPLA": 1620.0,
    "GAIL": 210.0,
    "EICHERMOT": 5100.0,
    "BAJAJ-AUTO": 9800.0,
    "ASIANPAINT": 2250.0,
}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


class BridgeState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.clients = set()

        self.mode = "simulation"
        self.source = "simulated"
        self.symbol = "NIFTY"
        self.exchange = "NSE"
        self.interval = "1m"
        self.bar_seconds = 60

        self.params: Dict[str, float] = {
            "atr_period": 14,
            "atr_mult": 1.0,
            "rsi_fast": 5,
            "rsi_slow": 14,
            "stoch_k": 5,
            "stoch_d": 3,
            "stoch_smooth": 3,
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "bb_period": 20,
            "bb_std": 2.0,
            "supertrend_len": 10,
            "supertrend_mult": 3.0,
            "vwap_std_period": 20,
        }

        self.bars = 0
        self.trade_count = 0
        self.day_pnl = 0.0

        self.base_price = 23134.0
        self.last_price = self.base_price
        self.last_tick_ts = time.time()
        self.close_prices = deque(maxlen=120)
        self.close_prices.append(self.last_price)

        self.kotak_client = None
        self.kotak_authenticated = False
        self.kotak_live_enabled = False
        self.kotak_creds: Dict[str, str] = {}
        self.fo_master: Dict[tuple[str, int, str], list[Dict[str, Any]]] = self._load_fo_master()
        self.symbol_universe: Dict[str, str] = self._load_symbol_universe()

        self.orders: Dict[str, Dict[str, Any]] = {}

    def _estimate_base_price(self, symbol: str, exchange: str) -> float:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_exchange = str(exchange or "").strip().upper()

        if normalized_symbol in KNOWN_BASE_PRICES:
            return KNOWN_BASE_PRICES[normalized_symbol]

        if normalized_exchange == "BSE" and normalized_symbol == "SENSEX":
            return 76192.0

        # Stable pseudo-price for unknown symbols so changing the symbol changes the feed immediately.
        seed = sum((idx + 1) * ord(ch) for idx, ch in enumerate(normalized_symbol or "NSE"))
        if normalized_symbol.endswith("NIFTY") or normalized_symbol in {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}:
            return float(10000 + (seed % 40000))
        return float(100 + (seed % 4900))

    def _reset_price_series(self, symbol: str, exchange: str) -> None:
        new_base = self._estimate_base_price(symbol, exchange)
        self.base_price = new_base
        self.last_price = new_base
        self.last_tick_ts = time.time()
        self.close_prices.clear()
        self.close_prices.append(new_base)
        self.bars = 0

    def _load_symbol_universe(self) -> Dict[str, str]:
        universe: Dict[str, str] = {}

        if CM_MASTER_PATH.exists():
            with CM_MASTER_PATH.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    symbol = str(row.get("pSymbolName", "")).strip().upper()
                    if symbol:
                        universe[symbol] = "NSE"

        if FO_MASTER_PATH.exists():
            with FO_MASTER_PATH.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    symbol = str(row.get("pSymbolName", "")).strip().upper()
                    if symbol and symbol not in universe:
                        universe[symbol] = "NSE"

        # Keep key broad-market benchmarks discoverable even if not present in NSE master files.
        if "SENSEX" not in universe:
            universe["SENSEX"] = "BSE"

        return universe

    def get_symbols(self, query: str = "", exchange: str = "", limit: int = 0) -> Dict[str, Any]:
        q = (query or "").strip().upper()
        exch = (exchange or "").strip().upper()
        lim = max(0, _to_int(limit, 0))

        with self.lock:
            items = [
                {"symbol": symbol, "exchange": ex}
                for symbol, ex in self.symbol_universe.items()
            ]

        if exch:
            items = [item for item in items if item["exchange"] == exch]

        if q:
            items = [item for item in items if q in item["symbol"]]

        items.sort(key=lambda item: item["symbol"])
        if lim > 0:
            items = items[:lim]

        return {
            "success": True,
            "count": len(items),
            "symbols": items,
        }

    def _load_fo_master(self) -> Dict[tuple[str, int, str], list[Dict[str, Any]]]:
        if not FO_MASTER_PATH.exists():
            return {}

        index: Dict[tuple[str, int, str], list[Dict[str, Any]]] = {}
        with FO_MASTER_PATH.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                symbol = str(row.get("pSymbolName", "")).strip().upper()
                option_type = str(row.get("pOptionType", "")).strip().upper()
                trading_symbol = str(row.get("pTrdSymbol", "")).strip().upper()
                if not symbol or option_type not in {"CE", "PE"} or not trading_symbol:
                    continue

                match = re.match(
                    rf"^{re.escape(symbol)}(?P<expiry>[A-Z0-9]+?)(?P<strike>\d+)(?P<option_type>CE|PE)$",
                    trading_symbol,
                )
                if not match:
                    continue

                strike = _to_int(match.group("strike"))
                if strike <= 0:
                    continue

                key = (symbol, strike, option_type)
                index.setdefault(key, []).append(
                    {
                        "trading_symbol": trading_symbol,
                        "expiry_code": match.group("expiry"),
                        "lot_size": _to_int(row.get("lLotSize") or row.get("iLotSize"), 0),
                    }
                )
        return index

    def _resolve_trading_symbol(
        self,
        symbol: str,
        strike: Optional[int],
        option_type: str,
        expiry_code: str,
        provided_symbol: str,
    ) -> Dict[str, Any]:
        trading_symbol = provided_symbol.strip().upper()
        if trading_symbol:
            return {
                "trading_symbol": trading_symbol,
                "expiry_code": expiry_code,
                "resolution": "manual",
                "resolution_error": "",
            }

        if strike is None or option_type not in {"CE", "PE"}:
            return {
                "trading_symbol": "",
                "expiry_code": expiry_code,
                "resolution": "missing",
                "resolution_error": "Provide strike and option_type to resolve the Kotak trading symbol.",
            }

        candidates = self.fo_master.get((symbol, strike, option_type), [])
        if not candidates:
            return {
                "trading_symbol": f"{symbol}{strike}{option_type}",
                "expiry_code": expiry_code,
                "resolution": "fallback",
                "resolution_error": "No matching contract was found in the bundled F&O master.",
            }

        normalized_expiry = expiry_code.strip().upper()
        if normalized_expiry:
            for candidate in candidates:
                if candidate["expiry_code"] == normalized_expiry:
                    return {
                        "trading_symbol": candidate["trading_symbol"],
                        "expiry_code": candidate["expiry_code"],
                        "lot_size": candidate["lot_size"],
                        "resolution": "master",
                        "resolution_error": "",
                    }
            available = ", ".join(sorted({item["expiry_code"] for item in candidates}))
            return {
                "trading_symbol": "",
                "expiry_code": normalized_expiry,
                "resolution": "ambiguous",
                "resolution_error": f"Expiry {normalized_expiry} was not found. Available expiries: {available}",
            }

        if len(candidates) == 1:
            candidate = candidates[0]
            return {
                "trading_symbol": candidate["trading_symbol"],
                "expiry_code": candidate["expiry_code"],
                "lot_size": candidate["lot_size"],
                "resolution": "master",
                "resolution_error": "",
            }

        available = ", ".join(sorted({item["expiry_code"] for item in candidates}))
        return {
            "trading_symbol": "",
            "expiry_code": "",
            "resolution": "ambiguous",
            "resolution_error": f"Multiple expiries are available for {symbol} {strike}{option_type}: {available}. Enter an expiry code.",
        }

    def _resolve_broker(self, requested: str) -> str:
        broker = requested.strip().lower() or "auto"
        if broker != "auto":
            return broker
        with self.lock:
            if self.kotak_authenticated:
                return "kotak"
        return "paper"

    def _normalize_order_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        symbol = str(payload.get("symbol") or self.symbol).strip().upper() or "NIFTY"
        option_type = str(payload.get("option_type", "")).strip().upper()
        expiry_code = str(payload.get("expiry_code", "")).strip().upper()

        strike_raw = payload.get("strike")
        try:
            strike = int(float(strike_raw)) if strike_raw is not None else None
        except (TypeError, ValueError):
            strike = None

        lots_raw = payload.get("lots", 1)
        try:
            lots = max(1, int(float(lots_raw)))
        except (TypeError, ValueError):
            lots = 1

        lot_size_raw = payload.get("lot_size", 75)
        try:
            lot_size = max(1, int(float(lot_size_raw)))
        except (TypeError, ValueError):
            lot_size = 75

        qty_raw = payload.get("quantity")
        if qty_raw is None or str(qty_raw).strip() == "":
            quantity = lots * lot_size
        else:
            try:
                quantity = max(1, int(float(qty_raw)))
            except (TypeError, ValueError):
                quantity = lots * lot_size

        direction = str(payload.get("direction", "")).strip().lower()
        transaction_type = str(payload.get("transaction_type", "")).strip().upper()
        if not transaction_type:
            if direction == "bull":
                transaction_type = "B"
            elif direction == "bear":
                transaction_type = "S"

        order_type = str(payload.get("order_type", "MKT")).strip().upper()
        if order_type in {"MARKET", "MKT"}:
            order_type = "MKT"

        symbol_resolution = self._resolve_trading_symbol(
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            expiry_code=expiry_code,
            provided_symbol=str(payload.get("trading_symbol", "")),
        )

        resolved_lot_size = _to_int(symbol_resolution.get("lot_size"), 0)
        if resolved_lot_size > 0:
            lot_size = resolved_lot_size
            if qty_raw is None or str(qty_raw).strip() == "":
                quantity = lots * lot_size

        return {
            "exchange_segment": str(payload.get("exchange_segment", "nse_fo")).strip().lower(),
            "product": str(payload.get("product", "NRML")).strip().upper(),
            "price": str(payload.get("price", "0")).strip() or "0",
            "order_type": order_type,
            "quantity": str(quantity),
            "validity": str(payload.get("validity", "DAY")).strip().upper() or "DAY",
            "trading_symbol": str(symbol_resolution.get("trading_symbol", "")).strip().upper(),
            "transaction_type": transaction_type,
            "lots": lots,
            "lot_size": lot_size,
            "expiry_code": str(symbol_resolution.get("expiry_code", expiry_code)).strip().upper(),
            "symbol_resolution": str(symbol_resolution.get("resolution", "")),
            "resolution_error": str(symbol_resolution.get("resolution_error", "")),
        }

    @staticmethod
    def _extract_order_id(response: Dict[str, Any]) -> Optional[str]:
        candidates = [
            response.get("nOrdNo"),
            response.get("order_id"),
            response.get("orderId"),
        ]
        data = response.get("data")
        if isinstance(data, dict):
            candidates.extend(
                [
                    data.get("nOrdNo"),
                    data.get("order_id"),
                    data.get("orderId"),
                ]
            )
        for val in candidates:
            if val is None:
                continue
            s = str(val).strip()
            if s:
                return s
        return None

    def set_config(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            symbol_changed = False
            if "mode" in cfg and str(cfg["mode"]).strip():
                val = str(cfg["mode"]).strip().lower()
                if val in {"simulation", "webhook", "tvdatafeed", "kotak"}:
                    self.mode = val
            if "symbol" in cfg and str(cfg["symbol"]).strip():
                new_symbol = str(cfg["symbol"]).strip().upper()
                symbol_changed = symbol_changed or new_symbol != self.symbol
                self.symbol = new_symbol
            if "exchange" in cfg and str(cfg["exchange"]).strip():
                new_exchange = str(cfg["exchange"]).strip().upper()
                symbol_changed = symbol_changed or new_exchange != self.exchange
                self.exchange = new_exchange
            if "interval" in cfg and str(cfg["interval"]).strip():
                self.interval = str(cfg["interval"]).strip().lower()

            if symbol_changed:
                self._reset_price_series(self.symbol, self.exchange)

            self.source = "simulated" if self.mode == "simulation" else self.mode
            return {
                "mode": self.mode,
                "symbol": self.symbol,
                "exchange": self.exchange,
                "interval": self.interval,
                "base_price": round(self.base_price, 2),
            }

    def set_bar_size(self, seconds: int) -> int:
        with self.lock:
            self.bar_seconds = max(15, min(3600, int(seconds)))
            return self.bar_seconds

    def get_ping(self) -> Dict[str, Any]:
        with self.lock:
            clients = len(self.clients)
            return {
                "status": "ok",
                "version": APP_VERSION,
                "mode": self.mode,
                "clients": clients,
                "bars": self.bars,
                "pandas_ta": False,
            }

    def update_params(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        updated: Dict[str, float] = {}
        errors: Dict[str, str] = {}
        with self.lock:
            for key, val in payload.items():
                if key not in self.params:
                    continue
                try:
                    f = float(val)
                    if f <= 0:
                        errors[key] = "must be > 0"
                        continue
                    self.params[key] = f
                    updated[key] = f
                except (TypeError, ValueError):
                    errors[key] = "must be numeric"
        return {"updated": updated, "errors": errors}

    def authenticate_kotak(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        consumer_key = str(payload.get("consumer_key", "")).strip()
        consumer_secret = str(payload.get("consumer_secret", "")).strip()
        mobile = str(payload.get("mobile", "")).strip()
        if not consumer_key:
            return {"success": False, "message": "consumer_key is required"}

        with self.lock:
            self.kotak_creds = {
                "consumer_key": consumer_key,
                "consumer_secret": consumer_secret,
                "mobile": mobile,
            }
            self.kotak_authenticated = False
            self.kotak_live_enabled = False
            if NeoAPI is not None:
                try:
                    # This SDK version accepts consumer_key in constructor.
                    self.kotak_client = NeoAPI(environment="prod", consumer_key=consumer_key)
                except Exception:
                    self.kotak_client = None
            else:
                self.kotak_client = None

        if self.kotak_client is None:
            return {
                "success": False,
                "message": "Kotak client initialization failed. Check your SDK install and credentials.",
            }

        return {
            "success": True,
            "message": "Kotak client initialized. Enter mobile, UCC, TOTP, and MPIN to authenticate.",
        }

    def verify_kotak_totp(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        mobile = str(payload.get("mobile") or self.kotak_creds.get("mobile", "")).strip()
        ucc = str(payload.get("ucc", "")).strip()
        totp = str(payload.get("totp", "")).strip()
        mpin = str(payload.get("mpin", "")).strip()

        if not mobile:
            return {"success": False, "message": "mobile is required"}
        if not ucc:
            return {"success": False, "message": "ucc is required"}
        if not totp:
            return {"success": False, "message": "totp is required"}
        if not mpin:
            return {"success": False, "message": "mpin is required"}

        with self.lock:
            client = self.kotak_client
            if client is None:
                return {"success": False, "message": "Kotak client is not initialized. Click connect first."}

        try:
            login_response = client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
        except Exception as exc:
            return {"success": False, "message": f"Kotak totp_login failed: {exc}"}

        if isinstance(login_response, dict) and login_response.get("Error"):
            return {"success": False, "message": f"Kotak totp_login failed: {login_response.get('Error')}"}

        try:
            validate_response = client.totp_validate(mpin=mpin)
        except Exception as exc:
            return {"success": False, "message": f"Kotak totp_validate failed: {exc}"}

        if isinstance(validate_response, dict) and validate_response.get("Error"):
            return {"success": False, "message": f"Kotak totp_validate failed: {validate_response.get('Error')}"}

        with self.lock:
            self.kotak_authenticated = True
            self.kotak_creds["mobile"] = mobile
            self.kotak_creds["ucc"] = ucc

        return {
            "success": True,
            "message": "Kotak authenticated. Live mode remains disabled until you enable it explicitly.",
        }

    def set_kotak_live_mode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        enabled_raw = payload.get("enabled", False)
        enabled = enabled_raw if isinstance(enabled_raw, bool) else str(enabled_raw).strip().lower() in {"1", "true", "yes", "on", "real", "live"}
        with self.lock:
            self.kotak_live_enabled = enabled
        return {
            "success": True,
            "enabled": enabled,
            "message": "Kotak LIVE execution enabled" if enabled else "Kotak execution set to simulation",
        }

    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        requested_broker = str(payload.get("broker", "auto")).strip().lower() or "auto"
        broker = self._resolve_broker(requested_broker)
        normalized = self._normalize_order_payload(payload)

        if broker == "kotak":
            with self.lock:
                if not self.kotak_authenticated:
                    return {
                        "success": False,
                        "message": "Kotak not authenticated. Initialize the client and complete TOTP login first.",
                    }

                trading_symbol = normalized["trading_symbol"]
                exchange_segment = normalized["exchange_segment"]
                quantity = normalized["quantity"]
                if not (trading_symbol and exchange_segment and quantity):
                    return {
                        "success": False,
                        "message": normalized["resolution_error"] or "For live Kotak orders, provide trading_symbol, exchange_segment, and quantity.",
                    }

                live_enabled = self.kotak_live_enabled

                if self.kotak_client is None:
                    return {
                        "success": False,
                        "message": "Kotak client is not initialized. Reconnect and try again.",
                    }

            if not live_enabled:
                order_id = f"KS-{int(time.time())}-{str(uuid.uuid4())[:8]}"
                with self.lock:
                    self.trade_count += 1
                    self.orders[order_id] = {
                        "id": order_id,
                        "broker": broker,
                        "payload": payload,
                        "normalized": normalized,
                        "ts": time.time(),
                        "status": "simulated",
                    }
                return {
                    "success": True,
                    "broker": broker,
                    "order_id": order_id,
                    "fill_price": round(self.last_price + random.uniform(-2.5, 2.5), 2),
                    "simulated": True,
                    "message": "Kotak authenticated, but LIVE execution is disabled. Order was simulated.",
                }

            try:
                live_response = self.kotak_client.place_order(
                    exchange_segment=normalized["exchange_segment"],
                    product=normalized["product"],
                    price=normalized["price"],
                    order_type=normalized["order_type"],
                    quantity=normalized["quantity"],
                    validity=normalized["validity"],
                    trading_symbol=normalized["trading_symbol"],
                    transaction_type=normalized["transaction_type"],
                )
            except Exception as exc:
                return {
                    "success": False,
                    "broker": broker,
                    "message": f"Kotak place_order failed: {exc}",
                }

            if isinstance(live_response, dict) and live_response.get("Error"):
                return {
                    "success": False,
                    "broker": broker,
                    "message": f"Kotak place_order failed: {live_response.get('Error')}",
                }

            order_id = self._extract_order_id(live_response if isinstance(live_response, dict) else {})
            if not order_id:
                order_id = f"KO-{int(time.time())}-{str(uuid.uuid4())[:8]}"

            with self.lock:
                self.trade_count += 1
                self.orders[order_id] = {
                    "id": order_id,
                    "broker": broker,
                    "payload": payload,
                    "normalized": normalized,
                    "raw_response": live_response,
                    "ts": time.time(),
                    "status": "placed",
                }

            return {
                "success": True,
                "broker": broker,
                "order_id": order_id,
                "fill_price": round(self.last_price, 2),
                "simulated": False,
                "message": "Kotak order placed",
            }

        order_id = f"{broker[:2].upper()}-{int(time.time())}-{str(uuid.uuid4())[:8]}"
        with self.lock:
            self.trade_count += 1
            self.orders[order_id] = {
                "id": order_id,
                "broker": broker,
                "payload": payload,
                "normalized": normalized,
                "ts": time.time(),
                "status": "filled",
            }

        fill_price = round(self.last_price + random.uniform(-2.5, 2.5), 2)
        return {
            "success": True,
            "broker": broker,
            "order_id": order_id,
            "fill_price": fill_price,
            "simulated": broker != "kotak",
            "message": "Order accepted",
        }

    def exit_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        order_id = str(payload.get("order_id", "")).strip()
        with self.lock:
            if order_id and order_id in self.orders:
                self.orders[order_id]["status"] = "exited"
        return {"success": True, "order_id": order_id or None, "message": "Exit acknowledged"}

    def _gen_indicator_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        dt = max(0.2, min(2.5, now - self.last_tick_ts))
        self.last_tick_ts = now

        # Random walk around last price.
        vol = 3.0 if self.interval in {"1m", "3m"} else 6.0
        drift = random.uniform(-1.0, 1.0) * dt
        self.last_price = max(100.0, self.last_price + drift * vol)
        self.close_prices.append(self.last_price)
        self.bars += 1

        bid = round(self.last_price - random.uniform(0.2, 0.9), 2)
        ask = round(self.last_price + random.uniform(0.2, 0.9), 2)
        spread = round(max(0.1, ask - bid), 2)

        closes = list(self.close_prices)
        mean_price = sum(closes) / len(closes)
        hi = max(closes[-20:]) if len(closes) >= 20 else max(closes)
        lo = min(closes[-20:]) if len(closes) >= 20 else min(closes)

        atr = round(max(5.0, (hi - lo) / max(3.0, min(len(closes), self.params["atr_period"]))), 2)
        bbw = round(max(0.2, min(4.5, (hi - lo) / max(1.0, mean_price) * 100 * 5)), 2)

        rsi5 = round(max(1.0, min(99.0, 50 + (self.last_price - mean_price) * 0.35 + random.uniform(-7, 7))), 1)
        stk = round(max(1.0, min(99.0, 50 + (self.last_price - mean_price) * 0.5 + random.uniform(-10, 10))), 1)
        std = round(max(1.0, min(99.0, stk + random.uniform(-8, 8))), 1)
        macd_h = round((self.last_price - mean_price) * 0.03 + random.uniform(-1.3, 1.3), 2)

        vwap = round(mean_price, 2)
        vwap1u = round(vwap + atr * 0.8, 2)
        vwap1d = round(vwap - atr * 0.8, 2)

        bbm = round(mean_price, 2)
        bbu = round(bbm + atr * 1.8, 2)
        bbl = round(bbm - atr * 1.8, 2)

        candle = random.choice(["bull", "bear", "doji", "hammer", "shooting_star"])
        vol_label = random.choice(["low", "normal", "high", "spike"])
        day_trend = "bull" if self.last_price >= self.base_price else "bear"

        pivot = round((hi + lo + self.last_price) / 3, 2)
        s1 = round(2 * pivot - hi, 2)
        r1 = round(2 * pivot - lo, 2)

        ready = self.bars >= 20
        return {
            "ready": ready,
            "bars": self.bars,
            "mode": self.mode,
            "source": self.source,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "ltp": round(self.last_price, 2),
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "atr": atr,
            "vwap": vwap,
            "vwap1u": vwap1u,
            "vwap1d": vwap1d,
            "rsi5": rsi5,
            "stk": stk,
            "std": std,
            "macd_h": macd_h,
            "bbu": bbu,
            "bbm": bbm,
            "bbl": bbl,
            "bbw": bbw,
            "candle": candle,
            "vol_label": vol_label,
            "day_trend": day_trend,
            "pivot": pivot,
            "s1": s1,
            "r1": r1,
            "day_pnl": round(self.day_pnl, 2),
            "trade_count": self.trade_count,
            "atr_period": int(self.params.get("atr_period", 14)),
            "atr_mult": self.params.get("atr_mult", 1.0),
        }


STATE = BridgeState()
WS_LOOP: Optional[asyncio.AbstractEventLoop] = None


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, status: int, body: str, content_type: str = "text/plain") -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", f"{content_type}; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(data)


class BridgeHandler(BaseHTTPRequestHandler):
    server_version = "NeoBridgeHTTP/0.1"

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path in {"/", "/terminal"}:
            page = Path(__file__).parent / "docs" / "nifty_scalper_terminal.html"
            if page.exists():
                text_response(self, 200, page.read_text(encoding="utf-8"), content_type="text/html")
            else:
                text_response(self, 404, "Terminal page not found")
            return

        if path in {"/terminal-main", "/main"}:
            page = Path(__file__).parent / "docs" / "nifty_scalper_terminal.html"
            if page.exists():
                text_response(self, 200, page.read_text(encoding="utf-8"), content_type="text/html")
            else:
                text_response(self, 404, "Main terminal page not found")
            return

        if path in {"/terminal-v21", "/v21"}:
            page = Path(__file__).parent / "docs" / "nexz_bot_v2_1" / "nexz_bot_v2_1.html"
            if page.exists():
                text_response(self, 200, page.read_text(encoding="utf-8"), content_type="text/html")
            else:
                text_response(self, 404, "v2.1 terminal page not found")
            return

        if path == "/ping":
            json_response(self, 200, STATE.get_ping())
            return

        if path == "/params":
            with STATE.lock:
                json_response(self, 200, dict(STATE.params))
            return

        if path == "/ngrok":
            try:
                resp = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=1.5)
                data = resp.json() if resp.ok else {}
                tunnels = data.get("tunnels", []) if isinstance(data, dict) else []
                pub = None
                for t in tunnels:
                    url = t.get("public_url")
                    if isinstance(url, str) and url.startswith("https://"):
                        pub = url
                        break
                if pub:
                    json_response(self, 200, {"public_url": pub, "webhook_url": f"{pub}/tv/webhook"})
                else:
                    json_response(self, 200, {"public_url": None})
            except Exception:
                json_response(self, 200, {"public_url": None})
            return

        if path == "/symbols":
            query = parse_qs(parsed.query)
            q = str((query.get("q", [""])[0] or "")).strip()
            exchange = str((query.get("exchange", [""])[0] or "")).strip()
            limit = _to_int((query.get("limit", [0])[0]), 0)
            result = STATE.get_symbols(query=q, exchange=exchange, limit=limit)
            json_response(self, 200, result)
            return

        json_response(self, 404, {"error": "not_found", "path": path})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        payload = self._read_json()

        if path == "/config":
            updated = STATE.set_config(payload)
            json_response(self, 200, updated)
            return

        if path == "/bar_size":
            seconds = int(payload.get("seconds", STATE.bar_seconds) or STATE.bar_seconds)
            out = STATE.set_bar_size(seconds)
            json_response(self, 200, {"bar_seconds": out})
            return

        if path == "/params":
            result = STATE.update_params(payload)
            json_response(self, 200, result)
            return

        if path == "/auth":
            result = STATE.authenticate_kotak(payload)
            json_response(self, 200, result)
            return

        if path == "/auth/otp":
            result = STATE.verify_kotak_totp(payload)
            json_response(self, 200, result)
            return

        if path == "/auth/totp":
            result = STATE.verify_kotak_totp(payload)
            json_response(self, 200, result)
            return

        if path == "/auth/live-mode":
            result = STATE.set_kotak_live_mode(payload)
            json_response(self, 200, result)
            return

        if path == "/kite/auth":
            json_response(
                self,
                200,
                {
                    "success": False,
                    "message": "Kite flow is not included in this repository bridge.",
                },
            )
            return

        if path == "/kite/token":
            json_response(
                self,
                200,
                {
                    "success": False,
                    "message": "Kite flow is not included in this repository bridge.",
                },
            )
            return

        if path == "/order/place":
            result = STATE.place_order(payload)
            json_response(self, 200, result)
            return

        if path == "/order/exit":
            result = STATE.exit_order(payload)
            json_response(self, 200, result)
            return

        json_response(self, 404, {"error": "not_found", "path": path})

    def log_message(self, _format: str, *_args: Any) -> None:
        # Keep console output clean; status is shown by the terminal UI itself.
        return


async def ws_handler(websocket, _path=None):
    with STATE.lock:
        STATE.clients.add(websocket)
    try:
        async for message in websocket:
            if isinstance(message, str) and message.lower() == "ping":
                await websocket.send("pong")
    except Exception:
        pass
    finally:
        with STATE.lock:
            STATE.clients.discard(websocket)


async def ws_broadcast_loop() -> None:
    while True:
        await asyncio.sleep(1.0)
        with STATE.lock:
            payload = STATE._gen_indicator_snapshot()
            clients = list(STATE.clients)

        if not clients:
            continue

        data = json.dumps(payload)
        dead = []
        for ws in clients:
            try:
                await ws.send(data)
            except Exception:
                dead.append(ws)

        if dead:
            with STATE.lock:
                for ws in dead:
                    STATE.clients.discard(ws)


async def ws_main() -> None:
    server = await websockets.serve(ws_handler, WS_HOST, WS_PORT, ping_interval=20, ping_timeout=20)
    try:
        await ws_broadcast_loop()
    finally:
        server.close()
        await server.wait_closed()


def run_ws_server() -> None:
    global WS_LOOP
    loop = asyncio.new_event_loop()
    WS_LOOP = loop
    asyncio.set_event_loop(loop)
    loop.run_until_complete(ws_main())


def run_http_server() -> None:
    server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), BridgeHandler)
    print(f"[bridge] HTTP http://{HTTP_HOST}:{HTTP_PORT}  WS ws://{WS_HOST}:{WS_PORT}")
    print("[bridge] Open http://127.0.0.1:8765 in your browser")
    server.serve_forever()


def main() -> None:
    ws_thread = threading.Thread(target=run_ws_server, daemon=True)
    ws_thread.start()
    run_http_server()


if __name__ == "__main__":
    main()
