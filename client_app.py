"""
TK ALGO Client - Multi-Account Edition
Executes trades for ALL accounts sharing the same license_key in a single instance.
"""

import json
import time
import logging
import threading
import datetime
import requests
import socketio
import os
import tempfile
import sys
import queue
import platform
import csv
import concurrent.futures

# Try to import tkinter; if it fails, fall back to console mode
try:
    import tkinter as tk
    from tkinter import scrolledtext, simpledialog, messagebox
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

from tkalgo_security import decrypt_payload

MASTER_URL  = "http://198.23.237.249:5050"
WEBHOOK_URL = "http://198.23.237.249:5000"
CURRENT_VERSION = "1.0.0"
ACCOUNTS_FILE = "accounts.json"           # ← loaded from same folder as the exe/script

_http_session = requests.Session()

# Groww instrument map
groww_instrument_map = {}
groww_last_update = 0

# ─────────────────────────────────────────────────────────────────────────────
# MULTI-ACCOUNT STATE
# After successful auth this list is populated with every account that has:
#   license_key == authenticated_license_key  AND  execution_mode == "client"
# ─────────────────────────────────────────────────────────────────────────────
# Lightweight list: [{"name": "Zerodha", "broker": "zerodha", "client_id": "ATL012"}, ...]
# Only used to know which account names to request execution tokens for.
_active_accounts: list[dict] = []
_active_accounts_lock = threading.Lock()


def fetch_accounts_from_server(license_key: str) -> list[dict]:
    """
    Call the master REST API to get client-mode accounts for this license key.
    Returns lightweight dicts (name, broker, client_id) — no credentials.
    Credentials are delivered per-trade inside encrypted execution tokens.
    """
    try:
        # Step 1: get a short-lived JWT for the member API
        resp = _http_session.post(
            f"{WEBHOOK_URL}/api/member/login",
            json={"license_key": license_key},
            timeout=10,
        )
        if resp.status_code != 200:
            log.error(f"[ACCOUNTS] Server login failed: HTTP {resp.status_code} — {resp.text[:100]}")
            return []
        token = resp.json().get("token")
        if not token:
            log.error("[ACCOUNTS] Server login response contained no token")
            return []

        # Step 2: fetch the account list for this member
        resp2 = _http_session.get(
            f"{WEBHOOK_URL}/api/member/accounts",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp2.status_code != 200:
            log.error(f"[ACCOUNTS] /api/member/accounts failed: HTTP {resp2.status_code}")
            return []

        all_accounts = resp2.json().get("accounts", [])
        client_accounts = [
            a for a in all_accounts
            if a.get("execution_mode", "client").lower() == "client"
        ]
        log.info(
            f"[ACCOUNTS] Server returned {len(all_accounts)} account(s), "
            f"{len(client_accounts)} are client-mode"
        )
        return client_accounts

    except Exception as e:
        log.error(f"[ACCOUNTS] Failed to fetch accounts from server: {e}")
        return []




# ─────────────────────────────────────────────────────────────────────────────
# TERMS & CONDITIONS (keep your full T&C text)
# ─────────────────────────────────────────────────────────────────────────────
TERMS_AND_CONDITIONS = """... (keep your full T&C text) ..."""

ACCEPTANCE_FILE = "tk_algo_acceptance.json"

def save_acceptance():
    data = {"accepted": True, "version": "1.0", "timestamp": datetime.datetime.now().isoformat()}
    try:
        with open(ACCEPTANCE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        log.warning(f"Could not save acceptance: {e}")

def has_accepted():
    if os.path.exists(ACCEPTANCE_FILE):
        try:
            with open(ACCEPTANCE_FILE, "r") as f:
                data = json.load(f)
            return data.get("accepted", False)
        except:
            return False
    return False

def show_terms_and_conditions():
    if has_accepted():
        return True
    if not TKINTER_AVAILABLE:
        print("\n" + "="*60)
        print(TERMS_AND_CONDITIONS)
        print("="*60)
        resp = input("\nType 'ACCEPT' to continue, anything else to exit: ").strip().upper()
        if resp == "ACCEPT":
            save_acceptance()
            return True
        return False
    root = tk.Tk()
    root.title("TK ALGO - Terms and Conditions")
    root.geometry("700x650")
    root.configure(bg="#1e1e1e")
    root.attributes('-topmost', True)
    title_label = tk.Label(root, text="TERMS AND CONDITIONS", font=("Arial", 16, "bold"),
                           fg="#10b981", bg="#1e1e1e", pady=10)
    title_label.pack()
    text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=80, height=25,
                                          font=("Consolas", 10), bg="#2d2d2d", fg="#f0f0f0",
                                          insertbackground="white")
    text_area.pack(padx=20, pady=10, fill=tk.BOTH, expand=True)
    text_area.insert(tk.INSERT, TERMS_AND_CONDITIONS)
    text_area.config(state=tk.DISABLED)
    button_frame = tk.Frame(root, bg="#1e1e1e")
    button_frame.pack(pady=15)
    accept_var = tk.BooleanVar(value=False)
    def on_accept():
        accept_var.set(True)
        root.destroy()
    def on_decline():
        accept_var.set(False)
        root.destroy()
    accept_btn = tk.Button(button_frame, text="✓ I ACCEPT", command=on_accept,
                           bg="#10b981", fg="black", font=("Arial", 12, "bold"),
                           padx=20, pady=5, width=15)
    accept_btn.pack(side=tk.LEFT, padx=10)
    decline_btn = tk.Button(button_frame, text="✗ DECLINE (Exit)", command=on_decline,
                            bg="#ef4444", fg="white", font=("Arial", 12, "bold"),
                            padx=20, pady=5, width=15)
    decline_btn.pack(side=tk.LEFT, padx=10)
    checkbox_var = tk.BooleanVar()
    def on_checkbox():
        accept_btn.config(state=tk.NORMAL if checkbox_var.get() else tk.DISABLED)
    checkbox = tk.Checkbutton(root, text="I have read and agree to the Terms and Conditions",
                              variable=checkbox_var, command=on_checkbox,
                              bg="#1e1e1e", fg="#cccccc", selectcolor="#1e1e1e",
                              font=("Arial", 10))
    checkbox.pack(pady=5)
    accept_btn.config(state=tk.DISABLED)
    root.mainloop()
    if accept_var.get():
        save_acceptance()
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE INSTANCE LOCK
# ─────────────────────────────────────────────────────────────────────────────
def is_already_running():
    lock_file = os.path.join(tempfile.gettempdir(), "tk_algo_client.lock")
    try:
        if platform.system() == "Windows":
            global _lock_fd
            _lock_fd = open(lock_file, 'w')
            import msvcrt
            msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            return False
        else:
            _lock_fd = open(lock_file, 'w')
            import fcntl
            fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
    except (IOError, OSError, ImportError):
        return True


# ─────────────────────────────────────────────────────────────────────────────
# GROWW HELPERS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────
_GROWW_MONTHS_3 = ["JAN","FEB","MAR","APR","MAY","JUN",
                   "JUL","AUG","SEP","OCT","NOV","DEC"]

def build_groww_symbol(strike, opt_type, expiry_str=None):
    try:
        d = datetime.datetime.strptime(expiry_str, "%Y-%m-%d")
        dd = f"{d.day:02d}"
        mmm = _GROWW_MONTHS_3[d.month - 1]
        yy = str(d.year)[-2:]
        return f"NSE-NIFTY-{dd}{mmm}{yy}-{int(strike)}-{opt_type.upper()}"
    except Exception as e:
        log.error(f"[GROWW] fallback symbol error: {e}")
        return None

def groww_ref_id():
    return f"TK{int(time.time() * 1000) % 10_000_000_000:010d}"

def _groww_headers(acc):
    return {
        "Authorization": f"Bearer {acc['access_token'].strip()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-API-VERSION": "1.0",
    }

groww_instruments_ready = False

def update_groww_instruments():
    global groww_instrument_map, groww_last_update, groww_instruments_ready
    try:
        url = "https://growwapi-assets.groww.in/instruments/instrument.csv"
        log.info("Downloading Groww instrument CSV...")
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            log.error(f"Groww CSV download failed: HTTP {r.status_code}")
            return
        content = r.content.decode('utf-8')
        new_map = {}
        lines = content.splitlines()
        reader = csv.DictReader(lines)
        count = 0
        for row in reader:
            if row.get('segment') == 'FNO' and row.get('underlying_symbol') == 'NIFTY':
                expiry = row.get('expiry_date', '')[:10]
                strike_raw = row.get('strike_price', '')
                opt_type = row.get('instrument_type', '')
                if expiry and strike_raw and opt_type in ('CE', 'PE'):
                    try:
                        strike = int(float(strike_raw))
                        key = f"NIFTY_{expiry}_{strike}_{opt_type}"
                        new_map[key] = row['trading_symbol']
                        count += 1
                    except:
                        pass
        if new_map:
            groww_instrument_map = new_map
            groww_last_update = time.time()
            groww_instruments_ready = True
            log.info(f"Groww instrument map updated: {count} NIFTY options")
        else:
            log.warning("Groww CSV contained no NIFTY FNO entries")
    except Exception as e:
        log.error(f"Failed to update Groww instruments: {e}")

def _reload_all():
    update_groww_instruments()


# ─────────────────────────────────────────────────────────────────────────────
# GUI MENU (with multi-account token update)
# ─────────────────────────────────────────────────────────────────────────────
gui_queue = queue.Queue()
gui_root = None
status_label = None

def update_gui_from_queue():
    while not gui_queue.empty():
        msg = gui_queue.get_nowait()
        if msg[0] == "status" and status_label:
            status_label.config(text=msg[1])
    if gui_root:
        gui_root.after(100, update_gui_from_queue)

def create_gui_menu():
    global gui_root, status_label
    if not TKINTER_AVAILABLE:
        return None
    gui_root = tk.Tk()
    gui_root.title("TK ALGO Client")
    gui_root.geometry("400x300")
    gui_root.configure(bg="#1e1e1e")
    gui_root.attributes('-topmost', True)
    status_label = tk.Label(gui_root, text="Connecting...", fg="#10b981", bg="#1e1e1e", font=("Arial", 10))
    status_label.pack(pady=10)

    def update_token():
        with _active_accounts_lock:
            accounts = list(_active_accounts)
        if not accounts:
            messagebox.showwarning("No Accounts", "No active accounts to update.")
            return
        for acc in accounts:
            new_token = simpledialog.askstring(
                "Update Token",
                f"Enter new access token for {acc['name']} ({acc['client_id']}):",
                parent=gui_root
            )
            if not new_token:
                continue
            idx = new_token.find("eyJ")
            if idx > 0:
                new_token = new_token[idx:]
            try:
                resp = requests.post(
                    f"{WEBHOOK_URL}/update_token",
                    json={"client_id": acc["client_id"], "access_token": new_token},
                    timeout=10
                )
                if resp.status_code == 200:
                    messagebox.showinfo("Success", f"Token updated for {acc['name']}")
                else:
                    messagebox.showerror("Error", f"Failed for {acc['name']}:\n{resp.text[:200]}")
            except Exception as e:
                messagebox.showerror("Error", f"Request failed for {acc['name']}:\n{e}")

    def test_signal():
        sio_client.emit("test_signal", {"license_key": _license_key})
        messagebox.showinfo("Test Signal", "Test signal sent.")

    def show_local_logs():
        log_file = "tkalgo_client.log"
        if os.path.exists(log_file):
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    lines = f.read().splitlines()
                    content = "\n".join(lines[-50:]) if lines else "Log is empty."
                    messagebox.showinfo("Client Execution Log (last 50 lines)", content)
            except Exception as e:
                messagebox.showerror("Error", f"Could not read log: {e}")
        else:
            messagebox.showinfo("Logs", "No client log file found yet.")

    def show_status():
        with _active_accounts_lock:
            acc_list = "\n".join(
                f"  • {a['name']} ({a['broker']}) — {a['client_id']}"
                for a in _active_accounts
            ) or "  (none)"
        status_txt = (
            f"Connected: {sio_client.connected}\n"
            f"User: {getattr(sio_client, 'auth_name', 'Unknown')}\n"
            f"Last alert: {getattr(sio_client, 'last_alert_time', 'Never')}\n\n"
            f"Active accounts ({len(_active_accounts)}):\n{acc_list}"
        )
        messagebox.showinfo("Status", status_txt)

    btn_frame = tk.Frame(gui_root, bg="#1e1e1e")
    btn_frame.pack(pady=10)
    tk.Button(btn_frame, text="Update Token", command=update_token, bg="#4d9fff", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="View Logs", command=show_local_logs, bg="#888", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="Status", command=show_status, bg="#888", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="Exit", command=lambda: (sio_client.disconnect(), gui_root.destroy()),
              bg="#ef4444", fg="white", width=15).pack(pady=5)
    gui_root.protocol("WM_DELETE_WINDOW", lambda: (sio_client.disconnect(), gui_root.destroy()))
    gui_root.after(100, update_gui_from_queue)
    return gui_root


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
class AsciiStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            msg = (msg.replace('\u2713','[OK]').replace('\u2717','[FAIL]')
                      .replace('\u2705','[OK]').replace('\u274c','[FAIL]')
                      .replace('\u26a0','[WARN]').replace('\u23f3','[WAIT]'))
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        AsciiStreamHandler(),
        logging.FileHandler("tkalgo_client.log", encoding="utf-8")
    ]
)
log = logging.getLogger("TKAlgoClient")


# ─────────────────────────────────────────────────────────────────────────────
# BROKER HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fyers_app_id(acc):
    return acc.get("api_key", "").strip()

def fyers_token(acc):
    raw = acc.get("access_token", "").strip()
    if ":" in raw:
        raw = raw.split(":", 1)[-1]
    return raw

def fyers_model(acc):
    from fyers_apiv3 import fyersModel
    return fyersModel.FyersModel(
        client_id=fyers_app_id(acc),
        token=fyers_token(acc),
        is_async=False,
        log_path=tempfile.gettempdir()
    )


# ─────────────────────────────────────────────────────────────────────────────
# DHAN: client-side live security_id lookup (fallback when payload is missing it)
# ─────────────────────────────────────────────────────────────────────────────
_dhan_sid_cache: dict[str, str] = {}

def _client_dhan_lookup_sid(strike, opt_type, expiry, access_token, client_id) -> str | None:
    cache_key = f"{int(strike)}_{expiry}_{opt_type.upper()}"
    if cache_key in _dhan_sid_cache:
        return _dhan_sid_cache[cache_key]
    try:
        headers = {
            "access-token": access_token,
            "client-id":    client_id,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
        body = {"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I", "Expiry": expiry}
        r = _http_session.post("https://api.dhan.co/v2/optionchain",
                               json=body, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        oc = r.json().get("data", {}).get("oc", {})
        strike_key = str(int(float(strike)))
        side_key   = "call" if opt_type.upper() == "CE" else "put"
        sid = oc.get(strike_key, {}).get(side_key, {}).get("security_id")
        if sid:
            sid = str(sid)
            _dhan_sid_cache[cache_key] = sid
            return sid
    except Exception as e:
        log.error(f"[DHAN SID LOOKUP] {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PLACE ORDER FUNCTIONS  (accept acc dict — unchanged logic)
# ─────────────────────────────────────────────────────────────────────────────
def add_log(name, action, status, detail=""):
    log.info(f"{name} | {action} | {status} | {detail}")


def place_order_dhan(acc, tx, strike, opt_type, ltp, expiry):
    name = acc.get("name", "unknown")
    sid = str(acc.get("security_id", "")).strip()
    if not sid or sid in ("None", "0", ""):
        sid = ""
    if not sid:
        log.warning(f"[DHAN] {name}: sid missing, calling live OC API | {strike}{opt_type} exp={expiry}")
        sid = _client_dhan_lookup_sid(strike, opt_type, expiry,
                                      acc["access_token"], acc["client_id"])
    if not sid:
        msg = f"security_id not found for {strike}{opt_type} exp={expiry}"
        log.error(f"[DHAN] {name}: {msg}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", msg)
        return None

    url = "https://api.dhan.co/v2/orders"
    headers = {
        "access-token": str(acc["access_token"]).strip(),
        "client-id":    str(acc["client_id"]).strip(),
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }
    payload = {
        "dhanClientId":      str(acc["client_id"]).strip(),
        "transactionType":   "BUY" if tx == "BUY" else "SELL",
        "exchangeSegment":   "NSE_FNO",
        "productType":       "INTRADAY",
        "orderType":         "MARKET",
        "validity":          "DAY",
        "securityId":        str(sid),
        "quantity":          int(acc["quantity"]),
        "disclosedQuantity": 0,
        "price":             0.0,
        "afterMarketOrder":  False,
    }
    try:
        r = _http_session.post(url, json=payload, headers=headers, timeout=10, allow_redirects=False)
        resp = r.json() if r.text else {}
    except Exception as e:
        log.error(f"[DHAN] {name}: {e}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", str(e)[:200])
        return None

    status = "OK" if isinstance(resp, dict) and "orderId" in resp else "FAILED"
    if status == "FAILED":
        log.error(f"[DHAN] {name}: FAILED | {resp}")
    add_log(name, f"{tx} {opt_type}{strike}", status, str(resp)[:200])
    return resp


def place_order_zerodha(acc, tx, strike, opt_type, ltp, expiry):
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=acc["api_key"])
    kite.set_access_token(acc["access_token"])
    expiry_date = datetime.datetime.strptime(expiry, "%Y-%m-%d").date()
    instruments = kite.instruments("NFO")
    sym = None
    for inst in instruments:
        if (inst["instrument_type"] == opt_type and
                inst["strike"] == int(strike) and
                inst["expiry"] == expiry_date and
                inst["name"] == "NIFTY"):
            sym = inst["tradingsymbol"]
            break
    if not sym:
        log.error(f"[ZERODHA] {acc.get('name','')} instrument not found for {strike}{opt_type}")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "instrument not found")
        return
    headers = {"X-Kite-Version": "3",
               "Authorization": f"token {acc['api_key']}:{acc['access_token']}"}
    order_data = {
        "variety": "regular", "exchange": "NFO", "tradingsymbol": sym,
        "transaction_type": tx, "quantity": acc["quantity"],
        "order_type": "MARKET", "product": "NRML", "validity": "DAY",
        "tag": "TKALGO", "market_protection": -1,
    }
    r = requests.post("https://api.kite.trade/orders/regular", data=order_data, headers=headers)
    status = "OK" if r.status_code == 200 else "FAILED"
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", status, r.text[:200])
    return r.json() if r.text else {}


def place_order_angel(acc, tx, strike, opt_type, ltp, expiry):
    from SmartApi import SmartConnect
    smart = SmartConnect(api_key=acc["api_key"])
    smart.access_token = acc["access_token"]
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    symbol = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    token = acc.get("symbol_token")
    if not token:
        log.error(f"[ANGEL] {acc.get('name','')} symbol_token missing for {symbol}")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "symbol_token missing")
        return
    resp = smart.placeOrder({
        "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
        "transactiontype": tx, "exchange": "NFO", "ordertype": "MARKET",
        "producttype": "INTRADAY", "duration": "DAY",
        "price": "0", "quantity": str(acc["quantity"]),
    })
    status = "OK" if isinstance(resp, dict) and resp.get("status") else "FAILED"
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", status, str(resp)[:200])
    return resp


def place_order_upstox(acc, tx, strike, opt_type, ltp, expiry):
    import upstox_client
    inst_key = acc.get("instrument_token")
    if not inst_key:
        log.error(f"[UPSTOX] {acc.get('name','')} instrument_token missing")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "instrument_token missing")
        return
    cfg = upstox_client.Configuration()
    cfg.access_token = acc["access_token"]
    api = upstox_client.OrderApi(upstox_client.ApiClient(cfg))
    body = upstox_client.PlaceOrderRequest(
        quantity=acc["quantity"], product="I", validity="DAY",
        price=0, tag="TKALGO", instrument_token=inst_key,
        order_type="MARKET", transaction_type=tx,
        disclosed_quantity=0, trigger_price=0, is_amo=False,
    )
    resp = api.place_order(body, "2.0")
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "OK", str(resp)[:200])
    return resp


def place_order_fyers(acc, action, strike, opt_type, ltp, expiry):
    try:
        app_id = fyers_app_id(acc)
        if not app_id:
            msg = "Fyers: api_key (App ID) missing"
            log.error(f"[{acc['name']}] {msg}")
            add_log(acc["name"], action, "FAILED", msg)
            return
        d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
        month_abbr = d.strftime("%b").upper()
        sym = f"NSE:NIFTY{d.strftime('%y')}{month_abbr}{d.strftime('%d')}{int(strike)}{opt_type.upper()}"
        log.info(f"[{acc['name']}] Fyers {action} | sym={sym} | qty={acc['quantity']}")
        fy = fyers_model(acc)
        data = {
            "symbol": sym, "qty": acc["quantity"], "type": 1,
            "side": 1 if action == "BUY" else -1,
            "productType": "INTRADAY", "limitPrice": 0.0025,
            "stopPrice": 0, "validity": "DAY", "disclosedQty": 0, "offlineOrder": False,
        }
        resp = fy.place_order(data=data)
        log.info(f"[{acc['name']}] Fyers RESP | {resp}")
        if not isinstance(resp, dict):
            add_log(acc["name"], action, "ERROR", f"unexpected: {resp}")
            return
        s    = resp.get("s", "")
        code = resp.get("code", "")
        if s == "ok":
            add_log(acc["name"], action, "OK", f"order={resp.get('id', '')}")
        elif code in (-16, "-16"):
            add_log(acc["name"], action, "FAILED", "Fyers token EXPIRED — regenerate daily token")
        elif code in (-7, "-7"):
            add_log(acc["name"], action, "FAILED", "Fyers -7: bad token format")
        else:
            add_log(acc["name"], action, "ERROR", f"s={s} code={code} | {resp.get('message', '')}"[:200])
    except Exception as e:
        log.error(f"[{acc['name']}] Fyers FAILED | {e}")
        add_log(acc["name"], action, "ERROR", str(e)[:200])


def place_order_groww(acc, tx, strike, opt_type, ltp, expiry):
    name = acc.get("name", "unknown")
    token = acc.get("access_token", "").strip()
    if not token:
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", "No Groww access_token")
        return {}
    key = f"NIFTY_{expiry}_{int(strike)}_{opt_type.upper()}"
    sym = groww_instrument_map.get(key) or build_groww_symbol(strike, opt_type, expiry)
    if not sym:
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED",
                f"Cannot build symbol: strike={strike} expiry={expiry}")
        return {}
    payload = {
        "trading_symbol": sym, "quantity": int(acc["quantity"]),
        "price": 0, "trigger_price": 0, "validity": "DAY",
        "exchange": "NSE", "segment": "FNO", "product": "MIS",
        "order_type": "MARKET", "transaction_type": tx.upper(),
        "order_reference_id": groww_ref_id(),
    }
    try:
        resp = requests.post("https://api.groww.in/v1/order/create",
                             json=payload, headers=_groww_headers(acc), timeout=12)
        rj = {}
        try:
            rj = resp.json()
        except Exception:
            pass
        if resp.status_code == 200:
            order_id = (rj.get("payload") or {}).get("orderId", "")
            add_log(name, f"{tx} {opt_type}{strike}", "OK", f"sym={sym} orderId={order_id}")
        elif resp.status_code == 401:
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", "Token expired (401)")
        else:
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED",
                    f"HTTP {resp.status_code}: {resp.text[:150]}")
        return rj
    except requests.Timeout:
        add_log(name, f"{tx} {opt_type}{strike}", "TIMEOUT", "Groww API timeout (12s)")
        return {}
    except Exception as e:
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", str(e)[:200])
        return {}


def place_order_kotak(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("Kotak not implemented")
def place_order_aliceblue(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("AliceBlue not implemented")
def place_order_flattrade(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("FlatTrade not implemented")
def place_order_iifl(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("IIFL not implemented")


BROKER_HANDLERS = {
    "dhan":       place_order_dhan,
    "zerodha":    place_order_zerodha,
    "angel":      place_order_angel,
    "upstox":     place_order_upstox,
    "fyers":      place_order_fyers,
    "groww":      place_order_groww,
    "kotak":      place_order_kotak,
    "aliceblue":  place_order_aliceblue,
    "flattrade":  place_order_flattrade,
    "iifl":       place_order_iifl,
}


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTE TRADE  ← KEY CHANGE: now loops all active accounts
# ─────────────────────────────────────────────────────────────────────────────
def execute_trade(encrypted_payload: str):
    """
    Decrypt one execution token sent by the master and place the order for
    the single account whose credentials are embedded in the payload.

    Multi-account execution is NOT a loop here.  The loop happens upstream:
    on_trade_alert() requests N tokens (one per account name).  The master
    fires N execution_token events back, each containing one account's full
    credentials.  This function is therefore called N times in parallel
    threads — one call, one account, one order.
    """
    data = decrypt_payload(encrypted_payload, max_age=10.0)
    if "error" in data:
        log.error(f"[EXECUTE] Blocked: {data['error']}")
        return

    acc      = data.get("account", {})
    action   = data.get("action", "BUY").upper()
    strike   = int(data.get("strike", 0))
    opt_type = data.get("opt_type", "CE").upper()
    expiry   = data.get("expiry", "")
    ltp      = float(data.get("ltp", 0))
    broker   = acc.get("broker", "").lower().strip()
    name     = acc.get("name", broker)

    log.info(f"[EXECUTE] {action} {opt_type}{strike} @ {ltp} | {name} ({broker}) | expiry={expiry}")

    handler = BROKER_HANDLERS.get(broker)
    if not handler:
        log.error(f"[EXECUTE] {name}: unknown broker '{broker}'")
        add_log(name, f"{action} {opt_type}{strike}", "FAILED", f"unknown broker: {broker}")
        data.clear()
        return

    try:
        handler(acc, action, strike, opt_type, ltp, expiry)
    except Exception as err:
        log.error(f"[EXECUTE] {name} ({broker}) error: {err}")
        add_log(name, f"{action} {opt_type}{strike}", "FAILED", str(err)[:200])
    finally:
        data.clear()   # wipe credentials from memory after use


def fetch_execution_logs(n=20):
    try:
        r = requests.get(f"{WEBHOOK_URL}/execution_logs", timeout=5)
        if r.ok:
            logs = r.json().get("logs", [])
            if not logs:
                print("\n  [No execution logs yet]")
                return
            print(f"\n{'─'*72}")
            print(f"  {'TIME':<10} {'NAME':<14} {'ACTION':<18} {'STATUS':<8} DETAIL")
            print(f"{'─'*72}")
            for l in logs[:n]:
                print(f"  {l.get('time',''):<10} {l.get('name',''):<14} "
                      f"{l.get('action',''):<18} {l.get('status',''):<8} "
                      f"{l.get('detail','')[:38]}")
            print(f"{'─'*72}")
        else:
            print(f"\n[FAIL] Could not fetch logs: HTTP {r.status_code}")
    except Exception as e:
        print(f"\n[FAIL] Logs unavailable: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SOCKET.IO CLIENT
# ─────────────────────────────────────────────────────────────────────────────
sio_client = socketio.Client(reconnection=True, reconnection_attempts=999, reconnection_delay=3)
_license_key = ""


@sio_client.event
def connect():
    log.info("Connected to Master. Authenticating...")
    sio_client.emit("auth", {"license_key": _license_key, "hwid": "none"})
    threading.Thread(target=_reload_all, daemon=True).start()


@sio_client.event
def disconnect():
    log.warning("Disconnected from Master. Retrying...")
    print("\n[WARN] Connection lost. Reconnecting...")


# ─────────────────────────────────────────────────────────────────────────────
# AUTH RESULT  ← KEY CHANGE: populate _active_accounts here
# ─────────────────────────────────────────────────────────────────────────────
@sio_client.on("auth_result")
def on_auth_result(data):
    global _active_accounts

    if not data.get("ok"):
        log.error(f"[AUTH] Rejected: {data.get('reason')}")
        sio_client.disconnect()
        gui_queue.put(("status", "Authentication failed"))
        return

    name        = data.get("name", "Unknown")
    license_key = data.get("license_key", _license_key)   # server echoes it back
    log.info(f"[AUTH] Authenticated as: {name} (license={license_key})")
    sio_client.auth_name = name

    # Fetch account list from the server REST API (no local file needed)
    def _load_and_store():
        global _active_accounts
        fetched = fetch_accounts_from_server(license_key)
        with _active_accounts_lock:
            _active_accounts = fetched
        if not fetched:
            log.warning(
                f"[ACCOUNTS] No client-mode accounts returned by server for "
                f"license_key='{license_key}'. "
                "Check that accounts exist in accounts.json on the server "
                "with execution_mode='client'."
            )
            gui_queue.put(("status", f"Connected as {name} | 0 accounts (check server)"))
        else:
            log.info(f"[ACCOUNTS] Active client accounts ({len(fetched)}):")
            for acc in fetched:
                log.info(f"           • {acc.get('name','?')} ({acc.get('broker','?')}) — {acc.get('client_id','?')}")
            gui_queue.put(("status", f"Connected as {name} | {len(fetched)} account(s)"))
        print(f"\n[OK] Ready. Watching {len(fetched)} account(s). Waiting for trade alerts...")

    threading.Thread(target=_load_and_store, daemon=True).start()

    # Start Groww instrument download in the background
    threading.Thread(target=update_groww_instruments, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# TRADE ALERT  ← KEY CHANGE: request execution token for every active account
# ─────────────────────────────────────────────────────────────────────────────
@sio_client.on("trade_alert")
def on_trade_alert(data):
    sio_client.last_alert_time = datetime.datetime.now().strftime("%H:%M:%S")
    action   = data.get("action", "?")
    strike   = data.get("strike", "?")
    opt_type = data.get("opt_type", "?")
    ltp      = data.get("ltp", 0)
    signal_id = data.get("signal_id")

    log.info(f"[ALERT] signal={signal_id} | {action} {opt_type}{strike} @ {ltp}")
    print(f"\n[ALERT] {action} {opt_type}{strike} @ {ltp} | signal={signal_id}")

    with _active_accounts_lock:
        accounts_copy = list(_active_accounts)

    if not accounts_copy:
        log.warning("[ALERT] No active accounts — ignoring signal")
        return

    # Parallel token requests for all accounts
    def _request_token_for_account(acc):
        client_id = acc.get("client_id")
        if client_id:
            sio_client.emit("request_execution_token", {
                "signal_id":    signal_id,
                "account_name": client_id,
            })
        else:
            log.warning(f"[ALERT] Account {acc['name']} has no client_id, skipping")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(_request_token_for_account, accounts_copy)


@sio_client.on("execution_token")
def on_execution_token(data):
    if not data.get("ok"):
        log.warning(f"[TOKEN] Denied: {data.get('reason')}")
        return
    payload = data.get("payload")
    threading.Thread(target=execute_trade, args=(payload,), daemon=True).start()


@sio_client.on("update_token_result")
def on_update_token_result(data):
    if data.get("ok"):
        print(f"\n[OK] {data.get('message', 'Token saved.')}")
        if TKINTER_AVAILABLE:
            messagebox.showinfo("Token Update", "Token updated successfully!")
    else:
        print(f"\n[FAIL] {data.get('reason', 'Unknown error')}")
        if TKINTER_AVAILABLE:
            messagebox.showerror("Token Update", f"Failed: {data.get('reason')}")


@sio_client.on("test_signal_result")
def on_test_signal_result(data):
    if data.get("ok"):
        print(f"\n[OK] Test signal sent: {data.get('message')}")
        if TKINTER_AVAILABLE:
            messagebox.showinfo("Test Signal", "Test signal sent successfully!")
    else:
        print(f"\n[FAIL] Test failed: {data.get('reason')}")
        if TKINTER_AVAILABLE:
            messagebox.showerror("Test Signal", f"Failed: {data.get('reason')}")


def check_for_updates():
    try:
        r = requests.get(f"{MASTER_URL}/check_update", timeout=5)
        if r.status_code == 200:
            data = r.json()
            latest = data.get("version")
            if latest and latest != CURRENT_VERSION:
                print("\n" + "!" * 50)
                print(f"      NEW UPDATE AVAILABLE: v{latest}")
                print(f"      Download from: {data.get('url')}")
                print("!" * 50 + "\n")
    except Exception as e:
        log.debug(f"Update check failed: {e}")


def get_license_key_gui():
    if not TKINTER_AVAILABLE:
        return input("Enter your License Key: ").strip()
    root = tk.Tk()
    root.withdraw()
    license_key = simpledialog.askstring("License Key", "Enter your License Key:", parent=root)
    root.destroy()
    return license_key


def main():
    if is_already_running():
        print("TK ALGO Client is already running. Exiting.")
        if TKINTER_AVAILABLE:
            messagebox.showerror("Already Running", "Another instance is already running.")
        return

    if not show_terms_and_conditions():
        print("You declined the Terms and Conditions. Exiting.")
        return

    if TKINTER_AVAILABLE:
        gui = create_gui_menu()
        console_mode = gui is None
    else:
        console_mode = True

    global _license_key
    check_for_updates()
    _license_key = get_license_key_gui()
    if not _license_key:
        print("No license key entered. Exiting.")
        return

    log.info(f"Connecting to {MASTER_URL} ...")
    try:
        sio_client.connect(MASTER_URL, transports=["websocket"])

        if console_mode:
            while True:
                print("\nOptions: [1] Update Token  [2] Test Signal  [3] Logs  [4] Status  [5] Exit")
                cmd = input("> ").strip()
                if cmd == "1":
                    from requests import post
                    with _active_accounts_lock:
                        accounts = list(_active_accounts)
                    if not accounts:
                        print("No active accounts to update. (Did the server return any?)")
                        continue
                    for acc in accounts:
                        print(f"\n--- Updating token for {acc['name']} ({acc['client_id']}) ---")
                        new_token = input("Paste new access token (or press Enter to skip): ").strip()
                        if not new_token:
                            continue
                        idx = new_token.find("eyJ")
                        if idx > 0:
                            new_token = new_token[idx:]
                        try:
                            resp = post(
                                f"{WEBHOOK_URL}/update_token",
                                json={"client_id": acc["client_id"], "access_token": new_token},
                                timeout=10
                            )
                            if resp.status_code == 200:
                                print(f"  ✅ Token updated for {acc['name']}")
                            else:
                                print(f"  ❌ Failed: {resp.text[:100]}")
                        except Exception as e:
                            print(f"  ❌ Request error: {e}")
                elif cmd == "2":
                    sio_client.emit("test_signal", {"license_key": _license_key})
                    print("Test signal sent. Waiting for response...")
                    for _ in range(10):
                        sio_client.sleep(0.1)
                elif cmd == "3":
                    fetch_execution_logs()
                elif cmd == "4":
                    with _active_accounts_lock:
                        accs = list(_active_accounts)
                    print(f"Connected : {sio_client.connected}")
                    print(f"User      : {getattr(sio_client, 'auth_name', 'Unknown')}")
                    print(f"Last alert: {getattr(sio_client, 'last_alert_time', 'Never')}")
                    print(f"Accounts  : {len(accs)}")
                    for a in accs:
                        print(f"  • {a['name']} ({a['broker']}) — {a['client_id']}")
                elif cmd == "5":
                    sio_client.disconnect()
                    sys.exit(0)
                sio_client.sleep(0.05)
        elif gui:
            gui.mainloop()
        else:
            sio_client.wait()

    except KeyboardInterrupt:
        log.info("Shutting down.")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        if TKINTER_AVAILABLE:
            messagebox.showerror("Connection Error", f"Failed to connect: {e}")
        else:
            print(f"Connection failed: {e}")


if __name__ == "__main__":
    main()
