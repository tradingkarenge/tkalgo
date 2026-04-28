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

# Try to import tkinter; if it fails, fall back to console mode
try:
    import tkinter as tk
    from tkinter import scrolledtext, simpledialog, messagebox
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

from tkalgo_security import decrypt_payload   # removed get_hwid

MASTER_URL  = "http://198.23.237.249:5050"
WEBHOOK_URL = "http://198.23.237.249:5000"
CURRENT_VERSION = "1.0.0"

# ========== TERMS AND CONDITIONS (keep your full T&C text) ==========
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
    # Tkinter version (unchanged)
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

# ========== SINGLE INSTANCE LOCK ==========
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

# ========== GUI MENU ==========
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
        new_token = simpledialog.askstring("Update Token", "Enter new access token:", parent=gui_root)
        if new_token:
            sio_client.emit("update_token", {"license_key": _license_key, "access_token": new_token})
            messagebox.showinfo("Token Update", "Token sent. Waiting for confirmation...")

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
        status = f"Connected: {sio_client.connected}\nUser: {getattr(sio_client, 'auth_name', 'Unknown')}\nLast alert: {getattr(sio_client, 'last_alert_time', 'Never')}"
        messagebox.showinfo("Status", status)

    btn_frame = tk.Frame(gui_root, bg="#1e1e1e")
    btn_frame.pack(pady=10)

    tk.Button(btn_frame, text="Update Token", command=update_token, bg="#4d9fff", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="View Logs", command=show_local_logs, bg="#888", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="Status", command=show_status, bg="#888", fg="black", width=15).pack(pady=5)
    tk.Button(btn_frame, text="Exit", command=lambda: (sio_client.disconnect(), gui_root.destroy()), bg="#ef4444", fg="white", width=15).pack(pady=5)

    gui_root.protocol("WM_DELETE_WINDOW", lambda: (sio_client.disconnect(), gui_root.destroy()))
    gui_root.after(100, update_gui_from_queue)
    return gui_root

# ========== LOGGING ==========
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

# ========== BROKER HELPERS ==========
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

# ------------------------------------------------------------
# PLACE ORDER FUNCTIONS (no broker_tokens, only account fields)
# ------------------------------------------------------------
def place_order_dhan(acc, tx, strike, opt_type, ltp, expiry):
    from dhanhq import dhanhq
    name = acc.get("name", "unknown")

    # ── Step 1: Build dhanhq client ───────────────────────────────────
    try:
        dhan = dhanhq(access_token=acc["access_token"], client_id=acc["client_id"])
    except TypeError:
        try:
            dhan = dhanhq(acc["access_token"])
            dhan.client_id = acc["client_id"]
        except TypeError:
            dhan = dhanhq(acc["client_id"], acc["access_token"])

    # ── Step 2: Resolve security_id ───────────────────────────────────

    # Level 1: from decrypted payload (sent by server via bridge)
    sid = str(acc.get("security_id", "")).strip()
    if sid and sid not in ("None", "0", ""):
        log.info(f"[DHAN] {name}: sid from payload = {sid}")
    else:
        sid = ""

    # Level 2: live Dhan OC API (client calls with own token)
    if not sid:
        log.warning(f"[DHAN] {name}: security_id missing in payload. "
                    f"Calling live OC API | {strike}{opt_type} exp={expiry}")
        sid = _client_dhan_lookup_sid(
            strike, opt_type, expiry,
            acc["access_token"], acc["client_id"]
        )

    if not sid:
        msg = (f"security_id not found for {strike}{opt_type} exp={expiry}. "
               f"Server may have stale EXPIRY_DATE. Contact admin.")
        log.error(f"[DHAN] {name}: {msg}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", msg)
        return None

    # ── Step 3: Place order ───────────────────────────────────────────
    log.info(f"[DHAN] {name}: {tx} {opt_type}{strike} | sid={sid} | "
             f"qty={acc['quantity']} | expiry={expiry}")
    try:
        resp = dhan.place_order(
            security_id      = str(sid),
            exchange_segment = dhan.NSE_FNO,
            transaction_type = dhan.BUY if tx == "BUY" else dhan.SELL,
            quantity         = acc["quantity"],
            order_type       = dhan.MARKET,
            product_type     = dhan.INTRA,
            price            = 0,
        )
    except Exception as e:
        log.error(f"[DHAN] {name}: place_order exception: {e}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", str(e)[:200])
        return None

    log.info(f"[DHAN] {name}: resp = {resp}")
    status = "OK" if isinstance(resp, dict) and resp.get("status") == "success" else "FAILED"
    if status == "FAILED":
        err_code = ""
        try:
            err_code = (resp.get("remarks", {}) or {}).get("error_code", "")
        except Exception:
            pass
        log.error(f"[DHAN] {name}: FAILED | error_code={err_code} | {resp}")
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
        log.error(f"Zerodha: instrument not found for strike {strike}")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "instrument not found")
        return
    headers = {"X-Kite-Version": "3",
               "Authorization": f"token {acc['api_key']}:{acc['access_token']}"}
    order_data = {
        "variety": "regular", "exchange": "NFO", "tradingsymbol": sym,
        "transaction_type": tx, "quantity": acc["quantity"],
        "order_type": "MARKET", "product": "NRML", "validity": "DAY", "tag": "TKALGO",
        "market_protection": -1
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
        log.error(f"Angel: symbol_token missing for {symbol}")
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
        log.error(f"Upstox: instrument_token missing for {strike} {opt_type} {expiry}")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "instrument_token missing")
        return
    cfg = upstox_client.Configuration()
    cfg.access_token = acc["access_token"]
    api = upstox_client.OrderApi(upstox_client.ApiClient(cfg))
    body = upstox_client.PlaceOrderRequest(
        quantity=acc["quantity"], product="I", validity="DAY",
        price=0, tag="TKALGO", instrument_token=inst_key,
        order_type="MARKET", transaction_type=tx,
        disclosed_quantity=0, trigger_price=0, is_amo=False
    )
    resp = api.place_order(body, "2.0")
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "OK", str(resp)[:200])
    return resp

# Fyers: always build symbol from expiry (no map, no token)
def place_order_fyers(acc, action, strike, opt_type, ltp, expiry):
    try:
        app_id = fyers_app_id(acc)
        if not app_id:
            msg = "Fyers: api_key (App ID) missing"
            log.error(f"[{acc['name']}] {msg}")
            add_log(acc["name"], action, "FAILED", msg)
            return

        # Build symbol from expiry (fallback, no map)
        d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
        month_abbr = d.strftime("%b").upper()
        sym = f"NSE:NIFTY{d.strftime('%y')}{month_abbr}{d.strftime('%d')}{int(strike)}{opt_type.upper()}"
        log.info(f"[{acc['name']}] Fyers {action} | sym={sym} | qty={acc['quantity']}")

        fy = fyers_model(acc)
        data = {
            "symbol": sym,
            "qty": acc["quantity"],
            "type": 1,                     # MARKET order
            "side": 1 if action == "BUY" else -1,
            "productType": "INTRADAY",
            "limitPrice": 0.0025,          # Minimum required for market orders
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False
        }
        resp = fy.place_order(data=data)
        log.info(f"[{acc['name']}] Fyers RESP | {resp}")

        if not isinstance(resp, dict):
            add_log(acc["name"], action, "ERROR", f"unexpected: {resp}")
            return

        s = resp.get("s", "")
        code = resp.get("code", "")
        if s == "ok":
            add_log(acc["name"], action, "OK", f"order={resp.get('id', '')}")
        elif code in (-16, "-16"):
            msg = "Fyers -16: token EXPIRED - regenerate daily token"
            log.error(f"[{acc['name']}] {msg}")
            add_log(acc["name"], action, "FAILED", msg)
        elif code in (-7, "-7"):
            msg = "Fyers -7: bad token format - api_key must be App ID"
            log.error(f"[{acc['name']}] {msg}")
            add_log(acc["name"], action, "FAILED", msg)
        else:
            msg = f"Fyers s={s} code={code} | {resp.get('message', '')}"
            log.error(f"[{acc['name']}] {msg}")
            add_log(acc["name"], action, "ERROR", msg[:200])
    except Exception as e:
        log.error(f"[{acc['name']}] Fyers FAILED | {e}")
        add_log(acc["name"], action, "ERROR", str(e)[:200])

def place_order_groww(acc, tx, strike, opt_type, ltp, expiry):
    name = acc.get("name", "unknown")
    log.info(f"[GROWW] {name} | {tx} {opt_type}{strike} | expiry={expiry}")

    token = acc.get("access_token", "").strip()
    if not token:
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", "No Groww access_token")
        return {}

    # Build symbol directly
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    dd = d.strftime("%d")
    mmm = d.strftime("%b").upper()
    yy = d.strftime("%y")
    strike_padded = f"{int(strike):05d}"
    sym = f"NSE-NIFTY-{dd}{mmm}{yy}-{strike_padded}-{opt_type.upper()}"
    log.info(f"[GROWW] {name} | Symbol={sym} | Qty={acc['quantity']}")

    payload = {
        "trading_symbol": sym,
        "quantity": int(acc["quantity"]),
        "price": 0,
        "trigger_price": 0,
        "validity": "DAY",
        "exchange": "NSE",
        "segment": "FNO",
        "product": "MIS",
        "order_type": "MARKET",
        "transaction_type": tx.upper(),
        "order_reference_id": f"TK{int(time.time())%10000}",
    }
    headers = {
        "Authorization": f"Bearer {acc['access_token'].strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-API-VERSION": "1.0",
    }
    try:
        resp = requests.post("https://api.groww.in/v1/order/create", json=payload, headers=headers, timeout=10)
        log.info(f"[GROWW] {name} | HTTP {resp.status_code} | {resp.text[:200]}")
        if resp.status_code == 200:
            add_log(name, f"{tx} {opt_type}{strike}", "OK", f"sym={sym}")
            return resp.json() if resp.text else {}
        else:
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", f"HTTP {resp.status_code}: {resp.text[:100]}")
            return {}
    except Exception as e:
        log.error(f"[GROWW] {name}: {e}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", str(e)[:200])
        return {}

# Dummy handlers for unsupported brokers
def place_order_kotak(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("Kotak not implemented")
def place_order_aliceblue(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("AliceBlue not implemented")
def place_order_flattrade(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("FlatTrade not implemented")
def place_order_iifl(acc, tx, strike, opt_type, ltp, expiry):
    log.warning("IIFL not implemented")

BROKER_HANDLERS = {
    "dhan": place_order_dhan,
    "zerodha": place_order_zerodha,
    "angel": place_order_angel,
    "upstox": place_order_upstox,
    "fyers": place_order_fyers,
    "groww": place_order_groww,
    "kotak": place_order_kotak,
    "aliceblue": place_order_aliceblue,
    "flattrade": place_order_flattrade,
    "iifl": place_order_iifl,
}

def add_log(name, action, status, detail=""):
    log.info(f"{name} | {action} | {status} | {detail}")

def execute_trade(encrypted_payload: str):
    data = decrypt_payload(encrypted_payload, max_age=10.0)
    if "error" in data:
        log.error(f"Execution blocked: {data['error']}")
        return
    acc    = data.get("account", {})
    action = data.get("action", "BUY").upper()
    strike = int(data.get("strike", 0))
    opt    = data.get("opt_type", "CE").upper()
    expiry = data.get("expiry", "")
    ltp    = float(data.get("ltp", 0))
    broker = acc.get("broker", "").lower().strip()
    log.info(f"[EXECUTE] {action} {opt}{strike} @ {ltp} | broker={broker}")
    handler = BROKER_HANDLERS.get(broker)
    if not handler:
        log.error(f"Unknown broker in payload: '{broker}'")
        return
    try:
        handler(acc, action, strike, opt, ltp, expiry)
    except Exception as err:
        log.error(f"Trade execution error [{broker}]: {err}")
    finally:
        acc.clear()

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

sio_client = socketio.Client(reconnection=True, reconnection_attempts=999, reconnection_delay=3)
_license_key = ""

@sio_client.event
def connect():
    log.info("Connected to Master. Authenticating...")
    sio_client.emit("auth", {"license_key": _license_key, "hwid": "none"})

@sio_client.event
def disconnect():
    log.warning("Disconnected from Master Server. Retrying...")
    print("\n[WARN] Connection lost. Reconnecting...")

@sio_client.on("auth_result")
def on_auth_result(data):
    if data.get("ok"):
        name = data.get("name")
        log.info(f"[OK] Authenticated as: {name}")
        sio_client.auth_name = name
        print("\n[SIGNAL] Connected. Waiting for trade alerts...")
        gui_queue.put(("status", f"Connected as {name}"))
    else:
        log.error(f"[FAIL] Auth rejected: {data.get('reason')}")
        sio_client.disconnect()
        gui_queue.put(("status", "Authentication failed"))

@sio_client.on("trade_alert")
def on_trade_alert(data):
    signal_id     = data.get("signal_id")
    account_names = data.get("account_names", [])
    sio_client.last_alert_time = datetime.datetime.now().strftime("%H:%M:%S")
    log.info(f"[ALERT] Signal={signal_id} | {data.get('action')} "
             f"{data.get('strike')}{data.get('opt_type')} @ {data.get('ltp')}")
    print(f"\n[ALERT] {data.get('action')} {data.get('strike')}{data.get('opt_type')} "
          f"@ {data.get('ltp')} | Signal={signal_id}")
    for acc_name in account_names:
        sio_client.emit("request_execution_token", {
            "signal_id": signal_id,
            "account_name": acc_name
        })

@sio_client.on("execution_token")
def on_execution_token(data):
    if not data.get("ok"):
        log.warning(f"Token denied: {data.get('reason')}")
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
        if TKINTER_AVAILABLE:
            messagebox.showerror("Terms Declined", "You declined the Terms and Conditions. Exiting.")
        else:
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
        if TKINTER_AVAILABLE:
            messagebox.showerror("Error", "No license key entered. Exiting.")
        else:
            print("No license key entered. Exiting.")
        return

    log.info(f"Connecting to {MASTER_URL} ...")
    try:
        sio_client.connect(MASTER_URL, transports=["websocket"])

        if console_mode:
            # Run console menu in the main thread, no daemon threads
            while True:
                print("\nOptions: [1] Update Token  [2] Test Signal  [3] Logs  [4] Status  [5] Exit")
                cmd = input("> ").strip()
                if cmd == "1":
                    new_token = input("Enter new access token: ").strip()
                    if new_token:
                        sio_client.emit("update_token", {"license_key": _license_key, "access_token": new_token})
                elif cmd == "2":
                    sio_client.emit("test_signal", {"license_key": _license_key})
                    print("Test signal sent. Waiting for response...")
                    # Give the socketio client a moment to process the response
                    for _ in range(10):
                        sio_client.sleep(0.1)
                elif cmd == "3":
                    fetch_execution_logs()
                elif cmd == "4":
                    print(f"Connected: {sio_client.connected}")
                    print(f"User: {getattr(sio_client, 'auth_name', 'Unknown')}")
                    print(f"Last alert: {getattr(sio_client, 'last_alert_time', 'Never')}")
                elif cmd == "5":
                    sio_client.disconnect()
                    sys.exit(0)
                # Allow socketio client to process incoming events (like trade alerts)
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
