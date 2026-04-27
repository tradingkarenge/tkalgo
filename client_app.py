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

# Try to import tkinter; if it fails, we fall back to console mode
try:
    import tkinter as tk
    from tkinter import scrolledtext, simpledialog, messagebox
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

from tkalgo_security import decrypt_payload, get_hwid

MASTER_URL  = "http://198.23.237.249:5050"
WEBHOOK_URL = "http://198.23.237.249:5000"
CURRENT_VERSION = "1.0.0"

# ========== TERMS AND CONDITIONS ==========
TERMS_AND_CONDITIONS = """... (same as before) ..."""

ACCEPTANCE_FILE = "tk_algo_acceptance.json"

def save_acceptance():
    data = {"accepted": True, "version": "1.0", "timestamp": datetime.datetime.now().isoformat()}
    try:
        with open(ACCEPTANCE_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logging.warning(f"Could not save acceptance: {e}")

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
    """Display T&C popup (main thread). If no Tkinter, use console."""
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
    # Tkinter version
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

# ========== GUI MENU (runs on main thread) ==========
gui_queue = queue.Queue()
gui_root = None
status_label = None

def update_gui_from_queue():
    """Process queued updates in the main thread."""
    while not gui_queue.empty():
        msg = gui_queue.get_nowait()
        if msg[0] == "status":
            if status_label:
                status_label.config(text=msg[1])
        elif msg[0] == "alert":
            # Could add a popup or update a text area
            pass
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

# ========== EXISTING BROKER FUNCTIONS (unchanged) ==========
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

_GROWW_MONTHS_3 = [
    "JAN","FEB","MAR","APR","MAY","JUN",
    "JUL","AUG","SEP","OCT","NOV","DEC"
]

def build_groww_symbol(strike, opt_type, expiry_str=None):
    """Fallback symbol builder using correct Groww format: NSE-NIFTY-{DDMMMYY}-{STRIKE}-{TYPE}"""
    try:
        date_str = expiry_str or EXPIRY_DATE
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        dd = f"{d.day:02d}"
        mmm = _GROWW_MONTHS_3[d.month - 1]
        yy = str(d.year)[-2:]
        st = int(strike)
        sym = f"NSE-NIFTY-{dd}{mmm}{yy}-{st}-{opt_type.upper()}"
        return sym
    except Exception as e:
        log.error(f"[GROWW] fallback symbol error: {e}")
        return None

def groww_ref_id():
    """Unique reference ID for each Groww order."""
    return f"TK{int(time.time() * 1000) % 10_000_000_000:010d}"

def _groww_headers(acc):
    return {
        "Authorization": f"Bearer {acc['access_token'].strip()}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-API-VERSION": "1.0",
    }



def place_order_dhan(acc, tx, strike, opt_type, ltp, expiry):
    from dhanhq import dhanhq
    dhan = dhanhq(acc["client_id"], acc["access_token"])
    sid = acc.get("security_id")
    if not sid:
        log.error(f"Dhan: security_id missing for {strike} {opt_type} {expiry}")
        add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", "FAILED", "security_id missing")
        return
    resp = dhan.place_order(
        security_id=str(sid),
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY if tx == "BUY" else dhan.SELL,
        quantity=acc["quantity"],
        order_type=dhan.MARKET,
        product_type=dhan.INTRA,
        price=0,
    )
    log.info(f"Dhan resp: {resp}")
    status = "OK" if isinstance(resp, dict) and resp.get("status") == "success" else "FAILED"
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", status, str(resp)[:200])
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
    log.info(f"Zerodha resp: {r.status_code} - {r.text}")

def place_order_angel(acc, tx, strike, opt_type, ltp, expiry):
    from SmartApi import SmartConnect
    smart = SmartConnect(api_key=acc["api_key"])
    smart.access_token = acc["access_token"]
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    symbol = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    token = acc.get("symbol_token")
    if not token:
        log.error("Angel: symbol_token missing")
        return
    resp = smart.placeOrder({
        "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
        "transactiontype": tx, "exchange": "NFO", "ordertype": "MARKET",
        "producttype": "INTRADAY", "duration": "DAY",
        "price": "0", "quantity": str(acc["quantity"]),
    })
    log.info(f"Angel resp: {resp}")
    return resp

def place_order_upstox(acc, tx, strike, opt_type, ltp, expiry):
    import upstox_client
    inst_key = acc.get("instrument_token")
    if not inst_key:
        log.error("Upstox: instrument_token missing")
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
    log.info(f"Upstox resp: {resp}")
    return resp

def place_order_fyers(acc, tx, strike, opt_type, ltp, expiry):
    fy = fyers_model(acc)
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    sym = f"NSE:NIFTY{d.strftime('%y')}{d.month}{d.strftime('%d')}{int(strike)}{opt_type}"
    data = {
        "symbol": sym, "qty": acc["quantity"], "type": 2,
        "side": 1 if tx == "BUY" else -1, "productType": "INTRADAY",
        "limitPrice": 0, "stopPrice": 0, "validity": "DAY",
        "disclosedQty": 0, "offlineOrder": False
    }
    resp = fy.place_order(data=data)
    status = "OK" if isinstance(resp, dict) and resp.get("s") == "ok" else "FAILED"
    add_log(acc.get("name", ""), f"{tx} {opt_type}{strike}", status, str(resp)[:200])
    return resp

def place_order_groww(acc, tx, strike, opt_type, ltp, expiry):
    name = acc.get("name", "unknown")
    log.info(f"[GROWW] {name} | {tx} {opt_type}{strike} | expiry={expiry}")

    token = acc.get("access_token", "").strip()
    if not token:
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", "No Groww access_token")
        log.error(f"[GROWW] {name}: access_token is empty")
        return {}

    # 1. Try to get trading_symbol from instrument map
    key = f"NIFTY_{expiry}_{int(strike)}_{opt_type.upper()}"
    sym = groww_instrument_map.get(key)
    if not sym:
        # 2. Fallback to manual builder
        sym = build_groww_symbol(strike, opt_type, expiry)
        if not sym:
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED",
                    f"Cannot build symbol: strike={strike} expiry={expiry}")
            return {}
        log.warning(f"[GROWW] {name}: Using fallback symbol {sym} (not found in instrument map)")

    log.info(f"[GROWW] {name} | Symbol={sym} | Qty={acc['quantity']}")

    try:
        exp_date = datetime.datetime.strptime(expiry, "%Y-%m-%d").date()
        weekday = exp_date.weekday()
        if weekday not in (1, 3):
            log.warning(f"[GROWW] Expiry {expiry} is {exp_date.strftime('%A')} - not standard NSE expiry day")
    except Exception:
        pass

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
        "order_reference_id": groww_ref_id(),
    }

    try:
        resp = requests.post(
            "https://api.groww.in/v1/order/create",
            json=payload,
            headers=_groww_headers(acc),
            timeout=12,
        )
        raw = resp.text[:500]
        log.info(f"[GROWW] {name} | HTTP {resp.status_code} | {raw}")

        rj = {}
        try:
            rj = resp.json()
        except Exception:
            pass

        if resp.status_code == 200:
            order_id = ""
            try:
                order_id = (rj.get("payload") or {}).get("orderId", "")
            except Exception:
                pass
            add_log(name, f"{tx} {opt_type}{strike}", "OK", f"sym={sym} orderId={order_id}")
            return rj

        elif resp.status_code == 400:
            err_obj = rj.get("error") or {} if isinstance(rj, dict) else {}
            err_code = err_obj.get("code", "?") if isinstance(err_obj, dict) else "?"
            err_msg = err_obj.get("message", raw) if isinstance(err_obj, dict) else raw
            hint = ""
            if err_code == "GA001":
                hint = f"Invalid symbol '{sym}'. Check expiry date ({expiry}) and strike {int(strike)}."
            elif err_code == "GA002":
                hint = f"Insufficient funds for {acc['quantity']} lots."
            elif err_code == "GA003":
                hint = "Market is closed or outside trading hours."
            else:
                hint = err_msg
            log.error(f"[GROWW] {name}: 400 {err_code} | {hint}")
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", f"400 {err_code}: {hint[:200]}")
            return rj

        elif resp.status_code == 401:
            log.error(f"[GROWW] {name}: 401 Unauthorized - token expired")
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", "Token expired (401)")
            return {}

        elif resp.status_code == 429:
            log.warning(f"[GROWW] {name}: 429 Rate limited. Retrying after 2s...")
            time.sleep(2)
            resp2 = requests.post(
                "https://api.groww.in/v1/order/create",
                json=payload,
                headers=_groww_headers(acc),
                timeout=12,
            )
            if resp2.status_code == 200:
                rj2 = resp2.json() if resp2.text else {}
                add_log(name, f"{tx} {opt_type}{strike}", "OK", f"sym={sym} (retry OK)")
                return rj2
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", f"Rate limited; retry HTTP {resp2.status_code}")
            return {}

        else:
            log.error(f"[GROWW] {name}: HTTP {resp.status_code} | {raw}")
            add_log(name, f"{tx} {opt_type}{strike}", "FAILED", f"HTTP {resp.status_code}: {raw[:150]}")
            return rj

    except requests.Timeout:
        log.error(f"[GROWW] {name}: request timeout (12s)")
        add_log(name, f"{tx} {opt_type}{strike}", "TIMEOUT", "Groww API did not respond within 12s")
        return {}
    except Exception as e:
        log.error(f"[GROWW] {name}: exception: {e}")
        add_log(name, f"{tx} {opt_type}{strike}", "FAILED", str(e)[:200])
        return {}

def place_order_kotak(acc, tx, strike, opt_type, ltp, expiry):
    from neo_api_client import NeoAPI
    client = NeoAPI(consumer_key=acc.get("api_key", ""), environment="prod",
                    access_token=acc.get("access_token", ""), neo_fin_key="neotradeapi")
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    sym = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    resp = client.place_order(exchange_segment="nse_fo", product="MIS", price="0",
                              order_type="MKT", quantity=str(acc["quantity"]),
                              validity="DAY", trading_symbol=sym,
                              transaction_type="B" if tx == "BUY" else "S",
                              amo="NO", disclosed_quantity="0",
                              market_protection="0", pf="N", trigger_price="0", tag="TKALGO")
    log.info(f"Kotak resp: {resp}")
    return resp

def place_order_aliceblue(acc, tx, strike, opt_type, ltp, expiry):
    import hashlib
    token = acc.get("symbol_token")
    tsym  = acc.get("tradingsymbol")
    if not token or not tsym:
        log.error("AliceBlue: token/tsym missing")
        return
    h1  = hashlib.sha256(acc["access_token"].encode()).hexdigest()
    tok = hashlib.sha256(h1.encode()).hexdigest()
    headers = {"Authorization": f"Bearer {acc['client_id']} {tok}",
               "Content-Type": "application/json"}
    order_body = [{"complexty": "regular", "discqty": "0", "exch": "NFO", "pCode": "MIS",
                   "prctyp": "MKT", "price": "0", "qty": str(acc["quantity"]), "ret": "DAY",
                   "symbol_id": token, "trading_symbol": tsym,
                   "transtype": tx, "trigPrice": "0", "orderTag": "TKALGO"}]
    resp = requests.post(
        "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/executePlaceOrder",
        json=order_body, headers=headers, timeout=10)
    log.info(f"AliceBlue resp: {resp.status_code} {resp.text}")
    return resp.json()

def place_order_flattrade(acc, tx, strike, opt_type, ltp, expiry):
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    tsym = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    uid  = acc["client_id"]
    payload = {"uid": uid, "actid": uid, "exch": "NFO", "tsym": tsym,
               "qty": str(acc["quantity"]), "prc": "0", "prd": "I",
               "trantype": "B" if tx == "BUY" else "S", "prctyp": "MKT", "ret": "DAY"}
    resp = requests.post("https://piconnect.flattrade.in/NorenWClientTP/PlaceOrder",
                         data={"jData": json.dumps(payload)},
                         headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=10)
    log.info(f"FlatTrade resp: {resp.status_code} {resp.text}")
    return resp.json()

def place_order_iifl(acc, tx, strike, opt_type, ltp, expiry):
    inst_id = acc.get("instrument_id")
    if not inst_id:
        log.error("IIFL: instrument_id missing")
        return
    limit_price = (round(ltp * 1.05, 2) if tx == "BUY" and ltp > 0 else
                   round(ltp * 0.90, 2) if ltp > 0 else
                   9999.0 if tx == "BUY" else 1.05)
    payload = [{"exchange": "NSEFO", "instrumentId": str(inst_id),
                "transactionType": tx, "quantity": acc["quantity"],
                "product": "INTRADAY", "orderComplexity": "REGULAR",
                "orderType": "LIMIT", "validity": "DAY", "price": str(limit_price)}]
    r = requests.post("https://api.iiflcapital.com/v1/orders", json=payload,
                      headers={"Authorization": f"Bearer {acc['access_token']}",
                               "Content-Type": "application/json"}, timeout=10)
    log.info(f"IIFL resp: {r.status_code} {r.text}")
    return r.json()

BROKER_HANDLERS = {
    "dhan": place_order_dhan, "zerodha": place_order_zerodha,
    "angel": place_order_angel, "upstox": place_order_upstox,
    "fyers": place_order_fyers, "groww": place_order_groww,
    "kotak": place_order_kotak, "aliceblue": place_order_aliceblue,
    "flattrade": place_order_flattrade, "iifl": place_order_iifl,
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
    hwid = get_hwid()
    log.info(f"Connected to Master. HWID={hwid}... Authenticating...")
    sio_client.emit("auth", {"license_key": _license_key, "hwid": hwid})

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
    # Show Terms & Conditions (main thread)
    if not show_terms_and_conditions():
        if TKINTER_AVAILABLE:
            messagebox.showerror("Terms Declined", "You declined the Terms and Conditions. Exiting.")
        else:
            print("You declined the Terms and Conditions. Exiting.")
        return

    # Create the GUI menu (if Tkinter available) – runs on main thread
    if TKINTER_AVAILABLE:
        gui = create_gui_menu()
        if gui is None:
            print("Warning: Could not create GUI, using console mode.")
            console_mode = True
        else:
            console_mode = False
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
            # Fallback to console menu if no GUI
            def console_menu():
                while True:
                    print("\nOptions: [1] Update Token  [2] Test Signal  [3] Logs  [4] Status  [5] Exit")
                    cmd = input("> ").strip()
                    if cmd == "1":
                        new_token = input("Enter new access token: ").strip()
                        if new_token:
                            sio_client.emit("update_token", {"license_key": _license_key, "access_token": new_token})
                    elif cmd == "2":
                        sio_client.emit("test_signal", {"license_key": _license_key})
                    elif cmd == "3":
                        fetch_execution_logs()
                    elif cmd == "4":
                        print(f"Connected: {sio_client.connected}")
                        print(f"User: {getattr(sio_client, 'auth_name', 'Unknown')}")
                        print(f"Last alert: {getattr(sio_client, 'last_alert_time', 'Never')}")
                    elif cmd == "5":
                        sio_client.disconnect()
                        sys.exit(0)
            threading.Thread(target=console_menu, daemon=True).start()
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
