"""
TK ALGO — Member Client App  v1.0
Runs on the MEMBER's local machine (their home Wi-Fi IP).

How it works:
  1. Member enters their License Key once.
  2. App detects hardware ID and registers with Master Server.
  3. When a trade alert arrives, the app requests a one-time execution token.
  4. Master Server validates quota and pushes back the encrypted account credentials.
  5. This app decrypts them, places the trade from the LOCAL machine IP, then clears memory.

Compile to .exe:
    pyinstaller --onefile --console --name TKAlgoClient client_app.py

Install deps:
    pip install python-socketio[client] requests cryptography websocket-client dhanhq kiteconnect smartapi-python upstox-python-sdk fyers-apiv3 neo-api-client
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

from tkalgo_security import decrypt_payload, get_hwid

# ── Config — hardcoded into the binary at compile time ───────────────────────
MASTER_URL = "http://198.23.237.249:5050"
CURRENT_VERSION = "1.0.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("TKAlgoClient")

# ── FYERS helpers ──────────────────────────────────────────────────────────
def fyers_app_id(acc):
    return acc.get("api_key", "").strip()

def fyers_token(acc):
    app_id = fyers_app_id(acc)
    raw = acc.get("access_token", "").strip()
    if not app_id: return raw
    if raw.startswith(app_id + ":"): return raw
    if ":" in raw: raw = raw.split(":", 1)[-1]
    return f"{app_id}:{raw}"

def fyers_model(acc):
    from fyers_apiv3 import fyersModel
    return fyersModel.FyersModel(
        client_id=fyers_app_id(acc),
        token=fyers_token(acc),
        is_async=False,
        log_path=tempfile.gettempdir()
    )

# ── GROWW helpers ──────────────────────────────────────────────────────────
_GROWW_MONTH = {1:"1",2:"2",3:"3",4:"4",5:"5",6:"6",
                7:"7",8:"8",9:"9",10:"O",11:"N",12:"D"}
def build_groww_symbol(strike, opt_type, expiry):
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    mc = _GROWW_MONTH.get(d.month, str(d.month))
    return f"NIFTY{d.year%100}{mc}{d.day:02d}{int(strike)}{opt_type}"

def groww_ref_id():
    return f"TK{str(int(time.time()))[-8:]}"

# ── Broker execution handlers ───────────────────────────────────────────────

def place_order_dhan(acc, tx, strike, opt_type, ltp, expiry):
    from dhanhq import dhanhq
    dhan = dhanhq(acc["client_id"], acc["access_token"])
    # We expect security_id to be passed in the account dict if available, 
    # but more robustly the Master should have resolved it.
    sid = acc.get("security_id")
    if not sid:
        log.error("Dhan: security_id missing in payload")
        return
    resp = dhan.place_order(
        security_id=sid,
        exchange_segment=dhan.NSE_FNO,
        transaction_type=dhan.BUY if tx == "BUY" else dhan.SELL,
        quantity=acc["quantity"],
        order_type=dhan.MARKET,
        product_type=dhan.INTRA,
        price=0,
    )
    log.info(f"Dhan resp: {resp}")
    return resp

def place_order_zerodha(acc, tx, strike, opt_type, ltp, expiry):
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=acc["api_key"])
    kite.set_access_token(acc["access_token"])
    # sym lookup is handled on server or can be rebuilt here
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    sym = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    # Note: sym might need adjustment based on Zerodha's specific mapping
    # For now using generic format; if Master sends 'tradingsymbol' it's better
    tradingsymbol = acc.get("tradingsymbol") or sym
    resp = kite.place_order(
        variety="regular",
        exchange="NFO",
        tradingsymbol=tradingsymbol,
        transaction_type=tx,
        quantity=acc["quantity"],
        order_type="MARKET",
        product="MIS",
    )
    log.info(f"Zerodha resp: {resp}")
    return resp

def place_order_angel(acc, tx, strike, opt_type, ltp, expiry):
    from SmartApi import SmartConnect
    smart = SmartConnect(api_key=acc["api_key"])
    smart.access_token = acc["access_token"]
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    symbol = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    token = acc.get("symbol_token")
    if not token:
        log.error("Angel: symbol_token missing in payload")
        return
    resp = smart.placeOrder({
        "variety": "NORMAL",
        "tradingsymbol": symbol,
        "symboltoken": token,
        "transactiontype": tx,
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": "0",
        "quantity": str(acc["quantity"]),
    })
    log.info(f"Angel resp: {resp}")
    return resp

def place_order_upstox(acc, tx, strike, opt_type, ltp, expiry):
    import upstox_client
    inst_key = acc.get("instrument_token")
    if not inst_key:
        log.error("Upstox: instrument_token missing in payload")
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
    sym = f"NSE:NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    data = {
        "symbol": sym, "qty": acc["quantity"], "type": 2,
        "side": 1 if tx == "BUY" else -1, "productType": "INTRADAY",
        "limitPrice": 0, "stopPrice": 0, "validity": "DAY",
        "disclosedQty": 0, "offlineOrder": False
    }
    resp = fy.place_order(data=data)
    log.info(f"Fyers resp: {resp}")
    return resp

def place_order_groww(acc, tx, strike, opt_type, ltp, expiry):
    sym = build_groww_symbol(strike, opt_type, expiry)
    headers = {"Authorization": f"Bearer {acc['access_token']}", "Content-Type": "application/json",
               "Accept": "application/json", "X-API-VERSION": "1.0"}
    payload = {"trading_symbol": sym, "quantity": acc["quantity"], "price": 0,
               "trigger_price": 0, "validity": "DAY", "exchange": "NSE", "segment": "FNO",
               "product": "MIS", "order_type": "MARKET",
               "transaction_type": tx,
               "order_reference_id": groww_ref_id()}
    resp = requests.post("https://api.groww.in/v1/order/create", json=payload, headers=headers, timeout=10)
    log.info(f"Groww resp: {resp.status_code} {resp.text}")
    return resp.json()

def place_order_kotak(acc, tx, strike, opt_type, ltp, expiry):
    from neo_api_client import NeoAPI
    client = NeoAPI(consumer_key=acc.get("api_key", ""),
                    environment="prod",
                    access_token=acc.get("access_token", ""),
                    neo_fin_key="neotradeapi")
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
    # Payload must include symbol_id (token) and tsym
    token = acc.get("symbol_token")
    tsym = acc.get("tradingsymbol")
    if not token or not tsym:
        log.error("AliceBlue: token/tsym missing in payload")
        return
    
    def _alice_susertoken(session_id):
        import hashlib
        h1 = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
        return hashlib.sha256(h1.encode("utf-8")).hexdigest()

    uid = acc["client_id"]
    tok = _alice_susertoken(acc["access_token"])
    headers = {
        "Authorization": f"Bearer {uid} {tok}",
        "Content-Type": "application/json"
    }
    order_body = [{
        "complexty": "regular", "discqty": "0", "exch": "NFO", "pCode": "MIS",
        "prctyp": "MKT", "price": "0", "qty": str(acc["quantity"]), "ret": "DAY",
        "symbol_id": token, "trading_symbol": tsym,
        "transtype": tx, "trigPrice": "0", "orderTag": "TKALGO"
    }]
    resp = requests.post("https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/placeOrder/executePlaceOrder", 
                         json=order_body, headers=headers, timeout=10)
    log.info(f"AliceBlue resp: {resp.status_code} {resp.text}")
    return resp.json()

def place_order_flattrade(acc, tx, strike, opt_type, ltp, expiry):
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    tsym = f"NIFTY{d.strftime('%d%b%y').upper()}{int(strike)}{opt_type}"
    uid = acc["client_id"]
    payload = {
        "uid": uid, "actid": uid, "exch": "NFO", "tsym": tsym,
        "qty": str(acc["quantity"]), "prc": "0", "prd": "I",
        "trantype": "B" if tx == "BUY" else "S", "prctyp": "MKT", "ret": "DAY"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post("https://piconnect.flattrade.in/NorenWClientTP/PlaceOrder", 
                         data={"jData": json.dumps(payload)}, headers=headers, timeout=10)
    log.info(f"FlatTrade resp: {resp.status_code} {resp.text}")
    return resp.json()

def place_order_iifl(acc, tx, strike, opt_type, ltp, expiry):
    headers = {"Authorization": f"Bearer {acc['access_token']}", "Content-Type": "application/json"}
    inst_id = acc.get("instrument_id")
    if not inst_id:
        log.error("IIFL: instrument_id missing in payload")
        return
    
    # Robust price calculation
    limit_price = 0.0
    if ltp > 0:
        limit_price = round(ltp * 1.05, 2) if tx == "BUY" else round(ltp * 0.90, 2)
    else:
        # Fallback if LTP is not available
        limit_price = 9999.0 if tx == "BUY" else 1.05

    payload = [{
        "exchange": "NSEFO", "instrumentId": str(inst_id),
        "transactionType": tx, "quantity": acc["quantity"],
        "product": "INTRADAY", "orderComplexity": "REGULAR",
        "orderType": "LIMIT", "validity": "DAY", "price": str(limit_price)
    }]
    r = requests.post("https://api.iiflcapital.com/v1/orders", json=payload, headers=headers, timeout=10)
    log.info(f"IIFL resp: {r.status_code} {r.text}")
    return r.json()

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

def execute_trade(encrypted_payload: str):
    """Full execution pipeline from decryption → broker order."""
    data = decrypt_payload(encrypted_payload, max_age=10.0) # increased age to 10s for slow networks
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

    handler = BROKER_HANDLERS.get(broker)
    if not handler:
        log.error(f"Unknown broker in payload: '{broker}'")
        return

    try:
        handler(acc, action, strike, opt, ltp, expiry)
    except Exception as err:
        log.error(f"Trade execution error [{broker}]: {err}")
    finally:
        # Immediately wipe sensitive token from memory
        acc.clear()

# ── Socket.IO Client ──────────────────────────────────────────────────────────

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

@sio_client.on("auth_result")
def on_auth_result(data):
    if data.get("ok"):
        log.info(f"✓ Authenticated as: {data.get('name')}")
    else:
        log.error(f"✗ Auth rejected: {data.get('reason')}")
        sio_client.disconnect()

@sio_client.on("trade_alert")
def on_trade_alert(data):
    signal_id     = data.get("signal_id")
    account_names = data.get("account_names", [])
    log.info(f"Trade alert received! Signal={signal_id} | {data.get('action')} {data.get('strike')}{data.get('opt_type')}")
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

# ── Entry Point ───────────────────────────────────────────────────────────────

def check_for_updates():
    """Checks the Master Server for the latest official version."""
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
                log.warning(f"Your version (v{CURRENT_VERSION}) is outdated. Please update to v{latest}.")
                return True
    except Exception as e:
        log.debug(f"Update check failed: {e}")
    return False

def main():
    global _license_key

    # Early update check
    check_for_updates()

    print("\n" + "=" * 50)
    print(f"    TK ALGO — Member Client App v{CURRENT_VERSION}")
    print("=" * 50)
    _license_key = input("\nEnter your License Key: ").strip()
    if not _license_key:
        print("No license key entered. Exiting.")
        return

    log.info(f"Connecting to {MASTER_URL} ...")
    try:
        sio_client.connect(MASTER_URL, transports=["websocket"])
        sio_client.wait()
    except KeyboardInterrupt:
        log.info("Shutting down.")
    except Exception as e:
        log.error(f"Connection failed: {e}")

if __name__ == "__main__":
    main()
