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

MASTER_URL  = "http://198.23.237.249:5050"
WEBHOOK_URL = "http://198.23.237.249:5000"
CURRENT_VERSION = "1.0.0"

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

_GROWW_MONTH = {1:"1",2:"2",3:"3",4:"4",5:"5",6:"6",
                7:"7",8:"8",9:"9",10:"O",11:"N",12:"D"}
def build_groww_symbol(strike, opt_type, expiry):
    d = datetime.datetime.strptime(expiry, "%Y-%m-%d")
    mc = _GROWW_MONTH.get(d.month, str(d.month))
    return f"NIFTY{d.year%100}{mc}{d.day:02d}{int(strike)}{opt_type}"

def groww_ref_id():
    return f"TK{str(int(time.time()))[-8:]}"

def place_order_dhan(acc, tx, strike, opt_type, ltp, expiry):
    from dhanhq import dhanhq
    dhan = dhanhq(acc["client_id"], acc["access_token"])
    
    # 1. Try direct lookup from payload or instrument_map
    sid = acc.get("security_id")
    if not sid:
        sid = instrument_map.get(f"{int(strike)}_{expiry}_{opt_type}")
    
    # 2. Fallback: exact normalised match (avoids substring collisions)
    if not sid:
        search_key = f"{int(strike)}_{expiry}_{opt_type.upper()}"
        sid = instrument_map.get(search_key)
    
    # 3. Fallback: try alternate expiry formats in map
    if not sid:
        for key, val in instrument_map.items():
            parts = key.split("_")
            if (len(parts) == 3
                    and parts[0] == str(int(strike))
                    and parts[2] == opt_type.upper()):
                sid = val
                log.info(f"[CLIENT] Dhan fallback key matched: {key}")
                break
    
    # 4. If still not found, log and exit
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
        "market_protection": -1    # <-- ADD THIS LINE
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
    log.info(f"Fyers resp: {resp}")
    return resp

def place_order_groww(acc, tx, strike, opt_type, ltp, expiry):
    sym = build_groww_symbol(strike, opt_type, expiry)
    headers = {"Authorization": f"Bearer {acc['access_token']}",
               "Content-Type": "application/json", "Accept": "application/json",
               "X-API-VERSION": "1.0"}
    payload = {"trading_symbol": sym, "quantity": acc["quantity"], "price": 0,
               "trigger_price": 0, "validity": "DAY", "exchange": "NSE", "segment": "FNO",
               "product": "MIS", "order_type": "MARKET", "transaction_type": tx,
               "order_reference_id": groww_ref_id()}
    resp = requests.post("https://api.groww.in/v1/order/create", json=payload, headers=headers, timeout=10)
    log.info(f"Groww resp: {resp.status_code} {resp.text}")
    return resp.json()

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
    else:
        log.error(f"[FAIL] Auth rejected: {data.get('reason')}")
        sio_client.disconnect()

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
    else:
        print(f"\n[FAIL] {data.get('reason', 'Unknown error')}")

@sio_client.on("test_signal_result")
def on_test_signal_result(data):
    if data.get("ok"):
        print(f"\n[OK] Test signal sent: {data.get('message')}")
    else:
        print(f"\n[FAIL] Test failed: {data.get('reason')}")

def handle_user_input():
    menu = "\nOptions: [1] Update Token  [2] Test Signal  [3] Logs  [4] Status"
    while True:
        print(menu)
        cmd = input("> ").strip()

        if cmd in ("1", "token"):
            new_token = input("Enter new access token: ").strip()
            if not new_token:
                print("No token entered.")
                continue
            sio_client.emit("update_token", {
                "license_key": _license_key,
                "access_token": new_token
            })
            print("[OK] Token sent. Waiting for confirmation...")

        elif cmd in ("2", "test"):
            print("Sending test signal to master...")
            sio_client.emit("test_signal", {"license_key": _license_key})

        elif cmd in ("3", "logs"):
            raw = input("How many logs? [default 20]: ").strip()
            n = int(raw) if raw.isdigit() else 20
            fetch_execution_logs(n)

        elif cmd in ("4", "status"):
            print(f"\n{'─'*40}")
            print(f"  Connected : {sio_client.connected}")
            print(f"  User      : {getattr(sio_client, 'auth_name', 'Unknown')}")
            print(f"  Last alert: {getattr(sio_client, 'last_alert_time', 'Never')}")
            print(f"{'─'*40}")

        else:
            print("Unknown. Enter 1, 2, 3, or 4.")

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

def main():
    global _license_key
    check_for_updates()
    print("\n" + "=" * 50)
    print(f"    TK ALGO -- Member Client App v{CURRENT_VERSION}")
    print("=" * 50)
    _license_key = input("\nEnter your License Key: ").strip()
    if not _license_key:
        print("No license key entered. Exiting.")
        return
    log.info(f"Connecting to {MASTER_URL} ...")
    try:
        sio_client.connect(MASTER_URL, transports=["websocket"])
        threading.Thread(target=handle_user_input, daemon=True).start()
        sio_client.wait()
    except KeyboardInterrupt:
        log.info("Shutting down.")
    except Exception as e:
        log.error(f"Connection failed: {e}")

if __name__ == "__main__":
    main()
