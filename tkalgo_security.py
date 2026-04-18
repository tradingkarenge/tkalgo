"""
TK ALGO — Security Module (shared between master & client)
Handles: AES-256 encryption, HWID fingerprinting, timestamp expiration
"""
import base64, json, time, uuid, platform, subprocess, os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# 32-byte shared key — keep this secret, it's baked into the compiled .exe
_KEY = b"TKAlgo!SecretK3y!2024!XYZ!987654"  # exactly 32 bytes

def get_hwid() -> str:
    """Returns a stable hardware fingerprint tied to this specific machine."""
    try:
        if platform.system() == "Windows":
            out = subprocess.check_output(
                "wmic baseboard get serialnumber", shell=True, stderr=subprocess.DEVNULL
            ).decode().strip().split("\n")
            serial = out[-1].strip() if len(out) > 1 else ""
            if serial and serial != "To be filled by O.E.M.":
                return serial
        # Fallback: MAC address as integer
        return str(uuid.getnode())
    except Exception:
        return str(uuid.getnode())

def encrypt_payload(data: dict) -> str:
    """
    Encrypts a dict with AES-256-GCM.
    Injects _ts (timestamp) automatically for expiry checking.
    Returns a base64 string safe to send over WebSocket.
    """
    data["_ts"] = time.time()
    raw = json.dumps(data).encode("utf-8")
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(_KEY), modes.GCM(iv), backend=default_backend())
    enc = cipher.encryptor()
    ct = enc.update(raw) + enc.finalize()
    # Format: [iv 16 bytes][tag 16 bytes][ciphertext]
    blob = iv + enc.tag + ct
    return base64.b64encode(blob).decode("utf-8")

def decrypt_payload(b64: str, max_age: float = 5.0) -> dict:
    """
    Decrypts a base64 AES-256-GCM payload.
    Returns {"error": ...} if expired or tampered.
    max_age: how many seconds old a signal is still valid (default 5s).
    """
    try:
        raw = base64.b64decode(b64)
        iv, tag, ct = raw[:16], raw[16:32], raw[32:]
        cipher = Cipher(algorithms.AES(_KEY), modes.GCM(iv, tag), backend=default_backend())
        dec = cipher.decryptor()
        data = json.loads(dec.update(ct) + dec.finalize())
        age = time.time() - data.get("_ts", 0)
        if age > max_age:
            return {"error": f"Signal expired ({age:.1f}s old)"}
        return data
    except Exception as e:
        return {"error": f"Decryption failed: {e}"}
