#!/usr/bin/env python3
"""
Encrypt concentrated_tracker.html into a password-protected index.html.

AES-256-GCM with a PBKDF2-HMAC-SHA256 derived key. The output is a single
self-contained HTML file: it shows a password prompt and decrypts the real
dashboard in the browser using the Web Crypto API. Nothing is readable
without the password (the content is genuinely encrypted, not just hidden).

Usage:
    SEAL_PW='your-password' python seal_dashboard.py
"""
import base64
import hashlib
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "concentrated_tracker.html")
OUT = os.path.join(HERE, "index.html")
ITERATIONS = 250_000


def main():
    pw = os.environ.get("SEAL_PW")
    if not pw:
        raise SystemExit("Set SEAL_PW env var to the password.")

    with open(SRC, encoding="utf-8") as f:
        plaintext = f.read().encode("utf-8")

    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt, ITERATIONS, dklen=32)
    ct = AESGCM(key).encrypt(nonce, plaintext, None)  # ciphertext || 16-byte tag

    b64 = lambda b: base64.b64encode(b).decode("ascii")
    payload = {
        "salt": b64(salt),
        "nonce": b64(nonce),
        "ct": b64(ct),
        "iter": ITERATIONS,
    }

    page = TEMPLATE.replace("__SALT__", payload["salt"]) \
                   .replace("__NONCE__", payload["nonce"]) \
                   .replace("__CT__", payload["ct"]) \
                   .replace("__ITER__", str(ITERATIONS))

    with open(OUT, "w", encoding="utf-8") as f:
        f.write(page)

    # sanity: round-trip decrypt in Python
    key2 = hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt, ITERATIONS, dklen=32)
    assert AESGCM(key2).decrypt(nonce, ct, None) == plaintext
    print(f"OK  wrote {OUT}  ({len(ct)} bytes ciphertext, {ITERATIONS} PBKDF2 iters)")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Protected — Superinvestor Tracker</title>
<style>
  :root { --bg:#f6f8fa; --card:#fff; --border:#d0d7de; --text:#1f2328; --muted:#656d76; --blue:#0969da; --red:#cf222e; }
  * { box-sizing:border-box; }
  html,body { margin:0; height:100%; }
  body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:var(--bg); color:var(--text); display:flex; align-items:center; justify-content:center; }
  .gate { background:var(--card); border:1px solid var(--border); border-radius:12px;
    padding:34px 30px; width:340px; box-shadow:0 6px 24px rgba(0,0,0,.06); text-align:center; }
  .gate h1 { font-size:17px; margin:0 0 4px; color:var(--blue); }
  .gate p { font-size:12px; color:var(--muted); margin:0 0 20px; }
  .gate input { width:100%; padding:10px 12px; border:1px solid var(--border); border-radius:8px;
    font-size:14px; outline:none; }
  .gate input:focus { border-color:var(--blue); }
  .gate button { width:100%; margin-top:12px; padding:10px; border:0; border-radius:8px;
    background:var(--blue); color:#fff; font-size:14px; font-weight:600; cursor:pointer; }
  .gate button:hover { background:#0860ca; }
  .err { color:var(--red); font-size:12px; margin-top:12px; min-height:16px; }
  .lock { font-size:26px; margin-bottom:8px; }
</style></head><body>
  <div class="gate" id="gate">
    <div class="lock">🔒</div>
    <h1>Superinvestor Tracker</h1>
    <p>Enter password to view.</p>
    <input id="pw" type="password" autofocus autocomplete="current-password" placeholder="Password" />
    <button id="go">Unlock</button>
    <div class="err" id="err"></div>
  </div>
<script>
const DATA = { salt:"__SALT__", nonce:"__NONCE__", ct:"__CT__", iter:__ITER__ };
const KEY = "sv_pw";
const b64 = s => Uint8Array.from(atob(s), c => c.charCodeAt(0));
async function decryptWith(pw) {
  const km = await crypto.subtle.importKey("raw", new TextEncoder().encode(pw), "PBKDF2", false, ["deriveKey"]);
  const key = await crypto.subtle.deriveKey(
    { name:"PBKDF2", salt:b64(DATA.salt), iterations:DATA.iter, hash:"SHA-256" },
    km, { name:"AES-GCM", length:256 }, false, ["decrypt"]);
  const pt = await crypto.subtle.decrypt({ name:"AES-GCM", iv:b64(DATA.nonce) }, key, b64(DATA.ct));
  return new TextDecoder().decode(pt);
}
async function unlock(pw, fromStore) {
  const err = document.getElementById("err");
  if (err) err.textContent = "";
  pw = (pw != null) ? pw : document.getElementById("pw").value;
  try {
    const html = await decryptWith(pw);
    try { sessionStorage.setItem(KEY, pw); } catch (e) {}   // per-tab; cleared on tab close
    document.open(); document.write(html); document.close();
  } catch (e) {
    if (fromStore) { try { sessionStorage.removeItem(KEY); } catch (_) {} }
    else if (err) err.textContent = "Wrong password.";
  }
}
function boot() {
  document.getElementById("go").addEventListener("click", () => unlock());
  document.getElementById("pw").addEventListener("keydown", e => { if (e.key === "Enter") unlock(); });
  let saved = null; try { saved = sessionStorage.getItem(KEY); } catch (e) {}
  if (saved) unlock(saved, true);   // auto-unlock after a Refresh reload
}
if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
else boot();   // script is at end of <body>; DOMContentLoaded may have already fired
</script>
</body></html>"""


if __name__ == "__main__":
    main()
