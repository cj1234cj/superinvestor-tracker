# Concentrated Superinvestor — Small-Cap Tracker

A password-protected dashboard of **concentrated fund managers** (≤ 20 holdings)
and their positions in **sub-$3B market-cap stocks**, flagging any position that
is **≥ 10% of the fund** (★ high conviction).

- **Live page:** served from GitHub Pages (`index.html`) — AES-256-GCM encrypted,
  unlocks in-browser with a password. Nothing is readable without it.
- **Data:** holdings + portfolio weights from [Dataroma](https://dataroma.com)
  (identical 13F data to [Valuesider](https://valuesider.com)); market caps from Yahoo Finance.

## Files
| File | Purpose |
|------|---------|
| `index.html` | Encrypted dashboard served on the web (open with the password). |
| `concentrated_tracker.py` | Scrapes Dataroma + Yahoo, generates `concentrated_tracker.html`. |
| `seal_dashboard.py` | Encrypts that HTML into `index.html` with your password. |

## Refresh workflow
```bash
python concentrated_tracker.py            # regenerate dashboard (monthly cache)
SEAL_PW='your-password' python seal_dashboard.py   # re-encrypt -> index.html
git add index.html && git commit -m "refresh" && git push   # redeploy
```
The raw (unencrypted) `concentrated_tracker.html` and the holdings cache are
**never committed** — only the encrypted `index.html` is published.

*Informational only — not investment advice.*
