#!/usr/bin/env python3
"""
Concentrated Superinvestor Small-Cap Tracker
=============================================
Tracks superinvestors (Dataroma + Valuesider draw from the SAME 13F filings, so
Dataroma is used as the authoritative source and every row links to both sites).

Screens for:
  - Fund managers holding <= 20 stocks  (concentrated portfolios)
  - Positions in stocks under $3B market cap
  - Highlights positions that are a BIG slice of the fund (>= 10% of portfolio)

Output:  concentrated_tracker.html   (auto-opens in browser)
Cache:   concentrated_holdings.json  (re-scraped monthly)
"""

import time
import random
import json
import os
import re
import threading
import webbrowser
import pathlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from curl_cffi import requests as crequests

# This machine sits behind an SSL-intercepting proxy; public read-only data.
VERIFY_SSL = False

BASE_URL = "https://www.dataroma.com"
MAX_HOLDINGS = 20          # only managers with <= this many stocks
MCAP_CEILING = 3_000_000_000   # only stocks under $3B market cap
BIG_POSITION = 10.0        # flag positions >= this % of portfolio
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "concentrated_holdings.json")
VS_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "valuesider_holdings.json")
CACHE_DAYS = 30

# ── Superinvestors (Dataroma manager id -> display name) ──────────────
INVESTORS = {
    "AKO": "AKO Capital", "AIM": "Alex Roepers - Atlantic Investment",
    "AP": "AltaRock Partners", "GFT": "Bill & Melinda Gates Foundation",
    "psc": "Bill Ackman - Pershing Square", "LMM": "Bill Miller - Miller Value Partners",
    "HA": "Bill Nygren - Oakmark Funds", "fairx": "Bruce Berkowitz - Fairholme",
    "OCL": "Bryan Lawrence - Oakcliff Capital", "ic": "Carl Icahn",
    "TGM": "Chase Coleman - Tiger Global", "tci": "Chris Hohn - TCI Fund",
    "SA": "Christopher Bloomstran - Semper Augustus", "DAV": "Christopher Davis - Davis Advisors",
    "AC": "Chuck Akre - Akre Capital", "CAS": "Clifford Sosin - CAS Investment",
    "tp": "Daniel Loeb - Third Point", "abc": "David Abrams - Abrams Capital",
    "GLRE": "David Einhorn - Greenlight Capital", "MAVFX": "David Katz - Matrix Asset Advisors",
    "WP": "David Rolfe - Wedgewood Partners", "AM": "David Tepper - Appaloosa",
    "SP": "Dennis Hong - ShawSpring Partners", "DAC": "Dodge & Cox Funds",
    "HH": "Duan Yongping - H&H International", "FE": "First Eagle Investment",
    "FPA": "First Pacific Advisors", "ca": "Francis Chou - Chou Associates",
    "GC": "Francois Rochon - Giverny Capital", "CCM": "Glenn Greenberg - Brave Warrior",
    "ENG": "Glenn Welling - Engaged Capital", "GA": "Greenhaven Associates",
    "CM": "Greg Alexander - Conifer Management", "aq": "Guy Spier - Aquamarine Capital",
    "SSHFX": "Harry Burn - Sound Shore", "DCP": "Henry Ellenbogen - Durable Capital",
    "HCM": "Hillman Capital Management", "oc": "Howard Marks - Oaktree Capital",
    "JIM": "Jensen Investment Management", "EC": "John Armitage - Egerton Capital",
    "AI": "John Rogers - Ariel Investments", "GLC": "Josh Tarasoff - Greenlea Lane",
    "KB": "Kahn Brothers Group", "mc": "Lee Ainslie - Maverick Capital",
    "HC": "Li Lu - Himalaya Capital", "LT": "Lindsell Train",
    "MPF": "Mairs & Power Funds", "SE": "Mason Hawkins - Southeastern Asset",
    "SAM": "Michael Burry - Scion Asset", "PI": "Mohnish Pabrai - Pabrai Investments",
    "TF": "Nelson Peltz - Trian Fund", "PC": "Norbert Lou - Punch Card",
    "DA": "Pat Dorsey - Dorsey Asset", "pcm": "Polen Capital Management",
    "FFH": "Prem Watsa - Fairfax Financial", "PIM": "Richard Pzena - Pzena Investment",
    "OFALX": "Robert Olstein - Olstein Capital", "RVC": "Robert Vinall - RV Capital",
    "RC": "Ruane Cunniff LP", "PTNT": "Samantha McLemore - Patient Capital",
    "CAU": "Sarah Ketterer - Causeway Capital", "BAUPOST": "Seth Klarman - Baupost Group",
    "LPC": "Stephen Mandel - Lone Pine Capital", "FS": "Terry Smith - Fundsmith",
    "TA": "Third Avenue Management", "MKL": "Thomas Gayner - Markel Group",
    "GR": "Thomas Russo - Gardner Russo & Quinn", "MP": "Tom Bancroft - Makaira Partners",
    "T": "Torray Funds", "TFP": "Triple Frond Partners",
    "TB": "Tweedy Browne", "VFC": "Valley Forge Capital Management",
    "VA": "ValueAct Capital", "vg": "Viking Global Investors",
    "VVP": "Vulcan Value Partners", "WIM": "Wallace Weitz - Weitz Investment",
    "BRK": "Warren Buffett - Berkshire Hathaway", "cc": "William Von Mueffling - Cantillon",
    "YAM": "Yacktman Asset Management",
}

# ── Yahoo Finance direct API (crumb based) — market cap only ──────────
_yf = crequests.Session(impersonate="chrome136", verify=VERIFY_SSL)
_crumb = None
_crumb_lock = threading.Lock()


def _get_crumb():
    global _crumb
    with _crumb_lock:
        if _crumb:
            return _crumb
        _yf.get("https://finance.yahoo.com/", timeout=15)
        r = _yf.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=15)
        _crumb = r.text.strip()
        return _crumb


def _yahoo_mcap(sym):
    crumb = _get_crumb()
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
    params = {"modules": "price", "crumb": crumb}
    r = _yf.get(url, params=params, timeout=15)
    if r.status_code == 401:
        global _crumb
        with _crumb_lock:
            _crumb = None
        params["crumb"] = _get_crumb()
        r = _yf.get(url, params=params, timeout=15)
    if r.status_code != 200:
        return None
    res = r.json().get("quoteSummary", {}).get("result")
    if not res:
        return None
    return ((res[0].get("price", {}).get("marketCap")) or {}).get("raw")


def fetch_market_cap(ticker):
    """Return market cap (float) or None. Retries with a Yahoo-normalized
    symbol (dot->dash, drop -OLD) so dot-class small caps aren't dropped."""
    try:
        time.sleep(random.uniform(0.15, 0.4))
        mc = _yahoo_mcap(ticker)
        if not mc:
            alt = ticker.replace(".", "-").replace("-OLD", "")
            if alt != ticker:
                mc = _yahoo_mcap(alt)
        return mc
    except Exception:
        return None


# ── Dataroma scraping ─────────────────────────────────────────────────
_dr = crequests.Session(impersonate="chrome136", verify=VERIFY_SSL)


def scrape_manager_list():
    """Scrape Dataroma's full current roster -> {mgr_id: name}. Falls back to
    the hardcoded INVESTORS dict if the page can't be fetched/parsed."""
    try:
        html = _dr.get(f"{BASE_URL}/m/managers.php", timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        managers = {}
        for a in soup.select('a[href*="holdings.php?m="]'):
            href = a.get("href", "")
            mid = href.split("m=")[-1].split("&")[0].strip()
            name = a.get_text(strip=True)
            if mid and name and mid not in managers:
                managers[mid] = name
        if len(managers) >= len(INVESTORS) * 0.8:   # sanity check
            return managers
    except Exception as e:
        print(f"  ! manager-list scrape failed ({e}); using built-in list.")
    return dict(INVESTORS)


def scrape_manager(mgr_id, mgr_name):
    """Return list of positions: [{ticker, company, pct}] for a manager."""
    url = f"{BASE_URL}/m/holdings.php?m={mgr_id}"
    try:
        html = _dr.get(url, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="grid") or soup.find("table")
        if not table:
            return []
        positions = []
        for row in table.find_all("tr")[1:]:
            tds = row.find_all("td")
            if len(tds) < 3:
                continue
            # ticker + company from the stock cell (2nd data col)
            sym, company = None, ""
            for a in row.find_all("a"):
                if "stock.php?sym=" in a.get("href", ""):
                    sym = a["href"].split("sym=")[-1].strip().upper()
                    break
            stock_text = tds[1].get_text(" ", strip=True)  # "GOOGL - Alphabet Inc."
            if " - " in stock_text:
                company = stock_text.split(" - ", 1)[1].strip()
            # % of portfolio is the 3rd column
            pct_text = tds[2].get_text(strip=True).replace("%", "")
            try:
                pct = float(pct_text)
            except ValueError:
                pct = None
            if sym and 1 < len(sym) <= 7:
                positions.append({"ticker": sym, "company": company, "pct": pct})
        return positions
    except Exception as e:
        print(f"    ! {mgr_name}: {e}")
        return []


# ── Valuesider scraping (funds NOT tracked by Dataroma) ───────────────
# Valuesider's holdings TABLE is JS-rendered, but its server HTML states each
# fund's total holding count and lists the TOP holdings with weights. Since any
# >=10% position is by definition a top holding, this captures the concentrated
# small-cap bets from funds Dataroma doesn't track (e.g. Alta Fox, Arbiter).
VS_BASE = "https://valuesider.com"
_vs = crequests.Session(impersonate="chrome136", verify=VERIFY_SSL)
_COMMON_TOKENS = {
    "capital", "management", "partners", "partner", "fund", "funds", "group",
    "lp", "llc", "inc", "co", "the", "and", "investment", "investments",
    "advisors", "advisers", "asset", "holdings", "trust", "value", "growth",
    "select", "us", "equity", "global", "international",
}


def _tokens(text):
    toks = re.split(r"[^a-z0-9]+", text.lower())
    return {t for t in toks if t and t not in _COMMON_TOKENS and len(t) > 1}


def _is_duplicate_of_dataroma(slug, dr_token_sets):
    """True if a Valuesider slug refers to a fund already covered by Dataroma."""
    vt = _tokens(slug)
    for dt in dr_token_sets:
        if not dt:
            continue
        overlap = len(vt & dt)
        if overlap >= min(3, len(dt)):
            return True
    return False


def valuesider_slugs():
    """All Valuesider guru slugs from its sitemap."""
    try:
        xml = _vs.get(f"{VS_BASE}/sitemap.xml", timeout=20).text
        return sorted(set(re.findall(r"/guru/([a-z0-9\-]+)/portfolio", xml)))
    except Exception as e:
        print(f"  ! Valuesider sitemap failed ({e})")
        return []


def scrape_valuesider_fund(slug):
    """Parse one Valuesider guru page -> {name, count, positions[top]} or None."""
    try:
        html = _vs.get(f"{VS_BASE}/guru/{slug}/portfolio", timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"([A-Za-z0-9 .,&'\-]+?) has disclosed a total of (\d+) security holdings", txt)
        if not m:
            return None
        firm = m.group(1).strip()
        count = int(m.group(2))
        pm = re.search(r"What is ([^?]+?)'s portfolio", txt)
        person = pm.group(1).strip() if pm else ""
        name = f"{person} - {firm}" if person and person.lower() not in firm.lower() else firm
        # top holdings summary: "(TICKER)</a> COMPANY NAME (NN.NN%)" in the HTML
        positions = []
        for tk, mid_txt, pct in re.findall(r"\(([A-Z0-9.\-]{1,6})\)([^(]*?)\(([0-9.]+)%\)", html):
            company = re.sub(r"<[^>]+>", "", mid_txt).strip(" ,-–").title()
            positions.append({
                "ticker": tk.upper().strip(),
                "company": company,
                "pct": float(pct),
            })
        # de-dupe tickers, keep highest pct
        best = {}
        for p in positions:
            if p["ticker"] not in best or p["pct"] > best[p["ticker"]]["pct"]:
                best[p["ticker"]] = p
        return {"name": name, "count": count, "positions": list(best.values())}
    except Exception as e:
        print(f"    ! Valuesider {slug}: {e}")
        return None


def scrape_valuesider_only(dataroma_names):
    """Return {slug: {name, count, positions}} for Valuesider funds NOT on Dataroma."""
    dr_token_sets = [_tokens(n) for n in dataroma_names]
    slugs = valuesider_slugs()
    only = [s for s in slugs if not _is_duplicate_of_dataroma(s, dr_token_sets)]
    print(f"  Valuesider: {len(slugs)} gurus, {len(only)} not tracked by Dataroma.")
    out = {}
    lock = threading.Lock()

    def work(slug):
        d = scrape_valuesider_fund(slug)
        with lock:
            if d and d["positions"]:
                out[slug] = d

    with ThreadPoolExecutor(max_workers=4) as ex:
        for f in as_completed({ex.submit(work, s): s for s in only}):
            try:
                f.result()
            except Exception:
                pass
    return out


# ── Cache ─────────────────────────────────────────────────────────────
def load_cache(path=CACHE_FILE, label="holdings"):
    if not os.path.exists(path):
        return None
    age = (time.time() - os.path.getmtime(path)) / 86400
    if age > CACHE_DAYS:
        print(f"  {label} cache {age:.0f}d old (>{CACHE_DAYS}) — re-scraping.")
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        print(f"  Loaded {label} from cache ({age:.0f}d old, {len(data)} managers).")
        return data
    except Exception:
        return None


def save_cache(data, path=CACHE_FILE, label="holdings"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"  Cached {label} to {path}")


# ── HTML output ───────────────────────────────────────────────────────
def valuesider_slug(name):
    # e.g. "Li Lu - Himalaya Capital" -> best-effort guru search link
    base = name.split(" - ")[0]
    return base


def generate_html(rows, mgr_meta, elapsed):
    # rows: list of dicts {manager, mgr_id, holdings, ticker, company, pct, mcap}
    rows_sorted = sorted(
        rows,
        key=lambda r: (r["pct"] is None, -(r["pct"] or 0)),
    )
    # Put big positions (>=10%) first, then by pct desc
    rows_sorted = sorted(rows_sorted, key=lambda r: (0 if (r["pct"] or 0) >= BIG_POSITION else 1))

    body = ""
    for r in rows_sorted:
        pct = r["pct"]
        pct_str = f"{pct:.1f}%" if pct is not None else "—"
        pct_val = pct if pct is not None else -1
        big = (pct is not None and pct >= BIG_POSITION)
        mc = r["mcap"]
        mc_str = f"${mc/1e9:,.2f}B" if mc >= 1e9 else f"${mc/1e6:,.0f}M"
        mc_val = round(mc / 1e6)
        star = '<span class="star" title="Big slice of the fund (≥10%)">★</span> ' if big else ""
        row_cls = ' class="big"' if big else ""
        src = r.get("source", "Dataroma")
        src_short = "VS" if src == "Valuesider" else "DR"
        src_title = ("Valuesider-tracked fund (not on Dataroma) — top holdings shown"
                     if src == "Valuesider" else "View this fund on Dataroma")
        body += f"""
        <tr{row_cls} data-mgr="{r['manager'].lower()}" data-ticker="{r['ticker'].lower()}"
            data-holdings="{r['holdings']}" data-pct="{pct_val:.2f}" data-mcap="{mc_val}"
            data-source="{src}" data-big="{1 if big else 0}">
          <td class="mgr">{r['manager']} <span class="src {src_short.lower()}" title="{src_title}">{src_short}</span></td>
          <td class="ctr"><span class="badge">{r['holdings']}</span></td>
          <td><a class="tk" href="https://finance.yahoo.com/quote/{r['ticker']}" target="_blank">{r['ticker']}</a></td>
          <td class="name" title="{r['company']}">{r['company']}</td>
          <td class="ctr pct">{star}{pct_str}</td>
          <td class="ctr">{mc_str}</td>
          <td class="ctr links">
            <a href="{r['fund_url']}" target="_blank" title="{src_title}">{src_short}</a>
          </td>
        </tr>"""

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    n_rows = len(rows_sorted)
    n_big = sum(1 for r in rows_sorted if (r["pct"] or 0) >= BIG_POSITION)
    n_mgrs = len({r["manager"] for r in rows_sorted})
    mins, secs = int(elapsed // 60), int(elapsed % 60)

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Concentrated Superinvestor Small-Cap Tracker</title>
<style>
  :root {{ --bg:#fff; --bg2:#f6f8fa; --bg3:#eaeef2; --border:#d0d7de;
    --text:#1f2328; --muted:#656d76; --blue:#0969da; --green:#1a7f37;
    --orange:#9a6700; --red:#cf222e; --gold:#bf8700; --goldbg:#fff8e6; --purple:#8250df; }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:var(--bg); color:var(--text); font-size:13px; }}
  .header {{ background:var(--bg2); border-bottom:1px solid var(--border); padding:26px 32px 18px; position:relative; }}
  .refresh {{ position:absolute; top:26px; right:32px; background:var(--blue); color:#fff; border:0;
    border-radius:8px; padding:9px 16px; font-size:13px; font-weight:600; cursor:pointer; }}
  .refresh:hover {{ background:#0860ca; }}
  .header h1 {{ font-size:22px; font-weight:700; color:var(--blue); margin-bottom:6px; }}
  .header p {{ color:var(--muted); }}
  .tag {{ display:inline-block; background:rgba(63,185,80,.12); color:var(--green);
    border:1px solid rgba(63,185,80,.3); border-radius:20px; padding:2px 10px; font-size:11px; margin:0 3px; }}
  .tag.gold {{ background:var(--goldbg); color:var(--gold); border-color:#f0d98c; }}
  .stats {{ display:flex; gap:12px; margin-top:16px; flex-wrap:wrap; }}
  .stat {{ background:var(--bg3); border:1px solid var(--border); border-radius:8px; padding:10px 18px; min-width:120px; }}
  .stat-v {{ font-size:20px; font-weight:700; color:var(--blue); }}
  .stat-v.gold {{ color:var(--gold); }}
  .stat-l {{ font-size:11px; color:var(--muted); margin-top:2px; }}
  .filters {{ background:var(--bg2); border-bottom:1px solid var(--border); padding:14px 32px;
    display:flex; gap:14px; flex-wrap:wrap; align-items:flex-end; }}
  .fg {{ display:flex; flex-direction:column; gap:4px; }}
  .fg label {{ font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }}
  .fg input, .fg select {{ background:var(--bg3); border:1px solid var(--border); color:var(--text);
    padding:6px 10px; border-radius:6px; font-size:12px; outline:none; min-width:150px; }}
  .fg input:focus, .fg select:focus {{ border-color:var(--blue); }}
  .btn-reset {{ background:var(--bg3); border:1px solid var(--border); color:var(--muted);
    padding:6px 14px; border-radius:6px; cursor:pointer; font-size:12px; }}
  .btn-reset:hover {{ border-color:var(--blue); color:var(--blue); }}
  .wrap {{ padding:20px 32px; overflow-x:auto; }}
  table {{ width:100%; border-collapse:collapse; white-space:nowrap; }}
  thead tr {{ background:var(--bg3); }}
  th {{ padding:10px 12px; text-align:left; color:var(--muted); font-weight:600; font-size:11px;
    text-transform:uppercase; letter-spacing:.4px; cursor:pointer; user-select:none;
    border-bottom:1px solid var(--border); }}
  th:hover {{ color:var(--text); }}
  th.ctr {{ text-align:center; }}
  th.asc::after {{ content:" ↑"; color:var(--blue); }}
  th.desc::after {{ content:" ↓"; color:var(--blue); }}
  tbody tr {{ border-bottom:1px solid var(--border); }}
  tbody tr:hover {{ background:var(--bg2); }}
  tbody tr.big {{ background:var(--goldbg); }}
  tbody tr.big:hover {{ background:#fff2cf; }}
  td {{ padding:9px 12px; vertical-align:middle; }}
  td.ctr {{ text-align:center; }}
  .mgr {{ font-weight:600; max-width:260px; }}
  .src {{ font-size:9px; font-weight:700; padding:1px 5px; border-radius:4px; vertical-align:middle;
    margin-left:4px; }}
  .src.dr {{ background:var(--bg3); color:var(--muted); }}
  .src.vs {{ background:rgba(130,80,223,.14); color:var(--purple); }}
  .tk {{ color:var(--blue); text-decoration:none; font-weight:700; }}
  .tk:hover {{ text-decoration:underline; }}
  .name {{ max-width:230px; overflow:hidden; text-overflow:ellipsis; color:var(--text); }}
  .pct {{ font-weight:700; }}
  .badge {{ background:rgba(88,166,255,.15); color:var(--blue); border:1px solid rgba(88,166,255,.3);
    padding:1px 9px; border-radius:10px; font-size:11px; font-weight:700; }}
  .star {{ color:var(--gold); }}
  .links a {{ color:var(--muted); text-decoration:none; font-weight:700; font-size:11px;
    border:1px solid var(--border); border-radius:5px; padding:2px 6px; margin:0 2px; }}
  .links a:hover {{ color:var(--blue); border-color:var(--blue); }}
  .no-res {{ text-align:center; padding:60px; color:var(--muted); display:none; }}
  .footer {{ text-align:center; padding:20px 32px; color:var(--muted); font-size:11px; border-top:1px solid var(--border); }}
</style></head><body>

<div class="header">
  <button class="refresh" onclick="location.reload()" title="Reload to pull the latest data (auto-refreshed daily)">⟳ Refresh</button>
  <h1>Concentrated Superinvestor — Small-Cap Tracker</h1>
  <p>Positions held by focused fund managers, filtered to small caps:
    <span class="tag">Manager holds ≤ {MAX_HOLDINGS} stocks</span>
    <span class="tag">Stock market cap &lt; $3B</span>
    <span class="tag gold">★ = ≥ {BIG_POSITION:.0f}% of the fund</span>
  </p>
  <div class="stats">
    <div class="stat"><div class="stat-v" id="s-total">{n_rows}</div><div class="stat-l">Small-Cap Positions</div></div>
    <div class="stat"><div class="stat-v gold">{n_big}</div><div class="stat-l">★ High-Conviction (≥{BIG_POSITION:.0f}%)</div></div>
    <div class="stat"><div class="stat-v">{n_mgrs}</div><div class="stat-l">Concentrated Managers</div></div>
    <div class="stat"><div class="stat-v" id="s-showing">{n_rows}</div><div class="stat-l">Showing</div></div>
    <div class="stat"><div class="stat-v" style="font-size:13px">{now}</div><div class="stat-l">Data as of · refreshes daily</div></div>
  </div>
</div>

<div class="filters">
  <div class="fg"><label>Search</label><input id="f-q" type="text" placeholder="Manager or ticker..."/></div>
  <div class="fg"><label>Max Holdings</label><select id="f-h">
    <option value="20">≤ 20 (all)</option><option value="15">≤ 15</option>
    <option value="10">≤ 10</option><option value="5">≤ 5</option></select></div>
  <div class="fg"><label>Max Market Cap</label><select id="f-mc">
    <option value="3000">&lt; $3B (all)</option><option value="2000">&lt; $2B</option>
    <option value="1000">&lt; $1B</option><option value="500">&lt; $500M</option></select></div>
  <div class="fg"><label>Min % of Fund</label><select id="f-p">
    <option value="0" selected>Any</option><option value="5">≥ 5%</option>
    <option value="10">≥ 10% (★)</option><option value="20">≥ 20%</option></select></div>
  <button class="btn-reset" onclick="reset()">Reset</button>
</div>

<div class="wrap">
  <table>
    <thead><tr>
      <th data-s="mgr">Manager</th>
      <th class="ctr" data-s="holdings"># Holdings</th>
      <th data-s="ticker">Ticker</th>
      <th data-s="name">Company</th>
      <th class="ctr" data-s="pct">% of Fund</th>
      <th class="ctr" data-s="mcap">Market Cap</th>
      <th class="ctr">Links</th>
    </tr></thead>
    <tbody id="tb">{body}</tbody>
  </table>
  <div class="no-res" id="nr">No positions match the current filters.</div>
</div>

<div class="footer">
  <span class="src dr">DR</span> = Dataroma (full holdings) ·
  <span class="src vs">VS</span> = Valuesider-only fund, not on Dataroma (top holdings shown — a ≥{BIG_POSITION:.0f}% position is always a top holding) ·
  Market caps from Yahoo Finance · Data from 13F filings · Generated {now} · Informational only — not investment advice.
</div>

<script>
const tb=document.getElementById("tb"), rows=[...tb.querySelectorAll("tr")];
let sk="pct", sd=-1;
function num(r,a){{return parseFloat(r.dataset[a]??"-9999")||-9999;}}
function apply(){{
  const q=document.getElementById("f-q").value.toLowerCase();
  const mh=parseFloat(document.getElementById("f-h").value);
  const mc=parseFloat(document.getElementById("f-mc").value);
  const mp=parseFloat(document.getElementById("f-p").value);
  let n=0;
  rows.forEach(r=>{{
    const ok=(!q||r.dataset.mgr.includes(q)||r.dataset.ticker.includes(q))
      && num(r,"holdings")<=mh && num(r,"mcap")<=mc*1e6 && num(r,"pct")>=mp;
    r.style.display=ok?"":"none"; if(ok)n++;
  }});
  document.getElementById("s-showing").textContent=n;
  document.getElementById("nr").style.display=n===0?"block":"none";
}}
function sort(k){{
  if(sk===k)sd*=-1; else {{sk=k; sd=-1;}}
  document.querySelectorAll("th").forEach(t=>t.classList.remove("asc","desc"));
  const th=document.querySelector(`th[data-s="${{k}}"]`); if(th)th.classList.add(sd===-1?"desc":"asc");
  const idx={{mgr:0,holdings:1,ticker:2,name:3,pct:4,mcap:5}};
  function val(r){{
    if(["holdings","pct","mcap"].includes(k))return num(r,k);
    return (r.cells[idx[k]]?.textContent||"").toLowerCase();
  }}
  [...rows].sort((a,b)=>{{const x=val(a),y=val(b);return x<y?sd:x>y?-sd:0;}}).forEach(r=>tb.appendChild(r));
}}
function reset(){{
  document.getElementById("f-q").value="";
  document.getElementById("f-h").selectedIndex=0;
  document.getElementById("f-mc").selectedIndex=0;
  document.getElementById("f-p").value="0";
  apply();
}}
document.querySelectorAll("th[data-s]").forEach(t=>t.addEventListener("click",()=>sort(t.dataset.s)));
["f-q","f-h","f-mc","f-p"].forEach(id=>document.getElementById(id)
  .addEventListener(id==="f-q"?"input":"change",apply));
apply();
</script>
</body></html>"""
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "concentrated_tracker.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    return out


# ── Main ──────────────────────────────────────────────────────────────
def main():
    print("=" * 64)
    print("  CONCENTRATED SUPERINVESTOR SMALL-CAP TRACKER")
    print(f"  Managers <= {MAX_HOLDINGS} holdings | stocks < ${MCAP_CEILING/1e9:.0f}B mcap | flag >= {BIG_POSITION:.0f}%")
    print("=" * 64)
    t0 = time.time()

    # Step 1: get the live manager roster, then holdings per manager (cache-aware)
    managers = scrape_manager_list()
    print(f"\n  Dataroma roster: {len(managers)} superinvestors.")

    holdings = load_cache()
    # Re-scrape if no cache OR the roster changed (new/removed managers)
    if holdings is None or set(holdings.keys()) != set(managers.keys()):
        if holdings is not None:
            print("  Roster changed since last cache — re-scraping.")
        print(f"\n[1/3] Scraping {len(managers)} superinvestor portfolios...")
        holdings = {}
        for i, (mid, name) in enumerate(managers.items()):
            print(f"  {i+1:>2}/{len(managers)}  {name[:44]:<44}", end="\r", flush=True)
            positions = scrape_manager(mid, name)
            holdings[mid] = {"name": name, "positions": positions}
            time.sleep(0.3)
        print()
        save_cache(holdings)

    # Step 1b: Valuesider funds NOT tracked by Dataroma (own monthly cache)
    dr_names = [d["name"] for d in holdings.values()]
    vs_data = load_cache(VS_CACHE_FILE, "Valuesider")
    if vs_data is None:
        print("\n[1b] Scraping Valuesider for funds Dataroma doesn't track...")
        vs_data = scrape_valuesider_only(dr_names)
        save_cache(vs_data, VS_CACHE_FILE, "Valuesider")

    # Step 2: unify concentrated funds from both sources
    funds = []
    for mid, d in holdings.items():
        if 0 < len(d["positions"]) <= MAX_HOLDINGS:
            funds.append({
                "name": d["name"], "source": "Dataroma",
                "holdings": len(d["positions"]),
                "fund_url": f"{BASE_URL}/m/holdings.php?m={mid}",
                "positions": d["positions"],
            })
    for slug, d in vs_data.items():
        if 0 < d["count"] <= MAX_HOLDINGS:
            funds.append({
                "name": d["name"], "source": "Valuesider",
                "holdings": d["count"],
                "fund_url": f"{VS_BASE}/guru/{slug}/portfolio",
                "positions": d["positions"],
            })
    n_dr = sum(1 for f in funds if f["source"] == "Dataroma")
    n_vs = sum(1 for f in funds if f["source"] == "Valuesider")
    print(f"\n  {len(funds)} concentrated funds (<= {MAX_HOLDINGS} holdings): {n_dr} Dataroma + {n_vs} Valuesider-only.")

    tickers = sorted({p["ticker"] for f in funds for p in f["positions"]})
    print(f"\n[2/3] Fetching market caps for {len(tickers)} unique tickers (Yahoo)...")
    mcaps = {}
    done = [0]
    lock = threading.Lock()

    def work(tk):
        mc = fetch_market_cap(tk)
        with lock:
            done[0] += 1
            print(f"  {done[0]}/{len(tickers)}  {tk:<8}", end="\r", flush=True)
            if mc:
                mcaps[tk] = mc

    with ThreadPoolExecutor(max_workers=3) as ex:
        for f in as_completed({ex.submit(work, t): t for t in tickers}):
            try:
                f.result()
            except Exception:
                pass
    print()

    # Step 3: build rows (positions under the mcap ceiling)
    rows = []
    for f in funds:
        for p in f["positions"]:
            mc = mcaps.get(p["ticker"])
            if mc and mc < MCAP_CEILING:
                rows.append({
                    "manager": f["name"], "source": f["source"],
                    "holdings": f["holdings"], "fund_url": f["fund_url"],
                    "ticker": p["ticker"], "company": p["company"],
                    "pct": p["pct"], "mcap": mc,
                })

    n_big = sum(1 for r in rows if (r["pct"] or 0) >= BIG_POSITION)
    print(f"\n[3/3] {len(rows)} small-cap positions ({n_big} are ≥{BIG_POSITION:.0f}% of a fund).")

    elapsed = time.time() - t0
    out = generate_html(rows, funds, elapsed)
    print(f"\n{'='*64}")
    print(f"  Done in {int(elapsed//60)}m {int(elapsed%60)}s — opening {os.path.basename(out)}")
    print("=" * 64)
    if not os.environ.get("CI"):
        try:
            webbrowser.open(pathlib.Path(out).resolve().as_uri())
        except Exception:
            pass


if __name__ == "__main__":
    main()
