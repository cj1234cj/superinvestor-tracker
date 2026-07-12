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
from datetime import datetime, date
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


def _yahoo_quote(sym):
    """Return (market_cap, current_price, trailing_pe, price_to_sales) or all None."""
    crumb = _get_crumb()
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
    params = {"modules": "price,summaryDetail", "crumb": crumb}
    r = _yf.get(url, params=params, timeout=15)
    if r.status_code == 401 or (r.status_code == 200 and "Invalid Crumb" in r.text):
        global _crumb
        with _crumb_lock:
            _crumb = None
        params["crumb"] = _get_crumb()
        r = _yf.get(url, params=params, timeout=15)
    if r.status_code != 200:
        return None, None, None, None
    res = r.json().get("quoteSummary", {}).get("result")
    if not res:
        return None, None, None, None
    p = res[0].get("price", {})
    sd = res[0].get("summaryDetail", {})
    return ((p.get("marketCap") or {}).get("raw"),
            (p.get("regularMarketPrice") or {}).get("raw"),
            (sd.get("trailingPE") or {}).get("raw"),
            (sd.get("priceToSalesTrailing12Months") or {}).get("raw"))


def fetch_quote(ticker):
    """Return (market_cap, current_price, pe, ps). Retries with a Yahoo-normalized
    symbol (dot->dash, drop -OLD) so dot-class small caps aren't dropped."""
    try:
        time.sleep(random.uniform(0.15, 0.4))
        q = _yahoo_quote(ticker)
        if not q[0]:
            alt = ticker.replace(".", "-").replace("-OLD", "")
            if alt != ticker:
                q = _yahoo_quote(alt)
        return q
    except Exception:
        return None, None, None, None


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
    """Return list of positions: [{ticker, company, pct, value}] for a manager."""
    url = f"{BASE_URL}/m/holdings.php?m={mgr_id}"
    try:
        html = _dr.get(url, timeout=20).text
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="grid") or soup.find("table")
        if not table:
            return []
        # Map header names -> column index (robust to column shifts)
        header = table.find("tr")
        cols = [c.get_text(strip=True).lower() for c in header.find_all(["th", "td"])]
        pct_i = next((i for i, c in enumerate(cols) if "portfolio" in c), 2)
        val_i = next((i for i, c in enumerate(cols) if c == "value" or c.startswith("value")), None)
        price_i = next((i for i, c in enumerate(cols) if "reported" in c and "price" in c), None)
        positions = []
        for row in table.find_all("tr")[1:]:
            tds = row.find_all("td")
            if len(tds) < 3:
                continue
            sym, company = None, ""
            for a in row.find_all("a"):
                if "stock.php?sym=" in a.get("href", ""):
                    sym = a["href"].split("sym=")[-1].strip().upper()
                    break
            stock_text = tds[1].get_text(" ", strip=True)  # "GOOGL - Alphabet Inc."
            if " - " in stock_text:
                company = stock_text.split(" - ", 1)[1].strip()
            pct_text = tds[pct_i].get_text(strip=True).replace("%", "") if pct_i < len(tds) else ""
            try:
                pct = float(pct_text)
            except ValueError:
                pct = None
            value = None
            if val_i is not None and val_i < len(tds):
                v = re.sub(r"[^0-9]", "", tds[val_i].get_text(strip=True))
                value = float(v) if v else None    # Dataroma "Value" is in full $
            buy_price = None
            if price_i is not None and price_i < len(tds):
                p = re.sub(r"[^0-9.]", "", tds[price_i].get_text(strip=True))
                buy_price = float(p) if p else None    # reported price at the 13F filing
            if sym and 1 < len(sym) <= 7:
                positions.append({"ticker": sym, "company": company, "pct": pct,
                                  "value": value, "buy_price": buy_price})
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


# SEC EDGAR — the 13F filing gives exact value + shares -> reported (buy) price
_sec = crequests.Session(impersonate="chrome136", verify=VERIFY_SSL)
_SEC_UA = {"User-Agent": "concentrated-tracker research njerseycraig@gmail.com"}
_NAME_STOP = {"inc", "corp", "corporation", "co", "company", "companies", "ltd",
              "limited", "lp", "llc", "plc", "sa", "ag", "nv", "the", "group",
              "holdings", "holding", "cl", "class", "a", "b", "c", "adr", "and",
              "&", "com", "new", "de", "reit", "spa", "hldgs", "int", "intl"}


def _name_key(s):
    toks = [t for t in re.split(r"[^a-z0-9]+", s.lower()) if t and t not in _NAME_STOP]
    return toks


_SQUISH_SUF = ("incorporated", "inc", "corporation", "corp", "company", "companies",
               "co", "ltd", "limited", "lp", "llp", "llc", "plc", "sa", "ag", "nv",
               "holdings", "holding", "hldgs", "group", "grp", "trust", "adr",
               "cl", "class", "the", "com")


def _squish(s):
    """Punctuation/spacing-insensitive key: 'Wix.com Ltd' & 'Wixcom Ltd' -> 'wix'/'wixcom'
    both reduce toward the same stem after stripping corporate suffixes."""
    x = re.sub(r"[^a-z0-9]", "", s.lower())
    changed = True
    while changed:
        changed = False
        for suf in _SQUISH_SUF:
            if len(x) > len(suf) + 2 and x.endswith(suf):
                x = x[: -len(suf)]
                changed = True
    return x


def _sec_fetch(url):
    return _sec.get(url, headers=_SEC_UA, timeout=20).text


def fetch_13f_reported(index_url):
    """From a SEC filing index URL, return {key: {value, shares, price, ticker}}.
    Handles 13F info tables and, as a fallback, mutual-fund N-PORT filings
    (which also carry a direct ticker). Keys: 'T:<TICKER>' and issuer-name tokens."""
    try:
        page = _sec_fetch(index_url)
        xmls = re.findall(r'href="([^"]+\.xml)"', page)
        full = lambda p: ("https://www.sec.gov" + p) if p.startswith("/") else p
        out = {}

        def add(name, ticker, value, shares):
            if shares <= 0 or value <= 0:
                return
            price = value / shares
            if price < 0.5:          # legacy 13F values were in $-thousands
                value *= 1000
                price *= 1000
            rec = {"value": value, "shares": shares, "price": price, "ticker": ticker}
            keys = []
            if ticker:
                keys.append("T:" + ticker.upper())
            sq = _squish(name or "")
            if len(sq) >= 3:
                keys.append("S:" + sq)
            toks = _name_key(name or "")
            if toks:
                keys += [" ".join(toks[:2]), toks[0]]
            for k in keys:
                if k not in out or value > out[k]["value"]:
                    out[k] = rec

        # 1) 13F info table
        info = [x for x in xmls if "xslform13f" not in x.lower() and "primary_doc" not in x.lower()]
        if info:
            xml = _sec_fetch(full(info[0]))
            for b in re.findall(r"<(?:\w+:)?infoTable>(.*?)</(?:\w+:)?infoTable>", xml, re.S):
                def g(tag, bb=b):
                    m = re.search(r"<(?:\w+:)?" + tag + r">(.*?)</(?:\w+:)?" + tag + ">", bb, re.S)
                    return m.group(1).strip() if m else ""
                add(g("nameOfIssuer"), None,
                    float(re.sub(r"[^0-9.]", "", g("value")) or 0),
                    float(re.sub(r"[^0-9.]", "", g("sshPrnamt")) or 0))
            if out:
                out["__n__"] = len({id(v) for v in out.values()})
                return out

        # 2) fallback: N-PORT (mutual funds) — has a direct <ticker>
        prim = [x for x in xmls if x.lower().endswith("primary_doc.xml") and "xsl" not in x.lower()]
        if prim:
            xml = _sec_fetch(full(prim[0]))
            for b in re.findall(r"<invstOrSec>(.*?)</invstOrSec>", xml, re.S):
                nm = re.search(r"<name>(.*?)</name>", b, re.S)
                val = re.search(r"<valUSD>(.*?)</valUSD>", b)
                bal = re.search(r"<balance>(.*?)</balance>", b)
                tk = re.search(r'<ticker value="([^"]*)"', b)
                add(nm.group(1).strip() if nm else "",
                    tk.group(1).strip() if tk and tk.group(1).strip() else None,
                    float(re.sub(r"[^0-9.]", "", val.group(1)) if val else 0 or 0),
                    float(re.sub(r"[^0-9.]", "", bal.group(1)) if bal else 0 or 0))
        out["__n__"] = len({id(v) for v in out.values()})
        return out
    except Exception as e:
        print(f"    ! SEC filing {index_url}: {e}")
        return {}


# ── SEC company-facts: did revenue / operating cash flow double every 5 years? ──
_REV_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "RevenuesNetOfInterestExpense",
]
_OCF_CONCEPTS = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
]
_cik_map = None
_cik_lock = threading.Lock()


def _ticker_cik_map():
    global _cik_map
    with _cik_lock:
        if _cik_map is not None:
            return _cik_map
        try:
            j = _sec.get("https://www.sec.gov/files/company_tickers.json",
                         headers=_SEC_UA, timeout=25).json()
            _cik_map = {v["ticker"].upper(): v["cik_str"] for v in j.values()}
        except Exception:
            _cik_map = {}
        return _cik_map


def _annual_usd(gaap, concepts):
    """Merge concepts (companies switch XBRL tags over the years) -> {fiscal_year: value}."""
    out = {}
    for c in concepts:
        node = gaap.get(c)
        if not node:
            continue
        for u in node.get("units", {}).get("USD", []):
            s, e, v = u.get("start"), u.get("end"), u.get("val")
            if not s or not e or v is None:
                continue
            try:
                v = float(v)
                days = (date.fromisoformat(e) - date.fromisoformat(s)).days
            except Exception:
                continue
            if days < 350 or days > 380:          # annual periods only
                continue
            if u.get("form") not in ("10-K", "10-K/A", "20-F", "20-F/A"):
                continue
            out.setdefault(int(e[:4]), v)          # higher-priority concept wins per year
    return out


def _doubled_5yr(series):
    """'Yes' if the latest annual value is >= 2x its value ~5 years earlier
    (at least one doubling in the last 5 years). '—' if history is too short."""
    ys = sorted(series)
    if len(ys) < 2:
        return "—"
    latest = ys[-1]
    base_years = [y for y in ys if y <= latest - 5] or [y for y in ys if y <= latest - 4]
    if not base_years:
        return "—"
    by = max(base_years)                            # closest point ~5 years back
    if series[by] <= 0:
        return "—"
    # count a near-double as a double (>= 1.95x instead of 2.0x)
    return "Yes" if series[latest] >= 1.95 * series[by] else "No"


def fetch_doubling(ticker):
    """Return (rev_doubled, ocf_doubled, latest_annual_revenue, latest_annual_ocf).
    Statuses are 'Yes'/'No'/'—'; the latest annual figures feed P/S and P/OCF."""
    cmap = _ticker_cik_map()
    cik = cmap.get(ticker.upper()) or cmap.get(ticker.replace(".", "-").upper()) \
        or cmap.get(ticker.split(".")[0].upper())
    if not cik:
        return ("—", "—", None, None)
    try:
        time.sleep(random.uniform(0.1, 0.3))
        f = _sec.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json",
                     headers=_SEC_UA, timeout=25).json()
        g = f.get("facts", {}).get("us-gaap", {})
        rev = _annual_usd(g, _REV_CONCEPTS)
        ocf = _annual_usd(g, _OCF_CONCEPTS)
        latest_rev = rev[max(rev)] if rev else None
        latest_ocf = ocf[max(ocf)] if ocf else None
        return (_doubled_5yr(rev), _doubled_5yr(ocf), latest_rev, latest_ocf)
    except Exception:
        return ("—", "—", None, None)


def scrape_valuesider_fund(slug):
    """Parse one Valuesider guru page -> {name, count, positions[top]} or None.
    Enriches each position with the SEC 13F reported price + exact value."""
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
        # total portfolio value -> used to derive each position's $ size
        vm = re.search(r"portfolio value of \$([0-9,]+)", txt)
        port_value = float(vm.group(1).replace(",", "")) if vm else None
        # SEC 13F filing link -> exact value + reported (buy) price per holding
        sm = re.search(r"(/Archives/edgar/data/\d+/[\d-]+-index\.html)", html)
        reported = fetch_13f_reported("https://www.sec.gov" + sm.group(1)) if sm else {}
        filing_ok = reported.pop("__n__", 0) >= 3   # filing parsed with several holdings
        # top holdings summary: "(TICKER)</a> COMPANY NAME (NN.NN%)" in the HTML
        positions = []
        for tk, mid_txt, pct in re.findall(r"\(([A-Z0-9.\-]{1,6})\)([^(]*?)\(([0-9.]+)%\)", html):
            company = re.sub(r"<[^>]+>", "", mid_txt).strip(" ,-–").title()
            pctf = float(pct)
            # match this holding to its 13F/N-PORT record: ticker, then squished name, then tokens
            tku = tk.upper().strip()
            keys = _name_key(company)
            rec = reported.get("T:" + tku) or reported.get("S:" + _squish(company))
            if rec is None:
                for key in ([" ".join(keys[:2]), keys[0]] if keys else []):
                    if key in reported:
                        rec = reported[key]
                        break
            # drop positions Valuesider still lists but the latest filing no longer holds
            if rec is None and filing_ok:
                print(f"      · dropping stale {tku} from {name[:30]} (not in latest filing)")
                continue
            positions.append({
                "ticker": tku,
                "company": company,
                "pct": pctf,
                "value": rec["value"] if rec else ((port_value * pctf / 100.0) if port_value else None),
                "value_exact": rec is not None,
                "buy_price": rec["price"] if rec else None,
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
        if mc:
            mc_str = f"${mc/1e9:,.2f}B" if mc >= 1e9 else f"${mc/1e6:,.0f}M"
            mc_val = round(mc / 1e6)
        else:
            mc_str, mc_val = "—", -1
        val = r.get("value")
        if val:
            val_str = (f"${val/1e9:,.2f}B" if val >= 1e9
                       else f"${val/1e6:,.1f}M" if val >= 1e6
                       else f"${val/1e3:,.0f}K")
            val_val = round(val)
        else:
            val_str, val_val = "—", -1
        val_est = ' <span class="est" title="Estimated from portfolio value × weight">~</span>' if (val and not r.get("value_exact", True)) else ""
        bp = r.get("buy_price")
        cp = r.get("cur_price")
        bp_str = f"${bp:,.2f}" if bp else "—"
        cp_str = f"${cp:,.2f}" if cp else "—"
        bp_val = bp if bp else -1
        cp_val = cp if cp else -1
        gain_cls, gain_tip = "", ""
        if bp and cp:
            chg = (cp / bp - 1) * 100
            gain_cls = " up" if cp >= bp else " down"
            gain_tip = f' title="{chg:+.0f}% vs reported buy price"'
        star = '<span class="star" title="Big slice of the fund (≥10%)">★</span> ' if big else ""
        row_cls = ' class="big"' if big else ""
        src = r.get("source", "Dataroma")
        src_short = "VS" if src == "Valuesider" else "DR"
        src_title = ("Valuesider-tracked fund (not on Dataroma) — top holdings shown"
                     if src == "Valuesider" else "View this fund on Dataroma")
        rv = r.get("rev2x", "—")
        oc = r.get("ocf2x", "—")
        yn = lambda v: f'<span class="yn {("yes" if v=="Yes" else "no" if v=="No" else "na")}">{v}</span>'
        def ratio(v):
            return (f"{v:.1f}x", round(v, 2)) if (v is not None and 0 < v < 1000) else ("—", -1)
        ps_str, ps_val = ratio(r.get("ps"))
        pe_str, pe_val = ratio(r.get("pe"))
        pocf_str, pocf_val = ratio(r.get("pocf"))
        body += f"""
        <tr{row_cls} data-mgr="{r['manager'].lower()}" data-ticker="{r['ticker'].lower()}"
            data-holdings="{r['holdings']}" data-pct="{pct_val:.2f}" data-mcap="{mc_val}"
            data-source="{src}" data-value="{val_val}" data-buy="{bp_val}" data-cur="{cp_val}"
            data-ps="{ps_val}" data-pe="{pe_val}" data-pocf="{pocf_val}"
            data-rev="{rv}" data-ocf="{oc}" data-big="{1 if big else 0}">
          <td class="mgr">{r['manager']} <span class="src {src_short.lower()}" title="{src_title}">{src_short}</span></td>
          <td class="ctr"><span class="badge">{r['holdings']}</span></td>
          <td><a class="tk" href="https://finance.yahoo.com/quote/{r['ticker']}" target="_blank">{r['ticker']}</a></td>
          <td class="name" title="{r['company']}">{r['company']}</td>
          <td class="ctr pct">{star}{pct_str}</td>
          <td class="ctr">{val_str}{val_est}</td>
          <td class="ctr">{bp_str}</td>
          <td class="ctr{gain_cls}"{gain_tip}>{cp_str}</td>
          <td class="ctr">{mc_str}</td>
          <td class="ctr">{ps_str}</td>
          <td class="ctr">{pe_str}</td>
          <td class="ctr">{pocf_str}</td>
          <td class="ctr">{yn(rv)}</td>
          <td class="ctr">{yn(oc)}</td>
          <td class="ctr links">
            <a href="{r['fund_url']}" target="_blank" title="{src_title}">{src_short}</a>
          </td>
          <td class="ctr"><input type="checkbox" class="hidechk" title="Hide this row"></td>
        </tr>"""

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    n_rows = len(rows_sorted)
    n_big = sum(1 for r in rows_sorted if (r["pct"] or 0) >= BIG_POSITION)
    n_mgrs = len({r["manager"] for r in rows_sorted})
    mins, secs = int(elapsed // 60), int(elapsed % 60)

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Superinvestor Portfolio Tracker</title>
<style>
  :root {{ --bg:#fff; --bg2:#f6f8fa; --bg3:#eaeef2; --border:#d0d7de;
    --text:#1f2328; --muted:#656d76; --blue:#0969da; --green:#1a7f37;
    --orange:#9a6700; --red:#cf222e; --gold:#bf8700; --goldbg:#fff8e6; --purple:#8250df; }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:var(--bg); color:var(--text); font-size:13px; }}
  .header {{ background:var(--bg2); border-bottom:1px solid var(--border); padding:26px 32px 18px; position:relative; }}
  .actions {{ position:absolute; top:24px; right:32px; display:flex; gap:8px; }}
  .btn2 {{ background:var(--blue); color:#fff; border:0; border-radius:8px; padding:9px 14px;
    font-size:13px; font-weight:600; cursor:pointer; white-space:nowrap; }}
  .btn2:hover {{ background:#0860ca; }}
  .btn2.ghost {{ background:var(--bg3); color:var(--text); border:1px solid var(--border); }}
  .btn2.ghost:hover {{ border-color:var(--blue); color:var(--blue); }}
  .btn2.ok {{ background:var(--green); }}
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
  .btn-reset.active {{ border-color:var(--blue); color:var(--blue); background:var(--bg2); }}
  .hidechk {{ cursor:pointer; width:15px; height:15px; }}
  tbody tr.hidden-row {{ opacity:.4; }}
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
  .est {{ color:var(--muted); font-size:11px; }}
  .up {{ color:var(--green); font-weight:600; }}
  .down {{ color:var(--red); font-weight:600; }}
  .yn {{ font-size:11px; font-weight:700; padding:1px 8px; border-radius:10px; }}
  .yn.yes {{ background:rgba(63,185,80,.14); color:var(--green); }}
  .yn.no {{ background:rgba(207,34,46,.10); color:var(--red); }}
  .yn.na {{ background:var(--bg3); color:var(--muted); }}
  .links a {{ color:var(--muted); text-decoration:none; font-weight:700; font-size:11px;
    border:1px solid var(--border); border-radius:5px; padding:2px 6px; margin:0 2px; }}
  .links a:hover {{ color:var(--blue); border-color:var(--blue); }}
  .no-res {{ text-align:center; padding:60px; color:var(--muted); display:none; }}
  .footer {{ text-align:center; padding:20px 32px; color:var(--muted); font-size:11px; border-top:1px solid var(--border); }}
</style></head><body>

<div class="header">
  <div class="actions">
    <button class="btn2" onclick="copyForSheets(this)" title="Copy the visible rows as tab-separated text — paste straight into Google Sheets">⧉ Copy for Sheets</button>
    <button class="btn2 ghost" onclick="location.reload()" title="Reload to pull the latest data (auto-refreshed daily)">⟳ Refresh</button>
  </div>
  <h1>Superinvestor Portfolio Tracker</h1>
  <p>Every holding of every Dataroma &amp; Valuesider superinvestor — use the filters to narrow by
    concentration, size, valuation or growth.
    <span class="tag gold">★ = ≥ {BIG_POSITION:.0f}% of the fund</span>
  </p>
  <div class="stats">
    <div class="stat"><div class="stat-v" id="s-total">{n_rows}</div><div class="stat-l">Total Positions</div></div>
    <div class="stat"><div class="stat-v gold">{n_big}</div><div class="stat-l">★ High-Conviction (≥{BIG_POSITION:.0f}%)</div></div>
    <div class="stat"><div class="stat-v">{n_mgrs}</div><div class="stat-l">Managers</div></div>
    <div class="stat"><div class="stat-v" id="s-showing">{n_rows}</div><div class="stat-l">Showing</div></div>
    <div class="stat"><div class="stat-v" style="font-size:13px">{now}</div><div class="stat-l">Data as of · refreshes daily</div></div>
  </div>
</div>

<div class="filters">
  <div class="fg"><label>Search</label><input id="f-q" type="text" placeholder="Manager or ticker..."/></div>
  <div class="fg"><label>Max Holdings</label><select id="f-h">
    <option value="9999" selected>Any</option><option value="20">≤ 20</option>
    <option value="15">≤ 15</option><option value="10">≤ 10</option>
    <option value="5">≤ 5</option></select></div>
  <div class="fg"><label>Max Market Cap</label><select id="f-mc">
    <option value="99000000" selected>Any</option><option value="50000">&lt; $50B</option>
    <option value="10000">&lt; $10B</option><option value="3000">&lt; $3B</option>
    <option value="2000">&lt; $2B</option><option value="1000">&lt; $1B</option>
    <option value="500">&lt; $500M</option></select></div>
  <div class="fg"><label>Min % of Fund</label><select id="f-p">
    <option value="0" selected>Any</option><option value="5">≥ 5%</option>
    <option value="10">≥ 10% (★)</option><option value="20">≥ 20%</option></select></div>
  <div class="fg"><label>Min Investment ($)</label><select id="f-inv">
    <option value="0" selected>Any</option><option value="1000000">≥ $1M</option>
    <option value="5000000">≥ $5M</option><option value="10000000">≥ $10M</option>
    <option value="25000000">≥ $25M</option><option value="50000000">≥ $50M</option>
    <option value="100000000">≥ $100M</option></select></div>
  <div class="fg"><label>Compounders</label><select id="f-comp">
    <option value="">Any</option>
    <option value="both">Rev &amp; OCF 2× (both Yes)</option>
    <option value="rev">Revenue 2× (Yes)</option>
    <option value="ocf">OCF 2× (Yes)</option></select></div>
  <button class="btn-reset" id="f-show" onclick="setHidden(true)" title="Reveal hidden rows (greyed) so you can un-check them">Show hidden (0)</button>
  <button class="btn-reset active" id="f-hide" onclick="setHidden(false)" title="Collapse hidden rows out of view">Hide hidden</button>
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
      <th class="ctr" data-s="value" title="Dollar size of the position at the latest 13F filing">$ Position</th>
      <th class="ctr" data-s="buy" title="Reported price at the manager's 13F filing (Dataroma funds)">Buy Price</th>
      <th class="ctr" data-s="cur" title="Current price from Yahoo Finance — refreshed daily">Current</th>
      <th class="ctr" data-s="mcap">Market Cap</th>
      <th class="ctr" data-s="ps" title="Price / Sales — trailing-12-month (Yahoo)">P/S</th>
      <th class="ctr" data-s="pe" title="Price / Earnings — trailing (Yahoo); blank if unprofitable">P/E</th>
      <th class="ctr" data-s="pocf" title="Price / Operating Cash Flow — market cap ÷ latest annual operating cash flow (SEC)">P/OCF</th>
      <th class="ctr" data-s="rev" title="Yes if revenue at least doubled over the last ~5 years — a near-double (≥1.95×) counts (SEC filings)">Rev 2×/5y</th>
      <th class="ctr" data-s="ocf" title="Yes if operating cash flow at least doubled over the last ~5 years — a near-double (≥1.95×) counts (SEC filings)">OCF 2×/5y</th>
      <th class="ctr">Links</th>
      <th class="ctr" title="Check to hide a row from the view &amp; export. Kept in your browser — the data is not lost, and 'Show hidden' brings it back.">Hide</th>
    </tr></thead>
    <tbody id="tb">{body}</tbody>
  </table>
  <div class="no-res" id="nr">No positions match the current filters.</div>
</div>

<div class="footer">
  <span class="src dr">DR</span> = Dataroma (full holdings) ·
  <span class="src vs">VS</span> = Valuesider-only fund, not on Dataroma (top holdings shown — a ≥{BIG_POSITION:.0f}% position is always a top holding) ·
  <b>$ Position</b> = value at latest 13F (<span class="est">~</span> = estimated for VS funds) ·
  <b>Buy Price</b> = reported price at the 13F filing (Dataroma; SEC EDGAR 13F for Valuesider funds) ·
  <b>Current</b> &amp; Market Cap = Yahoo Finance, refreshed daily ·
  <b>Rev/OCF 2×/5y</b> = Yes if revenue / operating cash flow at least doubled over the last ~5 years, counting a near-double (≥1.95×) (SEC filings; — = too little history) ·
  Generated {now} · Informational only — not investment advice.
</div>

<script>
const tb=document.getElementById("tb"), rows=[...tb.querySelectorAll("tr")];
let sk="", sdesc=true;
const NUMCOL=["holdings","pct","value","buy","cur","mcap","ps","pe","pocf"];
function num(r,a){{const n=parseFloat(r.dataset[a]);return isNaN(n)?-9999:n;}}

// --- Hide state (persisted in the browser; survives reloads & daily refreshes) ---
const HKEY="hidden_v1";
let hiddenSet=new Set(); try {{ hiddenSet=new Set(JSON.parse(localStorage.getItem(HKEY)||"[]")); }} catch(e) {{}}
let showHidden=false;
function rowId(r){{ return r.dataset.mgr+"|"+r.dataset.ticker; }}
function saveHidden(){{ try {{ localStorage.setItem(HKEY,JSON.stringify([...hiddenSet])); }} catch(e) {{}} }}
function updateHiddenLabel(){{
  const b=document.getElementById("f-show");
  if(b) b.textContent="Show hidden ("+hiddenSet.size+")";
}}
function setHidden(v){{
  showHidden=v;
  document.getElementById("f-show").classList.toggle("active",v);
  document.getElementById("f-hide").classList.toggle("active",!v);
  updateHiddenLabel(); apply();
}}

function apply(){{
  const q=document.getElementById("f-q").value.toLowerCase();
  const mh=parseFloat(document.getElementById("f-h").value);
  const mc=parseFloat(document.getElementById("f-mc").value);
  const mp=parseFloat(document.getElementById("f-p").value);
  const mi=parseFloat(document.getElementById("f-inv").value);
  const comp=document.getElementById("f-comp").value;
  let n=0;
  rows.forEach(r=>{{
    const isHidden=hiddenSet.has(rowId(r));
    const compOk = comp===""
      || (comp==="both" && r.dataset.rev==="Yes" && r.dataset.ocf==="Yes")
      || (comp==="rev" && r.dataset.rev==="Yes")
      || (comp==="ocf" && r.dataset.ocf==="Yes");
    const ok=(!q||r.dataset.mgr.includes(q)||r.dataset.ticker.includes(q))
      && num(r,"holdings")<=mh && num(r,"mcap")<=mc && num(r,"pct")>=mp
      && (mi<=0 || num(r,"value")>=mi) && compOk
      && (!isHidden || showHidden);
    r.classList.toggle("hidden-row", isHidden && ok);
    r.style.display=ok?"":"none"; if(ok)n++;
  }});
  document.getElementById("s-showing").textContent=n;
  document.getElementById("nr").style.display=n===0?"block":"none";
}}
function sort(k){{
  // first click: numbers high->low, text A->Z; click again to reverse
  if(sk===k) sdesc=!sdesc; else {{sk=k; sdesc=NUMCOL.includes(k);}}
  document.querySelectorAll("th").forEach(t=>t.classList.remove("asc","desc"));
  const th=document.querySelector(`th[data-s="${{k}}"]`); if(th)th.classList.add(sdesc?"desc":"asc");
  const idx={{mgr:0,holdings:1,ticker:2,name:3,pct:4,value:5,buy:6,cur:7,mcap:8,rev:9,ocf:10}};
  function val(r){{
    if(NUMCOL.includes(k))return num(r,k);
    if(k==="rev"||k==="ocf")return r.dataset[k];   // Yes / No / —
    return (r.cells[idx[k]]?.textContent||"").toLowerCase();
  }}
  const sorted=[...rows].sort((a,b)=>{{const x=val(a),y=val(b);
    if(x<y)return sdesc?1:-1; if(x>y)return sdesc?-1:1; return 0;}});
  const parent=tb.parentNode, next=tb.nextSibling;
  parent.removeChild(tb);              // detach tbody -> reorder off-layout
  sorted.forEach(r=>tb.appendChild(r));
  parent.insertBefore(tb, next);       // one reflow on reattach
}}
function reset(){{
  document.getElementById("f-q").value="";
  document.getElementById("f-h").selectedIndex=0;
  document.getElementById("f-mc").selectedIndex=0;
  document.getElementById("f-p").value="0";
  document.getElementById("f-inv").selectedIndex=0;
  document.getElementById("f-comp").value="";
  apply();
}}
document.querySelectorAll("th[data-s]").forEach(t=>t.addEventListener("click",()=>sort(t.dataset.s)));
["f-q","f-h","f-mc","f-p","f-inv","f-comp"].forEach(id=>document.getElementById(id)
  .addEventListener(id==="f-q"?"input":"change",apply));

function copyForSheets(btn){{
  const cols=["Manager","Source","# Holdings","Ticker","Company","% of Fund",
    "$ Position","Buy Price","Current Price","Market Cap","P/S","P/E","P/OCF","Rev 2x/5y","OCF 2x/5y"];
  const lines=[cols.join("\\t")];
  const nn=v=>{{const n=parseFloat(v);return (isNaN(n)||n<0)?"":n;}};
  rows.forEach(r=>{{
    if(r.style.display==="none"||hiddenSet.has(rowId(r)))return;
    const mgr=(r.cells[0].textContent||"").replace(/\\s+(DR|VS)\\s*$/,"").trim();
    const tk=r.cells[2].textContent.trim();
    const co=r.cells[3].textContent.trim();
    const mcap=nn(r.dataset.mcap); // in $M
    lines.push([mgr, r.dataset.source, r.dataset.holdings, tk, co,
      nn(r.dataset.pct), nn(r.dataset.value), nn(r.dataset.buy), nn(r.dataset.cur),
      mcap===""?"":Math.round(mcap*1e6), nn(r.dataset.ps), nn(r.dataset.pe), nn(r.dataset.pocf),
      r.dataset.rev, r.dataset.ocf].join("\\t"));
  }});
  const tsv=lines.join("\\n");
  const done=()=>{{const t=btn.textContent;btn.textContent="Copied "+(lines.length-1)+" rows ✓";
    btn.classList.add("ok");setTimeout(()=>{{btn.textContent=t;btn.classList.remove("ok");}},1800);}};
  if(navigator.clipboard&&navigator.clipboard.writeText){{
    navigator.clipboard.writeText(tsv).then(done).catch(()=>fallback(tsv,done));
  }} else fallback(tsv,done);
}}
function fallback(text,done){{
  const ta=document.createElement("textarea");ta.value=text;
  ta.style.position="fixed";ta.style.opacity="0";document.body.appendChild(ta);
  ta.select();try{{document.execCommand("copy");done();}}catch(e){{}}
  document.body.removeChild(ta);
}}

// wire the per-row Hide checkboxes to the persisted hidden set
rows.forEach(r=>{{
  const cb=r.querySelector(".hidechk"); if(!cb) return;
  cb.checked=hiddenSet.has(rowId(r));
  cb.addEventListener("change",()=>{{
    if(cb.checked) hiddenSet.add(rowId(r)); else hiddenSet.delete(rowId(r));
    saveHidden(); updateHiddenLabel(); apply();
  }});
}});
updateHiddenLabel();
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

    # Step 2: unify ALL funds from both sources (every stock in every portfolio)
    funds = []
    for mid, d in holdings.items():
        if len(d["positions"]) > 0:
            funds.append({
                "name": d["name"], "source": "Dataroma",
                "holdings": len(d["positions"]),
                "fund_url": f"{BASE_URL}/m/holdings.php?m={mid}",
                "positions": d["positions"],
            })
    for slug, d in vs_data.items():
        if d["count"] > 0 and d["positions"]:
            funds.append({
                "name": d["name"], "source": "Valuesider",
                "holdings": d["count"],
                "fund_url": f"{VS_BASE}/guru/{slug}/portfolio",
                "positions": d["positions"],
            })
    n_dr = sum(1 for f in funds if f["source"] == "Dataroma")
    n_vs = sum(1 for f in funds if f["source"] == "Valuesider")
    print(f"\n  {len(funds)} funds (all holdings): {n_dr} Dataroma + {n_vs} Valuesider-only.")

    tickers = sorted({p["ticker"] for f in funds for p in f["positions"]})
    print(f"\n[2/3] Fetching market cap + current price for {len(tickers)} tickers (Yahoo, live)...")
    quotes = {}
    done = [0]
    lock = threading.Lock()

    def work(tk):
        mc, price, pe, ps = fetch_quote(tk)
        with lock:
            done[0] += 1
            print(f"  {done[0]}/{len(tickers)}  {tk:<8}", end="\r", flush=True)
            if mc:
                quotes[tk] = {"mcap": mc, "price": price, "pe": pe, "ps": ps}

    with ThreadPoolExecutor(max_workers=3) as ex:
        for f in as_completed({ex.submit(work, t): t for t in tickers}):
            try:
                f.result()
            except Exception:
                pass
    print()

    # Step 3: build rows — EVERY position of every portfolio (no size/concentration cutoff)
    rows = []
    for f in funds:
        for p in f["positions"]:
            q = quotes.get(p["ticker"]) or {}
            rows.append({
                "manager": f["name"], "source": f["source"],
                "holdings": f["holdings"], "fund_url": f["fund_url"],
                "ticker": p["ticker"], "company": p["company"],
                "pct": p["pct"], "value": p.get("value"),
                "value_exact": p.get("value_exact", True),
                "buy_price": p.get("buy_price"), "cur_price": q.get("price"),
                "mcap": q.get("mcap"),
            })

    n_big = sum(1 for r in rows if (r["pct"] or 0) >= BIG_POSITION)
    print(f"\n[3/3] {len(rows)} total positions ({n_big} are ≥{BIG_POSITION:.0f}% of a fund).")

    # Step 4: revenue & operating-cash-flow doubling (SEC company-facts, ~10yr history)
    row_tickers = sorted({r["ticker"] for r in rows})
    print(f"\n[4/4] Checking 5-yr doubling (revenue & OCF) for {len(row_tickers)} stocks via SEC...")
    dbl = {}
    _ticker_cik_map()   # warm the ticker->CIK map once
    dlock = threading.Lock()

    def work_dbl(tk):
        res = fetch_doubling(tk)
        with dlock:
            dbl[tk] = res

    with ThreadPoolExecutor(max_workers=6) as ex:
        for fut in as_completed({ex.submit(work_dbl, t): t for t in row_tickers}):
            try:
                fut.result()
            except Exception:
                pass
    def _num(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    for r in rows:
        rv, oc, latest_rev, latest_ocf = dbl.get(r["ticker"], ("—", "—", None, None))
        r["rev2x"], r["ocf2x"] = rv, oc
        q = quotes.get(r["ticker"], {})
        mc = _num(r["mcap"])
        pe = _num(q.get("pe"))
        ps = _num(q.get("ps"))
        latest_rev = _num(latest_rev)
        latest_ocf = _num(latest_ocf)
        r["ps"] = ps if (ps and ps > 0) else ((mc / latest_rev) if (mc and latest_rev and latest_rev > 0) else None)
        r["pe"] = pe if (pe and pe > 0) else None
        r["pocf"] = (mc / latest_ocf) if (mc and latest_ocf and latest_ocf > 0) else None
    n_comp = sum(1 for r in rows if r["rev2x"] == "Yes" and r["ocf2x"] == "Yes")
    print(f"  {n_comp} stocks doubled BOTH revenue & OCF over the last 5 years.")

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
