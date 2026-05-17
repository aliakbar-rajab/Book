#!/usr/bin/env python3
"""
Libgen downloader for GitHub Actions.

Accepted inputs (BOOK_URL env var):
  - Bare MD5:                  ee012119554cf6f9a2a4fd5662de8d17
  - libgen ads page:           https://libgen.li/ads.php?md5=<md5>
                               https://libgen.is/ads.php?md5=<md5>
  - library.lol page:         https://library.lol/main/<md5>
  - libgen edition page:       https://libgen.li/edition.php?id=<id>
  - Direct CDN (key optional): https://cdn4.booksdl.lc/get.php?md5=<md5>&key=...

The safest input is a bare MD5 or ads.php?md5= URL.
Direct get.php?key= URLs have keys that expire within minutes.
"""

import concurrent.futures
import hashlib
import json
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# ─────────────────────────────────────────── config ───────────────────────────

CACHE_DIR  = Path(".download_cache")
CACHE_FILE = CACHE_DIR / "downloads.json"

MAX_RETRIES      = 20
CHUNK_SIZE       = 256 * 1024  # 256 KB
TIMEOUT          = (15, 90)    # connect, read
STALL_TIMEOUT    = 120         # seconds of silence = stalled
BASE_BACKOFF     = 3
MAX_BACKOFF      = 90
MAX_FILE_MB      = 1900        # warn above this

LIBGEN_MIRRORS = [
    "https://libgen.is",
    "https://libgen.rs",
    "https://libgen.st",
    "https://libgen.li",
    "https://libgen.vip",
]

# Hosts that serve raw file bytes given /main/<md5>
DOWNLOAD_HOSTS = [
    "https://library.lol",
    "https://libgen.li",
    "https://cdn1.booksdl.org",
    "https://cdn2.booksdl.org",
    "https://cdn3.booksdl.org",
    "https://cdn4.booksdl.lc",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

BOOK_EXTS = (
    ".epub", ".pdf", ".mobi", ".azw3", ".azw",
    ".djvu", ".fb2", ".cbz", ".cbr", ".lit", ".lrf",
)

# (magic_bytes, label)
MAGIC = [
    (b"PK\x03\x04", "epub/zip"),
    (b"%PDF",        "pdf"),
    (b"AT&TFORM",    "djvu"),
    (b"BOOKMOBI",    "mobi"),
]

CACHE_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────── session ──────────────────────────

def _retry() -> Retry:
    return Retry(
        total=6, connect=6, read=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
    })
    a = HTTPAdapter(max_retries=_retry(), pool_connections=4, pool_maxsize=8)
    s.mount("https://", a)
    s.mount("http://",  a)
    return s


# Shared session for page fetching only.
# Each download attempt gets its own session to avoid stale pool issues.
SESSION = make_session()


# ─────────────────────────────────────────── cache ────────────────────────────

def load_cache() -> dict:
    try:
        return json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(data: dict) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(data, indent=2))
    except OSError as e:
        print(f"  ⚠ cache write failed: {e}")


# ─────────────────────────────────────────── helpers ──────────────────────────

def fmt(size: float) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{size:.2f} TB"


def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def is_md5(s: str) -> bool:
    return bool(re.fullmatch(r"[a-fA-F0-9]{32}", s.strip()))


def md5_from_url(url: str) -> Optional[str]:
    """Extract MD5 from query string, path, or anywhere in the URL."""
    qs = parse_qs(urlparse(url).query)
    for k in ("md5", "MD5"):
        if k in qs and is_md5(qs[k][0]):
            return qs[k][0].lower()
    for part in urlparse(url).path.split("/"):
        if is_md5(part):
            return part.lower()
    m = re.search(r"[=/_-]([a-fA-F0-9]{32})(?:[&/?]|$)", url)
    return m.group(1).lower() if m else None


def sanitize(name: str) -> str:
    name = unquote(name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" ._")
    if len(name) > 200:
        stem, ext = os.path.splitext(name)
        name = stem[:200 - len(ext)] + ext
    return name or f"book_{int(time.time())}"


def fname_from_headers(r: requests.Response) -> Optional[str]:
    cd = r.headers.get("content-disposition", "")
    m = re.search(r"filename\*\s*=\s*(?:UTF-8'')?([^;]+)", cd, re.I)
    if m:
        return sanitize(m.group(1).strip().strip('"\''))
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.I)
    if m:
        return sanitize(m.group(1).strip())
    return None


def fname_from_url(url: str) -> str:
    name = os.path.basename(unquote(urlparse(url).path))
    if name and "." in name and not name.startswith("."):
        return sanitize(name)
    return f"book_{int(time.time())}.epub"


def is_binary_response(content_type: str, head: bytes) -> bool:
    ct = content_type.lower()
    binary_ct = (
        "application/epub", "application/pdf", "application/octet-stream",
        "application/zip", "application/x-mobipocket", "application/x-mobi8",
        "application/vnd.amazon", "application/x-fictionbook", "image/vnd.djvu",
    )
    if any(b in ct for b in binary_ct):
        return True
    return any(head[:len(m)] == m or m in head[:64] for m, _ in MAGIC)


def is_html(data: bytes) -> bool:
    s = data[:256].lower()
    return any(m in s for m in (b"<!doctype html", b"<html", b"<head>", b"<title>"))


# ─────────────────────────────────────────── mirror probe ─────────────────────

def _probe(mirror: str, md5: str) -> Optional[str]:
    url = f"{mirror}/ads.php?md5={md5}"
    try:
        r = SESSION.get(url, timeout=(8, 15))
        if r.status_code == 200 and len(r.text) > 400:
            return url
    except requests.RequestException:
        pass
    return None


def best_ads_url(md5: str) -> str:
    """Return the fastest responding ads.php URL, falling back to library.lol."""
    print("  🔍 Probing mirrors...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(LIBGEN_MIRRORS)) as ex:
        futs = {ex.submit(_probe, m, md5): m for m in LIBGEN_MIRRORS}
        for fut in concurrent.futures.as_completed(futs):
            url = fut.result()
            if url:
                print(f"  ✓ Using: {futs[fut]}")
                for f in futs:
                    f.cancel()
                return url
    fallback = f"https://library.lol/main/{md5}"
    print(f"  ⚠ All mirrors failed, using {fallback}")
    return fallback


# ─────────────────────────────────────────── link extraction ──────────────────

def _md5_from_page(soup: BeautifulSoup, html: str) -> Optional[str]:
    for tag in soup.find_all(["td", "th", "a", "span", "div"]):
        t = tag.get_text(strip=True)
        if is_md5(t):
            return t.lower()
    m = re.search(r'(?:md5|MD5)\s*[=:]\s*["\']?([a-fA-F0-9]{32})', html)
    return m.group(1).lower() if m else None


def _link_from_soup(soup: BeautifulSoup, base: str, html: str) -> Optional[str]:
    # 1. Explicit GET/DOWNLOAD button
    for a in soup.find_all("a", href=True):
        t = a.get_text(strip=True).upper()
        h = a["href"]
        if t in {"GET", "DOWNLOAD", "TÉLÉCHARGER", "DESCARGAR", "СКАЧАТЬ"}:
            return urljoin(base, h)
        if "get.php" in h.lower():
            return urljoin(base, h)
    # 2. href ends with book extension
    for a in soup.find_all("a", href=True):
        if any(a["href"].lower().split("?")[0].endswith(e) for e in BOOK_EXTS):
            return urljoin(base, a["href"])
    # 3. CDN keywords in href
    cdn = ("cloudflare-ipfs", "ipfs.io", "booksdl", "library.lol", "/main/")
    for a in soup.find_all("a", href=True):
        if any(k in a["href"].lower() for k in cdn):
            return urljoin(base, a["href"])
    # 4. Regex over raw HTML
    for m in re.findall(r'https?://[^\s"\'<>]+', html):
        ml = m.lower().split("?")[0]
        if any(ml.endswith(e) for e in BOOK_EXTS) or "/get.php" in m.lower():
            return m
    return None


def _fetch_page(url: str) -> tuple[requests.Response, BeautifulSoup]:
    r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r, BeautifulSoup(r.text, "lxml")


# ─────────────────────────────────────────── URL resolution ───────────────────

def resolve(url: str) -> tuple[str, Optional[str]]:
    """
    Given any supported input, return (direct_download_url, md5_or_None).

    Resolution order:
      bare MD5          → mirror probe → ads page → GET link
      ads.php?md5=      → extract md5 → same
      library.lol/main/ → parse GET button
      edition.php?id=   → parse page → fallback to md5
      direct get.php    → validate; if expired key, re-resolve via md5
      unknown           → try HEAD for binary, else parse as HTML
    """
    url = url.strip()

    # ── bare MD5 ──────────────────────────────────────────────────────────────
    if is_md5(url):
        return _resolve_md5(url.lower())

    parsed = urlparse(url)
    host   = parsed.netloc.lower()
    path   = parsed.path.lower()
    qs     = parsed.query.lower()

    # ── ads.php?md5= ──────────────────────────────────────────────────────────
    if "ads.php" in path and "md5" in qs:
        md5 = md5_from_url(url)
        if md5:
            return _resolve_md5(md5)

    # ── library.lol/main/<md5> ────────────────────────────────────────────────
    if "library.lol" in host and "/main/" in path:
        md5 = md5_from_url(url)
        r, soup = _fetch_page(url)
        link = _link_from_soup(soup, r.url, r.text)
        if link:
            return link, md5
        raise RuntimeError(f"No download link on library.lol page: {url}")

    # ── edition.php?id= ───────────────────────────────────────────────────────
    if "edition.php" in path:
        r, soup = _fetch_page(url)
        md5 = _md5_from_page(soup, r.text)
        link = _link_from_soup(soup, r.url, r.text)
        if link:
            return link, md5
        if md5:
            return _resolve_md5(md5)
        raise RuntimeError(f"Nothing usable on edition page: {url}")

    # ── direct get.php or file URL ────────────────────────────────────────────
    if "get.php" in path or any(path.endswith(e) for e in BOOK_EXTS):
        md5 = md5_from_url(url)
        # Validate it's still alive
        try:
            with SESSION.get(url, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 200:
                    head = next(r.iter_content(256), b"")
                    ct   = r.headers.get("content-type", "")
                    if is_binary_response(ct, head) and not is_html(head):
                        print("  ✓ Direct URL is live")
                        return r.url, md5 or md5_from_url(r.url)
        except requests.RequestException as e:
            print(f"  ⚠ Direct probe failed: {e}")
        # Key expired or bad — re-resolve via md5
        if md5:
            print(f"  ↩ Key likely expired; re-resolving via MD5 {md5}")
            return _resolve_md5(md5)
        raise RuntimeError(f"Direct URL failed and no MD5 in URL: {url}")

    # ── generic fallback ──────────────────────────────────────────────────────
    print(f"  ⚠ Unknown URL pattern, attempting generic resolution")
    md5 = md5_from_url(url)
    if md5:
        return _resolve_md5(md5)
    # Try fetching as HTML
    r, soup = _fetch_page(url)
    page_md5 = _md5_from_page(soup, r.text)
    link = _link_from_soup(soup, r.url, r.text)
    if link:
        return link, page_md5
    if page_md5:
        return _resolve_md5(page_md5)
    raise RuntimeError(f"Cannot resolve: {url}")


def _resolve_md5(md5: str) -> tuple[str, str]:
    """Given a confirmed MD5, get the best ads page and extract a download link."""
    ads_url = best_ads_url(md5)

    # library.lol/main/<md5> returns the file directly after one redirect
    if "library.lol/main/" in ads_url:
        return ads_url, md5

    r, soup = _fetch_page(ads_url)
    link = _link_from_soup(soup, r.url, r.text)
    if link:
        return link, md5

    # Hard fallback: library.lol
    fallback = f"https://library.lol/main/{md5}"
    print(f"  ↩ No link on ads page; using {fallback}")
    return fallback, md5


# ─────────────────────────────────────────── stall detection ──────────────────

class Stall:
    def __init__(self, timeout: int, grace: int = 30):
        self._t   = timeout
        self._g   = grace
        self._s   = time.monotonic()
        self._l   = time.monotonic()
        self._tot = 0

    def tick(self, n: int) -> None:
        self._tot += n
        self._l    = time.monotonic()

    def check(self) -> None:
        idle    = time.monotonic() - self._l
        elapsed = time.monotonic() - self._s
        limit   = self._t if elapsed > self._g else self._t * 2
        if idle > limit:
            raise TimeoutError(
                f"No data for {idle:.0f}s (got {fmt(self._tot)} total)"
            )


# ─────────────────────────────────────────── download ─────────────────────────

def _range_ok(url: str) -> bool:
    try:
        r = SESSION.head(url, timeout=(8, 10), allow_redirects=True)
        return r.headers.get("accept-ranges", "").lower() == "bytes"
    except requests.RequestException:
        return False


def download_file(url: str, out: Path, expected_md5: Optional[str] = None) -> Path:
    out.mkdir(parents=True, exist_ok=True)

    uid       = hashlib.sha1(url.encode()).hexdigest()[:16]
    tmp       = out / f".{uid}.part"
    meta      = out / f".{uid}.meta"
    existing  = tmp.stat().st_size if tmp.exists() else 0
    resumable = existing > 0 and _range_ok(url)

    if existing > 0 and not resumable:
        print(f"  ⚠ No range support — restarting (discarding {fmt(existing)})")
        tmp.unlink(missing_ok=True)
        existing = 0

    hdrs: dict[str, str] = {}
    if resumable:
        hdrs["Range"] = f"bytes={existing}-"
        print(f"  ↻ Resuming from {fmt(existing)}")

    sess = make_session()
    resp = sess.get(url, stream=True, timeout=TIMEOUT,
                    headers=hdrs, allow_redirects=True)

    if resp.status_code == 416:
        print("  ⚠ 416 — restarting")
        resp.close()
        tmp.unlink(missing_ok=True)
        meta.unlink(missing_ok=True)
        return download_file(url, out, expected_md5)

    if resp.status_code not in (200, 206):
        resp.close()
        raise RuntimeError(f"HTTP {resp.status_code}")

    if resp.status_code == 206:
        cr = resp.headers.get("content-range", "")
        m  = re.search(r"bytes (\d+)-", cr)
        if m and int(m.group(1)) != existing:
            print("  ⚠ Wrong resume offset — restarting")
            resp.close()
            tmp.unlink(missing_ok=True)
            return download_file(url, out, expected_md5)

    cl         = int(resp.headers.get("content-length", 0))
    total      = (cl + existing) if resp.status_code == 206 else cl
    final_name = (
        fname_from_headers(resp)
        or (meta.read_text().strip() if meta.exists() else None)
        or fname_from_url(resp.url)
    )
    meta.write_text(final_name)
    dest = out / final_name

    print(f"  📥 {final_name}")
    print(f"  📦 {fmt(total) if total else 'unknown size'}")
    if total and total > MAX_FILE_MB * 1024 * 1024:
        print(f"  ⚠ File > {MAX_FILE_MB} MB — consider Git LFS")

    stall = Stall(STALL_TIMEOUT)
    seen_first = False

    with open(tmp, "ab" if existing else "wb") as fh, tqdm(
        total=total or None, initial=existing,
        unit="B", unit_scale=True, unit_divisor=1024,
        ascii=True, dynamic_ncols=True, miniters=1, desc="  ↓",
    ) as bar:
        try:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                if not seen_first:
                    seen_first = True
                    if is_html(chunk[:256]):
                        resp.close()
                        raise RuntimeError(
                            "Server sent HTML (CAPTCHA / error page) instead of file"
                        )
                fh.write(chunk)
                bar.update(len(chunk))
                stall.tick(len(chunk))
                stall.check()
        finally:
            resp.close()

    actual = tmp.stat().st_size
    if total and actual < total:
        raise IOError(f"Incomplete: {fmt(actual)} of {fmt(total)}")

    if expected_md5:
        print("  🔍 Verifying MD5...")
        got = md5_file(tmp)
        if got != expected_md5.lower():
            tmp.unlink(missing_ok=True)
            meta.unlink(missing_ok=True)
            raise IOError(f"MD5 mismatch: expected {expected_md5}, got {got}")
        print(f"  ✓ MD5 OK")
    else:
        print("  ℹ Skipping MD5 check (none provided)")

    if dest.exists():
        dest.unlink()
    tmp.rename(dest)
    meta.unlink(missing_ok=True)
    return dest


# ─────────────────────────────────────────── retry loop ───────────────────────

def _alt_urls(primary: str, md5: Optional[str]) -> list[str]:
    urls = [primary]
    t    = md5 or md5_from_url(primary)
    if t:
        for h in DOWNLOAD_HOSTS:
            c = f"{h}/main/{t}"
            if c not in urls:
                urls.append(c)
    return urls


def download(url: str, out: Path, md5: Optional[str] = None) -> Path:
    alts = _alt_urls(url, md5)
    last: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        current = url if attempt <= 3 else alts[(attempt - 4) % len(alts)]
        print(f"\n{'━' * 55}")
        print(f"  Attempt {attempt}/{MAX_RETRIES}  —  {current[:80]}")

        try:
            return download_file(current, out, md5)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.Timeout,
            TimeoutError,
            IOError,
        ) as e:
            last = e
            print(f"  ✗ {type(e).__name__}: {e}")
        except RuntimeError as e:
            last = e
            print(f"  ✗ {e}")
        except Exception as e:
            last = e
            print(f"  ✗ Unexpected {type(e).__name__}: {e}")

        if attempt < MAX_RETRIES:
            wait = min(BASE_BACKOFF * 2 ** (attempt - 1), MAX_BACKOFF)
            wait *= random.uniform(0.8, 1.2)
            print(f"  ⏳ {wait:.1f}s before retry...")
            time.sleep(wait)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed. Last: {last}")


# ─────────────────────────────────────────── main ─────────────────────────────

def main() -> int:
    book_url = os.environ.get("BOOK_URL", "").strip()
    raw_md5  = os.environ.get("EXPECTED_MD5", "").strip().lower()
    out_dir  = Path(os.environ.get("OUTPUT_DIR", "downloads").strip())

    if not book_url:
        print("💥 BOOK_URL is required", file=sys.stderr)
        return 1

    user_md5: Optional[str] = raw_md5 if is_md5(raw_md5) else None

    # ── cache check ───────────────────────────────────────────────────────────
    cache = load_cache()
    if book_url in cache:
        p = Path(cache[book_url]["path"])
        if p.exists():
            print(f"✓ Already downloaded: {p}  ({fmt(p.stat().st_size)})")
            return 0
        print("⚠ Cache stale — re-downloading")
        del cache[book_url]

    print("=" * 60)
    print(f"📚 Input : {book_url}")
    print(f"🔐 MD5   : {user_md5 or '(none supplied)'}")
    print(f"📁 OutDir: {out_dir.resolve()}")
    print("=" * 60)

    # ── resolve ───────────────────────────────────────────────────────────────
    try:
        dl_url, page_md5 = resolve(book_url)
    except Exception as e:
        print(f"💥 Resolution failed: {e}", file=sys.stderr)
        return 1

    print(f"  → {dl_url[:80]}")

    # Final MD5 to verify against (user > page > url)
    final_md5 = (
        user_md5
        or page_md5
        or (book_url.lower() if is_md5(book_url) else None)
        or md5_from_url(book_url)
        or md5_from_url(dl_url)
    )
    if final_md5:
        print(f"🔐 Will verify MD5: {final_md5}")

    # ── download ──────────────────────────────────────────────────────────────
    try:
        path = download(dl_url, out_dir, final_md5)
    except RuntimeError as e:
        print(f"\n💥 {e}", file=sys.stderr)
        return 1

    size     = path.stat().st_size
    checksum = md5_file(path)

    print(f"\n{'=' * 60}")
    print(f"✅ Done: {path}")
    print(f"   Size: {fmt(size)}")
    print(f"   MD5 : {checksum}")
    print(f"{'=' * 60}")

    cache[book_url] = {
        "path": str(path), "size": size, "md5": checksum,
        "download_url": dl_url, "ts": time.time(),
    }
    save_cache(cache)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⚠ Interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
