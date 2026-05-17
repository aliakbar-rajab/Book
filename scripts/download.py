#!/usr/bin/env python3
"""
Robust Libgen book downloader.

Improvements over v1:
- Parallel mirror probing (faster mirror selection)
- Accurate MD5 extraction from Libgen's own metadata table
- Mirror fallback on download failure (not just on link resolution)
- Smarter stall detection that doesn't false-positive on slow starts
- Better filename extraction with more fallback layers
- Content validation (rejects HTML error pages saved as .epub etc.)
- Connection pooling tuned for single-host heavy use
- Cleaner separation of concerns
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
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# ─────────────────────────── Configuration ────────────────────────────────────

CACHE_DIR  = Path(".download_cache")
CACHE_FILE = CACHE_DIR / "downloads.json"

MAX_RETRIES      = 20
CHUNK_SIZE       = 256 * 1024   # 256 KB — good balance of RAM and progress granularity
TIMEOUT          = (15, 90)     # (connect timeout, read timeout)
STALL_TIMEOUT    = 120          # seconds with zero bytes = stalled connection
BASE_BACKOFF     = 3
MAX_BACKOFF      = 90
MAX_FILE_SIZE_MB = 1900         # Warn at 1.9 GB (GitHub's hard limit is 2 GB)

# Libgen mirror list — ordered by reliability, probed in parallel
LIBGEN_MIRRORS: list[str] = [
    "https://libgen.is",
    "https://libgen.rs",
    "https://libgen.st",
    "https://libgen.li",
    "https://libgen.vip",
]

# Hosts known to serve actual file bytes
DOWNLOAD_HOSTS: list[str] = [
    "https://library.lol",
    "https://libgen.li",
    "https://cdn1.booksdl.org",
    "https://cdn2.booksdl.org",
    "https://cdn3.booksdl.org",
]

USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

# File extensions we consider valid book formats
BOOK_EXTENSIONS = (
    ".epub", ".pdf", ".mobi", ".azw3", ".azw",
    ".djvu", ".fb2", ".cbz", ".cbr", ".txt", ".lit", ".lrf",
)

# Magic bytes → definitely a binary book file
MAGIC_BYTES: list[tuple[bytes, str]] = [
    (b"PK\x03\x04",   "epub/zip"),
    (b"%PDF",          "pdf"),
    (b"AT&TFORM",      "djvu"),
    (b"\x00\x00\x00", "mobi/azw"),   # broad match; refined below
    (b"BOOKMOBI",      "mobi"),
    (b"\xefBOOKMOBI",  "mobi"),
]

CACHE_DIR.mkdir(exist_ok=True)


# ─────────────────────────── Session ──────────────────────────────────────────

def _make_retry() -> Retry:
    return Retry(
        total=6,
        connect=6,
        read=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )


def make_session(rotate_ua: bool = True) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":      random.choice(USER_AGENTS) if rotate_ua else USER_AGENTS[0],
        "Accept":          "text/html,application/xhtml+xml,application/xml;"
                           "q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "DNT":             "1",
    })
    adapter = HTTPAdapter(
        max_retries=_make_retry(),
        pool_connections=4,
        pool_maxsize=8,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s


# One shared session for HTML fetching; separate sessions per download thread
SESSION = make_session()


# ─────────────────────────── Cache ────────────────────────────────────────────

def load_cache() -> dict:
    try:
        return json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(cache: dict) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(cache, indent=2))
    except OSError as e:
        print(f"  ⚠ Could not save cache: {e}")


# ─────────────────────────── Utilities ────────────────────────────────────────

def sanitize_filename(name: str) -> str:
    """Remove filesystem-unsafe characters and truncate to 200 chars."""
    name = unquote(name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" ._")
    if len(name) > 200:
        stem, ext = os.path.splitext(name)
        name = stem[: 200 - len(ext)] + ext
    return name or f"book_{int(time.time())}"


def format_size(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def md5_of_file(path: Path, block: int = 4 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def is_md5(s: str) -> bool:
    return bool(re.fullmatch(r"[a-fA-F0-9]{32}", s.strip()))


def extract_md5_from_url(url: str) -> Optional[str]:
    """Pull an MD5 hash out of a URL path or query string."""
    m = re.search(r"[?&/]([a-fA-F0-9]{32})(?:[&/?]|$)", url)
    if m:
        return m.group(1).lower()
    # Some mirrors put it as the bare path segment
    parts = urlparse(url).path.split("/")
    for part in parts:
        if is_md5(part):
            return part.lower()
    return None


def filename_from_headers(resp: requests.Response) -> Optional[str]:
    cd = resp.headers.get("content-disposition", "")
    # RFC 5987  filename*=UTF-8''...
    m = re.search(r"filename\*\s*=\s*(?:UTF-8''|)([^;]+)", cd, re.IGNORECASE)
    if m:
        return sanitize_filename(m.group(1).strip().strip('"\''))
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
    if m:
        return sanitize_filename(m.group(1).strip())
    return None


def filename_from_url(url: str) -> str:
    path = unquote(urlparse(url).path)
    name = os.path.basename(path)
    if name and "." in name:
        return sanitize_filename(name)
    return f"book_{int(time.time())}.epub"


def sniff_content(content_type: str, first_bytes: bytes) -> bool:
    """Return True if the response looks like an actual binary book file."""
    ct = content_type.lower()
    binary_ct = (
        "application/epub", "application/pdf", "application/octet-stream",
        "application/zip", "application/x-mobipocket", "application/x-mobi8",
        "application/vnd.amazon", "application/x-fictionbook",
        "image/vnd.djvu", "application/x-cbz", "application/x-cbr",
    )
    if any(b in ct for b in binary_ct):
        return True
    for magic, _ in MAGIC_BYTES:
        if first_bytes[:len(magic)] == magic or magic in first_bytes[:64]:
            return True
    return False


def content_looks_like_error_page(first_bytes: bytes) -> bool:
    """Return True if we're about to save an HTML error as a book file."""
    snippet = first_bytes[:512].lower()
    html_markers = (b"<!doctype html", b"<html", b"<head>", b"<title>")
    return any(m in snippet for m in html_markers)


# ─────────────────────────── Mirror probing ───────────────────────────────────

def probe_mirror(mirror: str, md5: str) -> Optional[str]:
    """Return mirror URL if it responds with a valid Libgen page, else None."""
    url = f"{mirror}/ads.php?md5={md5}"
    try:
        r = SESSION.get(url, timeout=(8, 15), allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 400:
            return url
    except requests.RequestException:
        pass
    return None


def fastest_mirror(md5: str) -> Optional[str]:
    """Probe all mirrors in parallel; return the first one that responds."""
    print("  🔍 Probing mirrors in parallel...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(LIBGEN_MIRRORS)) as ex:
        futures = {ex.submit(probe_mirror, m, md5): m for m in LIBGEN_MIRRORS}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                mirror = futures[future]
                print(f"  ✓ Fastest mirror: {mirror}")
                # Cancel remaining (best-effort)
                for f in futures:
                    f.cancel()
                return result
    return None


# ─────────────────────────── Link resolution ──────────────────────────────────

def _extract_md5_from_libgen_page(soup: BeautifulSoup, html: str) -> Optional[str]:
    """
    Libgen pages contain the MD5 in a metadata table.
    Try structured extraction first, regex as fallback.
    """
    # Structured: look for a table cell/link whose text is exactly 32 hex chars
    for tag in soup.find_all(["td", "th", "a", "span"]):
        text = (tag.get_text() or "").strip()
        if is_md5(text):
            return text.lower()
    # Regex fallback over raw HTML (avoid matching random hex in JS/CSS)
    for m in re.finditer(r'(?:md5|MD5)[=: "\']+([a-fA-F0-9]{32})', html):
        return m.group(1).lower()
    return None


def _find_link_in_soup(
    soup: BeautifulSoup,
    base_url: str,
    html: str,
) -> Optional[str]:
    """
    Multi-strategy link extraction from an intermediate Libgen page.
    Returns the best download URL found, or None.
    """
    # Strategy 1 — explicit GET / DOWNLOAD button
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).upper()
        href = a["href"]
        if text in {"GET", "DOWNLOAD", "TÉLÉCHARGER", "DESCARGAR"}:
            return urljoin(base_url, href)
        if "get.php" in href.lower():
            return urljoin(base_url, href)

    # Strategy 2 — href ends with a known book extension
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].lower()
        if any(href.endswith(ext) for ext in BOOK_EXTENSIONS):
            return urljoin(base_url, a["href"])

    # Strategy 3 — CDN / IPFS / booksdl links anywhere in href
    cdn_keywords = ("cloudflare-ipfs", "ipfs.io", "booksdl", "library.lol", "/main/")
    for a in soup.find_all("a", href=True):
        if any(kw in a["href"].lower() for kw in cdn_keywords):
            return urljoin(base_url, a["href"])

    # Strategy 4 — regex over raw HTML
    for m in re.findall(r'https?://[^\s"\'<>]+', html):
        ml = m.lower().split("?")[0]
        if any(ml.endswith(ext) for ext in BOOK_EXTENSIONS):
            return m
        if "/get.php" in m.lower() or "booksdl" in m.lower():
            return m

    return None


def find_download_link(
    url: str,
    depth: int = 0,
) -> tuple[str, Optional[str]]:
    """
    Resolve any Libgen-related URL / MD5 hash to a direct download URL.
    Returns (download_url, md5_or_none).
    """
    if depth > 5:
        raise RuntimeError("Redirect depth exceeded while resolving download link")

    print(f"→ Resolving: {url[:100]}")

    # Bare MD5 hash → find fastest mirror page
    if is_md5(url):
        md5 = url.strip().lower()
        page_url = fastest_mirror(md5)
        if page_url is None:
            # All mirrors failed — try library.lol directly
            page_url = f"https://library.lol/main/{md5}"
            print(f"  ⚠ All mirrors failed, trying {page_url}")
        return find_download_link(page_url, depth + 1)

    # Quick probe: is this URL already a direct file?
    try:
        with SESSION.get(
            url, timeout=TIMEOUT, stream=True, allow_redirects=True
        ) as r:
            if r.status_code == 200:
                ct = r.headers.get("content-type", "")
                first = next(r.iter_content(256), b"")
                if sniff_content(ct, first) and not content_looks_like_error_page(first):
                    print("  ✓ Direct file URL detected")
                    md5 = extract_md5_from_url(r.url)
                    return r.url, md5
    except requests.RequestException as e:
        print(f"  ⚠ Probe failed: {e}")

    # Fetch the page as HTML
    try:
        r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch page {url}: {e}") from e

    soup = BeautifulSoup(r.text, "lxml")
    page_md5 = _extract_md5_from_libgen_page(soup, r.text)

    link = _find_link_in_soup(soup, r.url, r.text)
    if link is None:
        raise RuntimeError(f"No download link found at {url}")

    print(f"  ✓ Found link: {link[:100]}")
    return link, page_md5


# ─────────────────────────── Stall detection ──────────────────────────────────

class StallDetector:
    """
    Raises TimeoutError if no bytes are received for `timeout` seconds.
    Includes a grace period for slow-starting connections.
    """
    def __init__(self, timeout: int, grace: int = 30):
        self.timeout = timeout
        self.grace = grace
        self._start = time.monotonic()
        self._last_byte = time.monotonic()
        self._total_received = 0

    def tick(self, nbytes: int) -> None:
        self._total_received += nbytes
        self._last_byte = time.monotonic()

    def check(self) -> None:
        elapsed_since_byte = time.monotonic() - self._last_byte
        # Only enforce stall timeout after grace period
        elapsed_total = time.monotonic() - self._start
        threshold = self.timeout if elapsed_total > self.grace else self.timeout * 2
        if elapsed_since_byte > threshold:
            raise TimeoutError(
                f"No data for {elapsed_since_byte:.0f}s (stalled; "
                f"received {format_size(self._total_received)} total)"
            )


# ─────────────────────────── Core download ────────────────────────────────────

def _check_range_support(url: str) -> bool:
    try:
        r = SESSION.head(url, timeout=(8, 10), allow_redirects=True)
        return r.headers.get("accept-ranges", "").lower() == "bytes"
    except requests.RequestException:
        return False


def download_file(
    url: str,
    output_dir: Path,
    expected_md5: Optional[str] = None,
) -> Path:
    """
    Download `url` into `output_dir` with resume support.

    - Uses a URL-keyed .part file so resume works across process restarts.
    - Validates integrity with MD5 if `expected_md5` is provided.
    - Rejects HTML error pages masquerading as book files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    url_id    = hashlib.sha1(url.encode()).hexdigest()[:16]
    temp_file = output_dir / f".{url_id}.part"
    meta_file = output_dir / f".{url_id}.meta"

    existing  = temp_file.stat().st_size if temp_file.exists() else 0
    can_resume = existing > 0 and _check_range_support(url)

    if existing > 0 and not can_resume:
        print(f"  ⚠ Server doesn't support range requests; discarding "
              f"{format_size(existing)} and restarting")
        temp_file.unlink(missing_ok=True)
        existing = 0

    headers: dict[str, str] = {}
    if can_resume:
        headers["Range"] = f"bytes={existing}-"
        print(f"  ↻ Resuming from {format_size(existing)}")

    sess = make_session()  # fresh session per attempt
    resp = sess.get(url, stream=True, timeout=TIMEOUT,
                    headers=headers, allow_redirects=True)

    if resp.status_code == 416:
        print("  ⚠ 416 Range Not Satisfiable — restarting cleanly")
        resp.close()
        temp_file.unlink(missing_ok=True)
        meta_file.unlink(missing_ok=True)
        return download_file(url, output_dir, expected_md5)

    if resp.status_code not in (200, 206):
        resp.close()
        raise RuntimeError(f"HTTP {resp.status_code} from {url}")

    # Validate resume offset
    if resp.status_code == 206:
        cr = resp.headers.get("content-range", "")
        m = re.search(r"bytes (\d+)-\d+/(\d+|\*)", cr)
        if m and int(m.group(1)) != existing:
            print("  ⚠ Server resumed at wrong offset — restarting")
            resp.close()
            temp_file.unlink(missing_ok=True)
            return download_file(url, output_dir, expected_md5)

    content_length = int(resp.headers.get("content-length", 0))
    total_size     = (content_length + existing) if resp.status_code == 206 \
                     else content_length

    # Resolve filename
    final_name = (
        filename_from_headers(resp)
        or (meta_file.read_text().strip() if meta_file.exists() else None)
        or filename_from_url(resp.url)
    )
    meta_file.write_text(final_name)
    final_path = output_dir / final_name

    print(f"  📥 {final_name}")
    print(f"  📦 {format_size(total_size) if total_size else 'unknown size'}")

    if total_size and total_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        print(f"  ⚠ File exceeds {MAX_FILE_SIZE_MB} MB — consider Git LFS")

    stall   = StallDetector(STALL_TIMEOUT)
    mode    = "ab" if existing else "wb"
    first_chunk_checked = False

    with open(temp_file, mode) as fh, tqdm(
        total      = total_size or None,
        initial    = existing,
        unit       = "B",
        unit_scale = True,
        unit_divisor = 1024,
        ascii      = True,
        dynamic_ncols = True,
        miniters   = 1,
        desc       = "  ↓",
    ) as bar:
        try:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue

                # Validate first chunk isn't an HTML error page
                if not first_chunk_checked:
                    first_chunk_checked = True
                    if content_looks_like_error_page(chunk[:512]):
                        resp.close()
                        raise RuntimeError(
                            "Server returned an HTML page instead of a file "
                            "(likely a CAPTCHA or error page)"
                        )

                fh.write(chunk)
                bar.update(len(chunk))
                stall.tick(len(chunk))
                stall.check()
        finally:
            resp.close()

    # Size sanity check
    actual = temp_file.stat().st_size
    if total_size and actual < total_size:
        raise IOError(
            f"Incomplete download: {format_size(actual)} of "
            f"{format_size(total_size)}"
        )

    # MD5 integrity check
    if expected_md5:
        print(f"  🔍 Verifying MD5...")
        actual_md5 = md5_of_file(temp_file)
        if actual_md5 != expected_md5.lower():
            temp_file.unlink(missing_ok=True)
            meta_file.unlink(missing_ok=True)
            raise IOError(
                f"MD5 mismatch: expected {expected_md5}, got {actual_md5}"
            )
        print(f"  ✓ MD5 OK: {actual_md5}")
    else:
        print("  ℹ No expected MD5 supplied — skipping integrity check")

    # Atomic rename into place
    if final_path.exists():
        final_path.unlink()
    temp_file.rename(final_path)
    meta_file.unlink(missing_ok=True)

    return final_path


# ─────────────────────────── Retry orchestration ──────────────────────────────

def download_with_retries(
    url: str,
    output_dir: Path,
    expected_md5: Optional[str] = None,
) -> Path:
    """
    Attempt download up to MAX_RETRIES times with exponential back-off.
    On repeated failure, also tries alternate download hosts if the URL
    contains a recognisable MD5 we can re-derive.
    """
    last_error: Optional[Exception] = None
    alt_urls   = _build_alt_urls(url, expected_md5)

    for attempt in range(1, MAX_RETRIES + 1):
        # Cycle through alternate URLs after the first few failures
        current_url = url if attempt <= 3 else alt_urls[(attempt - 4) % len(alt_urls)]

        print(f"\n{'━' * 50}")
        print(f"  Attempt {attempt}/{MAX_RETRIES}  →  {current_url[:80]}")

        try:
            path = download_file(current_url, output_dir, expected_md5)
            return path

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.Timeout,
            TimeoutError,
            IOError,
        ) as e:
            last_error = e
            print(f"  ✗ Network/IO: {e}")
        except RuntimeError as e:
            last_error = e
            print(f"  ✗ Runtime: {e}")
            # HTML error page — rotating the URL is the best remedy
        except Exception as e:
            last_error = e
            print(f"  ✗ Unexpected {type(e).__name__}: {e}")

        if attempt < MAX_RETRIES:
            wait = min(BASE_BACKOFF * 2 ** (attempt - 1), MAX_BACKOFF)
            wait *= random.uniform(0.8, 1.2)   # ±20 % jitter
            print(f"  ⏳ Back-off {wait:.1f}s…")
            time.sleep(wait)

    raise RuntimeError(
        f"All {MAX_RETRIES} attempts failed. Last error: {last_error}"
    )


def _build_alt_urls(primary_url: str, md5: Optional[str]) -> list[str]:
    """
    Build alternate download URLs from different hosts for the same file.
    """
    alts = [primary_url]
    target_md5 = md5 or extract_md5_from_url(primary_url)
    if target_md5:
        for host in DOWNLOAD_HOSTS:
            alts.append(f"{host}/main/{target_md5}")
        # library.lol specific pattern
        alts.append(f"https://library.lol/main/{target_md5}")
    return alts


# ─────────────────────────── Entry point ──────────────────────────────────────

def main() -> int:
    book_url    = os.environ.get("BOOK_URL", "").strip()
    raw_md5     = os.environ.get("EXPECTED_MD5", "").strip().lower()
    output_dir  = Path(os.environ.get("OUTPUT_DIR", "downloads").strip())

    if not book_url:
        print("💥 BOOK_URL environment variable is required", file=sys.stderr)
        return 1

    expected_md5: Optional[str] = raw_md5 if is_md5(raw_md5) else None

    cache     = load_cache()
    cache_key = book_url

    # ── Cache hit ──
    if cache_key in cache:
        entry      = cache[cache_key]
        cached_path = Path(entry["path"])
        if cached_path.exists():
            print(f"✓ Already downloaded: {cached_path}")
            print(f"  Size : {format_size(cached_path.stat().st_size)}")
            print(f"  MD5  : {entry.get('md5', 'unknown')}")
            return 0
        print("⚠ Cached path missing from disk — re-downloading")
        del cache[cache_key]

    # ── Header ──
    print("=" * 60)
    print(f"📚 Book URL    : {book_url}")
    print(f"🔐 Expected MD5: {expected_md5 or '(none)'}")
    print(f"📁 Output dir  : {output_dir.resolve()}")
    print("=" * 60)

    # ── Resolve to direct download URL ──
    try:
        download_url, page_md5 = find_download_link(book_url)
    except Exception as e:
        print(f"💥 Could not resolve download link: {e}", file=sys.stderr)
        return 1

    # Prefer user-supplied MD5, then page-extracted, then MD5 in original URL
    if expected_md5 is None:
        expected_md5 = (
            page_md5
            or (book_url.strip().lower() if is_md5(book_url) else None)
            or extract_md5_from_url(book_url)
        )
        if expected_md5:
            print(f"🔐 MD5 from page : {expected_md5}")

    # ── Download ──
    try:
        final_path = download_with_retries(download_url, output_dir, expected_md5)
    except RuntimeError as e:
        print(f"\n💥 Download failed: {e}", file=sys.stderr)
        return 1

    final_size = final_path.stat().st_size
    final_md5  = md5_of_file(final_path)

    print(f"\n{'=' * 60}")
    print(f"✅ SUCCESS")
    print(f"   File : {final_path}")
    print(f"   Size : {format_size(final_size)}")
    print(f"   MD5  : {final_md5}")
    print(f"{'=' * 60}")

    # ── Update cache ──
    cache[cache_key] = {
        "path":         str(final_path),
        "size":         final_size,
        "md5":          final_md5,
        "download_url": download_url,
        "completed_at": time.time(),
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
        print(f"\n💥 Unhandled error: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
