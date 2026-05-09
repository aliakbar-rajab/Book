#!/usr/bin/env python3
"""
Robust Libgen book downloader with proper resume, integrity checking,
mirror fallback, exponential backoff, and stall detection.
"""

import hashlib
import json
import os
import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

# ---------- Configuration ----------
CACHE_DIR = Path(".download_cache")
CACHE_FILE = CACHE_DIR / "downloads.json"
MAX_RETRIES = 20
CHUNK_SIZE = 1024 * 64           # 64 KB - smaller chunks = better resume granularity
TIMEOUT = (15, 60)                # (connect, read) timeout
STALL_TIMEOUT = 90                # seconds without data = stalled
BASE_BACKOFF = 4
MAX_BACKOFF = 120
MAX_FILE_SIZE_MB = 1900           # GitHub limit is 2GB; warn before that

# Libgen mirrors (in priority order)
LIBGEN_MIRRORS = [
    "https://libgen.is",
    "https://libgen.rs",
    "https://libgen.st",
    "https://libgen.li",
]

DOWNLOAD_HOSTS = [
    "https://library.lol",
    "https://libgen.li",
    "https://cdn1.booksdl.org",
    "https://cdn2.booksdl.org",
    "https://cdn3.booksdl.org",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CACHE_DIR.mkdir(exist_ok=True)


# ---------- Session ----------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    # urllib3 retries for connection-level errors
    retry = Retry(
        total=5,
        connect=5,
        read=3,
        backoff_factor=1.5,
        status_forcelist=(500, 502, 503, 504, 429),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = make_session()


# ---------- Cache ----------
def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=2))


# ---------- Utilities ----------
def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" ._")
    # Limit length to avoid filesystem issues
    if len(name) > 200:
        stem, ext = os.path.splitext(name)
        name = stem[:200 - len(ext)] + ext
    return name or f"book_{int(time.time())}"


def format_size(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def format_eta(seconds: float) -> str:
    if seconds <= 0 or seconds == float("inf"):
        return "?"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m}m"
    if m:
        return f"{m}m{s}s"
    return f"{s}s"


def md5_hash_file(path: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest().lower()


def is_md5_hash(s: str) -> bool:
    return bool(re.fullmatch(r"[a-fA-F0-9]{32}", s.strip()))


def filename_from_headers(resp: requests.Response) -> str | None:
    cd = resp.headers.get("content-disposition", "")
    # RFC 5987 filename*=
    m = re.search(r"filename\*=(?:UTF-8''|)([^;]+)", cd, re.IGNORECASE)
    if m:
        return sanitize_filename(unquote(m.group(1).strip().strip('"')))
    m = re.search(r'filename="?([^";]+)"?', cd, re.IGNORECASE)
    if m:
        return sanitize_filename(unquote(m.group(1).strip()))
    return None


def filename_from_url(url: str, fallback_ext: str = "epub") -> str:
    path = unquote(urlparse(url).path)
    name = os.path.basename(path)
    if not name or "." not in name:
        return f"book_{int(time.time())}.{fallback_ext}"
    return sanitize_filename(name)


def looks_like_binary(content_type: str, first_bytes: bytes) -> bool:
    ct = (content_type or "").lower()
    binary_types = (
        "application/epub", "application/pdf", "application/octet-stream",
        "application/x-mobipocket", "application/zip", "application/x-mobi8",
        "application/vnd.amazon.ebook", "application/x-fictionbook",
        "image/vnd.djvu",
    )
    if any(b in ct for b in binary_types):
        return True
    # Magic bytes
    magics = (b"PK", b"%PDF", b"AT&TFORM", b"\xefBOOKMOBI", b"BOOKMOBI")
    return any(first_bytes.startswith(m) or m in first_bytes[:32] for m in magics)


# ---------- Link resolution ----------
def resolve_md5_to_page(md5: str) -> str:
    """Try Libgen mirrors to find a working book page for an MD5 hash."""
    md5 = md5.lower().strip()
    for mirror in LIBGEN_MIRRORS:
        url = f"{mirror}/ads.php?md5={md5}"
        try:
            r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                print(f"  ✓ Mirror reachable: {mirror}")
                return url
        except requests.RequestException as e:
            print(f"  ✗ {mirror}: {e}")
    # Fallback to library.lol
    return f"https://library.lol/main/{md5}"


def find_download_link(url: str, depth: int = 0) -> tuple[str, str | None]:
    """
    Returns (download_url, expected_md5_if_known).
    Follows intermediate pages to find a real downloadable file.
    """
    if depth > 4:
        raise RuntimeError("Too many redirects while finding download link")

    print(f"→ Resolving: {url}")

    # If user provided just an MD5
    if is_md5_hash(url):
        url = resolve_md5_to_page(url)

    # Try a HEAD-like check via streamed GET
    try:
        with SESSION.get(url, timeout=TIMEOUT, stream=True,
                         allow_redirects=True) as r:
            r.raise_for_status()
            ct = r.headers.get("content-type", "")
            # Peek without consuming the stream irrevocably
            first = b""
            try:
                first = next(r.iter_content(32), b"")
            except StopIteration:
                first = b""

            if looks_like_binary(ct, first):
                print("  ✓ Direct file detected")
                # Extract MD5 from URL if present
                md5_match = re.search(r"([a-fA-F0-9]{32})", url)
                return r.url, (md5_match.group(1).lower() if md5_match else None)
    except requests.RequestException as e:
        print(f"  ! Probe failed: {e}")

    # Fetch as HTML and parse
    r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Try to extract the MD5 hash mentioned on the page (for integrity check)
    expected_md5 = None
    md5_match = re.search(r"\b([a-fA-F0-9]{32})\b", r.text)
    if md5_match:
        expected_md5 = md5_match.group(1).lower()

    # Priority 1: GET button (library.lol style)
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().upper()
        href = a["href"]
        if text in ("GET", "DOWNLOAD") or "get.php" in href.lower():
            final = urljoin(url, href)
            print(f"  ✓ Found GET link: {final[:80]}")
            return final, expected_md5

    # Priority 2: Direct file extension links
    file_exts = (".epub", ".pdf", ".mobi", ".azw3", ".djvu", ".fb2", ".cbz", ".cbr")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(href.lower().split("?")[0].endswith(ext) for ext in file_exts):
            final = urljoin(url, href)
            print(f"  ✓ Found extension link: {final[:80]}")
            return final, expected_md5

    # Priority 3: Cloudflare/cdn links
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if any(host in href for host in
               ("cloudflare-ipfs", "ipfs.io", "booksdl", "library.lol")):
            final = urljoin(url, a["href"])
            print(f"  ✓ Found CDN link: {final[:80]}")
            return final, expected_md5

    # Priority 4: regex through raw text
    for m in re.findall(r'https?://[^\s"\'<>]+', r.text):
        ml = m.lower().split("?")[0]
        if any(ml.endswith(ext) for ext in file_exts) or "/get.php" in m.lower():
            print(f"  ✓ Found regex link: {m[:80]}")
            return m, expected_md5

    raise RuntimeError(f"No download link found at {url}")


# ---------- Download with proper resume ----------
class StallDetector:
    """Raises an exception if no progress for `timeout` seconds."""
    def __init__(self, timeout: int):
        self.timeout = timeout
        self.last_progress = time.time()

    def tick(self):
        self.last_progress = time.time()

    def check(self):
        if time.time() - self.last_progress > self.timeout:
            raise TimeoutError(f"No data received for {self.timeout}s (stalled)")


def server_supports_range(url: str) -> bool:
    """HEAD request to see if the server supports byte-range resume."""
    try:
        r = SESSION.head(url, timeout=TIMEOUT, allow_redirects=True)
        accept_ranges = r.headers.get("accept-ranges", "").lower()
        return accept_ranges == "bytes"
    except requests.RequestException:
        return False


def download_with_resume(url: str, output_dir: Path,
                          known_filename: str | None = None) -> Path:
    """
    Download with reliable resume support.
    Uses a stable temp filename so resume works even if server returns
    a different content-disposition on a later attempt.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use URL hash as a stable identifier for the .part file
    url_id = hashlib.sha1(url.encode()).hexdigest()[:16]
    temp_file = output_dir / f".{url_id}.part"
    meta_file = output_dir / f".{url_id}.meta"

    existing_size = temp_file.stat().st_size if temp_file.exists() else 0
    can_resume = existing_size > 0 and server_supports_range(url)

    if existing_size > 0 and not can_resume:
        print(f"  ⚠ Server doesn't support resume; restarting "
              f"({format_size(existing_size)} discarded)")
        temp_file.unlink()
        existing_size = 0

    headers = {}
    if can_resume:
        headers["Range"] = f"bytes={existing_size}-"
        print(f"  ↻ Resuming from {format_size(existing_size)}")

    response = SESSION.get(url, stream=True, timeout=TIMEOUT,
                            headers=headers, allow_redirects=True)

    if response.status_code == 416:
        # Range Not Satisfiable - file is complete on server, our part is bigger
        print("  ⚠ Got 416 - restarting download")
        response.close()
        temp_file.unlink(missing_ok=True)
        return download_with_resume(url, output_dir, known_filename)

    if response.status_code not in (200, 206):
        response.close()
        raise RuntimeError(f"Unexpected HTTP {response.status_code}")

    # Validate Content-Range on resume
    if response.status_code == 206:
        content_range = response.headers.get("content-range", "")
        m = re.search(r"bytes (\d+)-\d+/(\d+|\*)", content_range)
        if m and int(m.group(1)) != existing_size:
            print(f"  ⚠ Server resumed at wrong offset, restarting")
            response.close()
            temp_file.unlink(missing_ok=True)
            return download_with_resume(url, output_dir, known_filename)

    total_size = int(response.headers.get("content-length", 0))
    if response.status_code == 206:
        total_size += existing_size

    # Determine final filename (prefer header, then meta cache, then URL)
    server_name = filename_from_headers(response)
    if server_name:
        final_name = server_name
    elif meta_file.exists():
        final_name = meta_file.read_text().strip()
    elif known_filename:
        final_name = known_filename
    else:
        final_name = filename_from_url(response.url)

    # Persist filename so resume across runs uses same name
    meta_file.write_text(final_name)

    final_path = output_dir / final_name
    print(f"  📥 → {final_name}")
    print(f"  📦 Total: {format_size(total_size) if total_size else 'unknown'}")

    if total_size and total_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        print(f"  ⚠ WARNING: file > {MAX_FILE_SIZE_MB}MB; Git LFS recommended")

    mode = "ab" if existing_size > 0 else "wb"
    stall = StallDetector(STALL_TIMEOUT)

    with open(temp_file, mode) as f, tqdm(
        total=total_size or None,
        initial=existing_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        ascii=True,
        dynamic_ncols=True,
        miniters=1,
    ) as bar:
        try:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
                    stall.tick()
                stall.check()
        finally:
            response.close()

    actual_size = temp_file.stat().st_size
    if total_size and actual_size < total_size:
        raise IOError(
            f"Incomplete: got {format_size(actual_size)} of "
            f"{format_size(total_size)}"
        )

    # Atomic move into place
    if final_path.exists():
        final_path.unlink()
    temp_file.rename(final_path)
    meta_file.unlink(missing_ok=True)

    return final_path


# ---------- Orchestration ----------
def download_with_retries(url: str, output_dir: Path,
                           expected_md5: str | None = None) -> Path:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n━━━ Attempt {attempt}/{MAX_RETRIES} ━━━")
        try:
            path = download_with_resume(url, output_dir)

            # Integrity check (per github.com issue #77)
            if expected_md5:
                print(f"  🔍 Verifying MD5 ({expected_md5})...")
                actual_md5 = md5_hash_file(path)
                if actual_md5 != expected_md5.lower():
                    print(f"  ✗ MD5 MISMATCH: got {actual_md5}")
                    path.unlink()
                    # Remove partial cache too
                    raise IOError("Integrity check failed")
                print(f"  ✓ MD5 verified")
            else:
                print("  ⚠ No expected MD5 - skipping integrity check")

            return path

        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.Timeout,
                TimeoutError,
                IOError) as e:
            last_error = e
            print(f"  ✗ Network/IO error: {e}")
        except Exception as e:
            last_error = e
            print(f"  ✗ Error: {type(e).__name__}: {e}")

        if attempt < MAX_RETRIES:
            # Exponential backoff with jitter
            backoff = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
            jitter = random.uniform(0, backoff * 0.3)
            wait = backoff + jitter
            print(f"  ⏳ Waiting {wait:.1f}s before retry...")
            time.sleep(wait)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed. Last: {last_error}")


def main() -> int:
    book_url = os.environ["BOOK_URL"].strip()
    expected_md5 = os.environ.get("EXPECTED_MD5", "").strip().lower() or None
    output_dir = Path(os.environ.get("OUTPUT_DIR", "downloads").strip())

    cache = load_cache()
    cache_key = book_url

    if cache_key in cache:
        cached = cache[cache_key]
        cached_path = Path(cached["path"])
        if cached_path.exists():
            print(f"✓ Already downloaded: {cached_path}")
            print(f"  Size: {format_size(cached_path.stat().st_size)}")
            return 0
        else:
            print("⚠ Cached entry exists but file missing; re-downloading")
            del cache[cache_key]

    print(f"📚 Book URL: {book_url}")
    if expected_md5:
        print(f"🔐 Expected MD5: {expected_md5}")
    print(f"📁 Output dir: {output_dir.absolute()}")

    # Resolve to actual download URL
    download_url, page_md5 = find_download_link(book_url)
    if expected_md5 is None and page_md5:
        expected_md5 = page_md5
        print(f"🔐 MD5 from page: {expected_md5}")
    # If user provided MD5 directly, use it
    if expected_md5 is None and is_md5_hash(book_url):
        expected_md5 = book_url.lower()

    final_path = download_with_retries(download_url, output_dir, expected_md5)

    final_size = final_path.stat().st_size
    print(f"\n✅ SUCCESS")
    print(f"   File: {final_path}")
    print(f"   Size: {format_size(final_size)}")

    cache[cache_key] = {
        "path": str(final_path),
        "size": final_size,
        "md5": expected_md5 or md5_hash_file(final_path),
        "completed_at": time.time(),
    }
    save_cache(cache)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        sys.exit(1)
