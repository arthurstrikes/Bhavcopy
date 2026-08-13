"""
Shared GitHub-backed file store.

Files are committed to a SEPARATE repo (GITHUB_REPO in secrets), never to the
app's own repo - a commit to the app repo triggers a Streamlit Cloud rebuild,
which would bounce the app mid-run on every upload.

An `index.json` manifest at the storage repo root lists every file with its
raw URL, tag, description, size and upload timestamp. That manifest has one
permanent URL and is the thing to hand to an assistant at the start of a
conversation - one paste instead of one per file.

Every failure path raises or returns an explicit reason. Nothing is skipped
silently.
"""

import base64
import io
import json
import mimetypes
import re
from datetime import datetime, timezone

import requests

API = "https://api.github.com"
MANIFEST_PATH = "index.json"

# GitHub's Contents API rejects blobs over ~100 MB. 95 MB leaves room for the
# base64 expansion in the request body.
MAX_BYTES = 95 * 1024 * 1024

TAGS = ["advice-log", "bhavcopy", "portfolio", "tradebook", "sof",
        "kb", "research", "report", "config", "misc"]

_TIMEOUT_READ = 30
_TIMEOUT_WRITE = 180


# ------------------------------------------------------------------ config

class ConfigError(RuntimeError):
    pass


def cfg(st):
    """Read storage config from Streamlit secrets. Raises ConfigError with a
    specific missing-key message rather than a generic failure."""
    try:
        s = st.secrets
    except Exception as e:
        raise ConfigError(f"Secrets unavailable: {e}")

    missing = [k for k in ("GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO")
               if k not in s]
    if missing:
        raise ConfigError("Missing secret(s): " + ", ".join(missing))

    return {
        "token": s["GITHUB_TOKEN"],
        "owner": s["GITHUB_OWNER"],
        "repo": s["GITHUB_REPO"],
        "branch": s.get("GITHUB_BRANCH", "main"),
        "folder": s.get("UPLOAD_FOLDER", "files"),
    }


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def raw_url(c, path):
    return (f"https://raw.githubusercontent.com/{c['owner']}/{c['repo']}"
            f"/{c['branch']}/{path}")


def manifest_url(c):
    return raw_url(c, MANIFEST_PATH)


def _api_error(e):
    """Turn an HTTPError into a specific, actionable message."""
    r = getattr(e, "response", None)
    if r is None:
        return str(e)
    try:
        msg = r.json().get("message", r.text[:200])
    except Exception:
        msg = r.text[:200]
    if r.status_code == 401:
        return f"401 - token rejected. Check GITHUB_TOKEN in Secrets. ({msg})"
    if r.status_code in (403, 429):
        if r.headers.get("x-ratelimit-remaining") == "0":
            reset = r.headers.get("x-ratelimit-reset", "")
            return (f"{r.status_code} - GitHub API rate limit reached. Wait and "
                    f"retry (resets at epoch {reset}).")
        return (f"{r.status_code} - token lacks permission. It needs 'Contents: "
                f"Read and write' on this repo. ({msg})")
    if r.status_code == 404:
        return (f"404 - repo or branch not found. Check GITHUB_OWNER / "
                f"GITHUB_REPO / GITHUB_BRANCH. ({msg})")
    if r.status_code == 409:
        return f"409 - conflicting write, retry. ({msg})"
    if r.status_code == 422:
        return f"422 - rejected by GitHub, often file too large. ({msg})"
    return f"{r.status_code} - {msg}"


# ------------------------------------------------------------------ raw io

def gh_get(c, path):
    """Return (bytes, sha). (None, None) if the path does not exist."""
    r = requests.get(f"{API}/repos/{c['owner']}/{c['repo']}/contents/{path}",
                     headers=_headers(c["token"]),
                     params={"ref": c["branch"]}, timeout=_TIMEOUT_READ)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    j = r.json()
    if isinstance(j, list):
        raise RuntimeError(f"{path} is a directory, not a file")
    if j.get("encoding") == "base64" and j.get("content"):
        return base64.b64decode(j["content"]), j["sha"]
    # Files over 1 MB come back with an empty content field; fall back to raw.
    raw = requests.get(raw_url(c, path), timeout=_TIMEOUT_WRITE)
    raw.raise_for_status()
    return raw.content, j["sha"]


def gh_put(c, path, data, message, sha=None):
    body = {"message": message,
            "content": base64.b64encode(data).decode(),
            "branch": c["branch"]}
    if sha:
        body["sha"] = sha
    r = requests.put(f"{API}/repos/{c['owner']}/{c['repo']}/contents/{path}",
                     headers=_headers(c["token"]), json=body,
                     timeout=_TIMEOUT_WRITE)
    r.raise_for_status()
    return r.json()


def gh_delete(c, path, sha, message):
    r = requests.delete(f"{API}/repos/{c['owner']}/{c['repo']}/contents/{path}",
                        headers=_headers(c["token"]),
                        json={"message": message, "sha": sha,
                              "branch": c["branch"]},
                        timeout=_TIMEOUT_READ)
    r.raise_for_status()


# ------------------------------------------------------------------ manifest

def _empty_manifest():
    return {"schema": 1, "updated": None, "files": []}


def load_manifest(c):
    """Return (manifest_dict, sha). Never raises on a malformed manifest -
    a corrupt index must not lock the user out of uploading."""
    try:
        data, sha = gh_get(c, MANIFEST_PATH)
    except requests.HTTPError:
        return _empty_manifest(), None
    if data is None:
        return _empty_manifest(), None
    try:
        m = json.loads(data.decode("utf-8"))
        if not isinstance(m.get("files"), list):
            raise ValueError
        return m, sha
    except Exception:
        return _empty_manifest(), sha


def save_manifest(c, manifest, sha):
    manifest["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifest["files"].sort(key=lambda f: f.get("uploaded", ""), reverse=True)
    blob = json.dumps(manifest, indent=2).encode("utf-8")
    for attempt in (0, 1):
        try:
            gh_put(c, MANIFEST_PATH, blob, "chore: update manifest", sha)
            return
        except requests.HTTPError as e:
            r = getattr(e, "response", None)
            if r is not None and r.status_code == 409 and attempt == 0:
                _, sha = load_manifest(c)   # someone else wrote; re-read sha
                continue
            raise


# ------------------------------------------------------------------ helpers

def slug(name):
    stem, _, ext = name.rpartition(".")
    if not stem:
        stem, ext = name, ""
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-.")[:80].lower() or "file"
    ext = re.sub(r"[^A-Za-z0-9]+", "", ext).lower()
    return f"{stem}.{ext}" if ext else stem


def human(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ------------------------------------------------------------------ publish

def publish(c, data, filename, tag="misc", description="",
            manifest=None, manifest_sha=None, commit_manifest=True):
    """Commit one file and register it in the manifest.

    Returns the manifest entry dict.
    Raises ValueError if the file exceeds MAX_BYTES, requests.HTTPError on a
    GitHub failure - callers must surface either to the user, never swallow.

    Pass manifest/manifest_sha and commit_manifest=False to batch several
    uploads into a single manifest commit.
    """
    if len(data) > MAX_BYTES:
        raise ValueError(f"{human(len(data))} exceeds the {human(MAX_BYTES)} limit")

    ts = datetime.now(timezone.utc)
    path = f"{c['folder']}/{ts.strftime('%Y%m%d-%H%M%S')}-{slug(filename)}"
    gh_put(c, path, data, f"add: {filename}")

    entry = {
        "name": filename,
        "path": path,
        "url": raw_url(c, path),
        "tag": tag,
        "description": (description or "").strip(),
        "size": len(data),
        "content_type": mimetypes.guess_type(filename)[0] or "application/octet-stream",
        "uploaded": ts.isoformat(timespec="seconds"),
    }

    if manifest is None:
        manifest, manifest_sha = load_manifest(c)
    manifest["files"].append(entry)
    if commit_manifest:
        save_manifest(c, manifest, manifest_sha)
    return entry


def remove(c, entry, manifest=None, manifest_sha=None):
    """Delete a stored file and drop it from the manifest."""
    if manifest is None:
        manifest, manifest_sha = load_manifest(c)
    try:
        _, sha = gh_get(c, entry["path"])
        if sha:
            gh_delete(c, entry["path"], sha, f"remove: {entry['name']}")
    except requests.HTTPError as e:
        r = getattr(e, "response", None)
        if r is None or r.status_code != 404:
            raise           # 404 = already gone, safe to drop from manifest
    manifest["files"] = [f for f in manifest["files"]
                         if f.get("path") != entry["path"]]
    save_manifest(c, manifest, manifest_sha)


# ------------------------------------------------------------------ consume

class StoredFile(io.BytesIO):
    """A dropbox file wrapped so it is indistinguishable from an
    st.file_uploader result. engine.load_log() only needs .name and .read()."""
    def __init__(self, data, name):
        super().__init__(data)
        self.name = name


def fetch(entry, timeout=120):
    """Download a manifest entry and return it as a file-like object.
    Raises requests.HTTPError with a specific reason on failure."""
    r = requests.get(entry["url"], timeout=timeout)
    r.raise_for_status()
    return StoredFile(r.content, entry["name"])


def list_files(c, tag=None):
    """Manifest entries, newest first, optionally filtered by tag."""
    m, _ = load_manifest(c)
    files = m.get("files", [])
    if tag:
        files = [f for f in files if f.get("tag") == tag]
    return files
