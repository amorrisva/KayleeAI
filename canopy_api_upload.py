"""
CanopyRouter Phase 2 -- Upload files via Canopy's local sync daemon API.

The Canopy sync daemon runs on localhost:42456 and accepts REST API calls.
Paths are base64-encoded. This bypasses the interactive gateway shell entirely.

Usage:
    python canopy_api_upload.py                   # dry-run
    python canopy_api_upload.py --go              # upload all files
    python canopy_api_upload.py --go --start 5    # start from file #5
"""

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_config,
    find_mapping_csv,
    load_canopy_mapping,
    parse_filename,
)

SYNC_DAEMON_URL = "http://127.0.0.1:42456/sync/v2"
INSTALL_TOKEN = "1894cf3c-4bc6-410f-8daa-871b00aae652"
DAEMON_MOUNT_JSON = r"C:\Users\Administrator\AppData\Local\canopy\Sync Dist\sync_daemon\mount.json"
# Fallback paths to find mount.json
MOUNT_SEARCH_PATHS = [
    DAEMON_MOUNT_JSON,
    r"C:\Users\Administrator\AppData\Local\canopy\Sync Dist\gateway_shell\mount.json",
]

_auth_token = None


def authenticate():
    """Authenticate with the sync daemon using the gateway shell's mount config."""
    global _auth_token

    # The gateway shell POSTs its mount config to establish a session
    mount_config = {
        "gateway.auth.access.token": INSTALL_TOKEN,
        "gateway.auth.id": None,
        "gateway.auth.metadata.id": "Lw==",
        "gateway.auth.refresh.token": None,
        "gateway.auth.time": time.time(),
        "gateway.upload.segment.size": None,
        "gateway.url": "http://127.0.0.1:42456/sync",
        "mount.path": "/sync"
    }

    url = f"{SYNC_DAEMON_URL}/gateway_auth"
    attempts = [
        ("mount config", json.dumps(mount_config).encode(), "application/json"),
        ("mount array", json.dumps([mount_config]).encode(), "application/json"),
        ("token only", json.dumps({"gateway.auth.access.token": INSTALL_TOKEN}).encode(), "application/json"),
        ("empty json", b"{}", "application/json"),
        ("no content-type", json.dumps(mount_config).encode(), None),
    ]

    for label, body, ct in attempts:
        try:
            req = urllib.request.Request(url, data=body, method="POST")
            if ct:
                req.add_header("Content-Type", ct)
            resp = urllib.request.urlopen(req, timeout=10)
            result = resp.read().decode("utf-8", errors="replace")
            print(f"  Auth ({label}): OK HTTP {resp.status}")
            print(f"  Response: {result[:200]}")
            _auth_token = INSTALL_TOKEN
            return True
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")[:100]
            print(f"  Auth ({label}): HTTP {e.code} {body_text}")
        except Exception as e:
            print(f"  Auth ({label}): {e}")

    # If all auth attempts fail, try using install token directly for requests
    print("  Auth failed, trying install token for direct requests...")
    _auth_token = INSTALL_TOKEN
    return False


def make_request(url, method="GET", data=None, content_type=None):
    """Make an HTTP request trying multiple auth styles."""
    errors = []
    auth_styles = [
        ("Bearer", f"Bearer {_auth_token}"),
        ("Basic", f"Basic {base64.b64encode(_auth_token.encode()).decode()}"),
        ("Token", _auth_token),
        ("No auth", None),
    ]

    for label, auth_header in auth_styles:
        try:
            req = urllib.request.Request(url, data=data, method=method)
            if auth_header:
                req.add_header("Authorization", auth_header)
            if content_type:
                req.add_header("Content-Type", content_type)
            return urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                errors.append(f"{label}: 401")
                continue
            raise
    raise Exception(f"All auth styles failed: {errors}")


def encode_path(path):
    """Base64-encode a gateway path."""
    return base64.b64encode(path.encode("utf-8")).decode("utf-8")


def api_list(path):
    """List contents of a remote path."""
    encoded = encode_path(path)
    url = f"{SYNC_DAEMON_URL}/gateway_metadata_children/{encoded}"
    try:
        resp = make_request(url)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "body": e.read().decode()[:200]}
    except Exception as e:
        return {"error": str(e)}


def api_upload(local_file_path, remote_folder_path):
    """Upload a file to a remote folder path.

    Tries multiple body formats since we don't know exactly what the
    sync daemon expects.
    """
    filename = os.path.basename(local_file_path)

    # Try 1: JSON with local path (daemon reads file from disk)
    # Try 2: Full file path in base64 (not just folder)
    # Try 3: Raw file bytes
    # Try 4: Multipart form data

    attempts = []

    # Attempt 1: POST folder path, JSON body with local file path
    encoded_folder = encode_path(remote_folder_path)
    url_folder = f"{SYNC_DAEMON_URL}/gateway_metadata_file/{encoded_folder}"
    attempts.append((
        "json-local-path",
        url_folder,
        json.dumps({"local_path": local_file_path, "filename": filename}).encode(),
        "application/json",
    ))

    # Attempt 2: POST with full file path (folder + filename) in URL
    full_remote = f"{remote_folder_path}/{filename}"
    encoded_full = encode_path(full_remote)
    url_full = f"{SYNC_DAEMON_URL}/gateway_metadata_file/{encoded_full}"
    attempts.append((
        "full-path-json",
        url_full,
        json.dumps({"local_path": local_file_path}).encode(),
        "application/json",
    ))

    # Attempt 3: POST folder path, raw file bytes
    with open(local_file_path, "rb") as f:
        file_data = f.read()
    attempts.append((
        "raw-bytes-folder",
        url_folder,
        file_data,
        "application/octet-stream",
    ))

    # Attempt 4: POST full path, raw file bytes
    attempts.append((
        "raw-bytes-full",
        url_full,
        file_data,
        "application/octet-stream",
    ))

    # Attempt 5: Multipart with different field name
    boundary = "----CanopyRouterBoundary"
    multipart_body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="upload"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n"
        f"\r\n"
    ).encode("utf-8") + file_data + f"\r\n--{boundary}--\r\n".encode("utf-8")
    attempts.append((
        "multipart",
        url_folder,
        multipart_body,
        f"multipart/form-data; boundary={boundary}",
    ))

    for label, url, body, ct in attempts:
        try:
            resp = make_request(url, method="POST", data=body, content_type=ct)
            result = resp.read().decode("utf-8", errors="replace")
            print(f"[{label}] OK HTTP {resp.status}: {result[:100]}")
            return resp.status, result
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")[:100]
            except Exception:
                err_body = ""
            print(f"[{label}] HTTP {e.code} {err_body}")
            if e.code != 400:  # If it's not "bad format", might be a real error
                return e.code, err_body
        except ConnectionResetError:
            print(f"[{label}] Connection reset")
        except Exception as e:
            print(f"[{label}] {e}")

    return None, "All upload formats failed"


def build_file_list(routed_dir, mapping):
    """Build list of files to upload from the Routed/ directory."""
    files = []

    for folder in sorted(os.listdir(routed_dir)):
        folder_path = os.path.join(routed_dir, folder)
        if not os.path.isdir(folder_path) or folder.startswith("_"):
            continue

        match = re.match(r"^(\S+)\s*-\s*(.+)$", folder)
        if not match:
            continue

        client_id = match.group(1)
        if client_id not in mapping:
            continue

        canopy_name = mapping[client_id]

        for pdf in sorted(os.listdir(folder_path)):
            if not pdf.lower().endswith(".pdf"):
                continue

            year_match = re.search(r"\b(20\d{2})\b", pdf)
            if not year_match:
                continue
            tax_year = year_match.group(1)

            local_path = os.path.join(folder_path, pdf)
            remote_path = f"/Clients/{canopy_name.rstrip('.')}/{tax_year}/Tax/Tax Files"

            files.append({
                "local_path": local_path,
                "remote_path": remote_path,
                "filename": pdf,
                "client_id": client_id,
                "canopy_name": canopy_name,
            })

    return files


def main():
    parser = argparse.ArgumentParser(
        description="Upload files to Canopy via local sync daemon API."
    )
    parser.add_argument("--go", action="store_true", help="Execute uploads")
    parser.add_argument("--start", type=int, default=1, help="Start from file #")
    parser.add_argument("--routed-dir", help="Path to Routed/ directory")
    parser.add_argument("--mapping-csv", help="Path to Canopy CSV")
    parser.add_argument("--test-auth", action="store_true", help="Just test auth and list root")
    args = parser.parse_args()

    # Test mode -- just check connectivity
    if args.test_auth:
        print("Testing sync daemon API...")
        print(f"  URL: {SYNC_DAEMON_URL}")

        authenticate()

        print("\nListing root (/):")
        result = api_list("/")
        print(f"  {json.dumps(result)[:300]}")

        print("\nListing /Clients:")
        result = api_list("/Clients")
        if isinstance(result, dict) and "error" not in result:
            print(f"  Found entries (showing first 5)")
            items = result if isinstance(result, list) else [result]
            for item in items[:5]:
                print(f"    {item}")
        else:
            print(f"  {json.dumps(result)[:300]}")
        return

    # Load config and mapping
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))
    base_staging = config.get("staging_dir") or os.path.dirname(script_dir)
    routed_dir = args.routed_dir or os.path.join(base_staging, "Routed")
    mapping_csv = args.mapping_csv or config.get("mapping_csv") or find_mapping_csv(base_staging)

    if not os.path.isdir(routed_dir):
        print(f"ERROR: Routed directory not found: {routed_dir}")
        sys.exit(1)

    mapping = load_canopy_mapping(mapping_csv)
    files = build_file_list(routed_dir, mapping)

    print(f"CanopyRouter API Upload")
    print(f"=" * 50)
    print(f"  Sync Daemon:  {SYNC_DAEMON_URL}")
    print(f"  Routed dir:   {routed_dir}")
    print(f"  Files:        {len(files)}")
    print(f"  Mode:         {'UPLOAD' if args.go else 'DRY RUN'}")
    print(f"=" * 50)

    if not args.go:
        for i, f in enumerate(files, 1):
            if i < args.start:
                continue
            print(f"  [{i}/{len(files)}] {f['filename']}")
            print(f"           -> {f['remote_path']}")
        print(f"\nDry run. Use --go to upload.")
        return

    # Authenticate
    print("\nAuthenticating...")
    authenticate()

    success = 0
    failed = 0

    for i, f in enumerate(files, 1):
        if i < args.start:
            continue

        print(f"  [{i}/{len(files)}] {f['filename']}")
        print(f"           -> {f['remote_path']}")

        status, result = api_upload(f["local_path"], f["remote_path"])

        if status == 200:
            print("OK")
            success += 1
        elif status == 409 or (result and "already exists" in result.lower()):
            print("SKIP (exists)")
            success += 1
        else:
            print(f"FAIL (HTTP {status}: {result[:80]})")
            failed += 1

        time.sleep(1)

    print(f"\n{'=' * 50}")
    print(f"Uploaded: {success}")
    print(f"Failed:   {failed}")


if __name__ == "__main__":
    main()
