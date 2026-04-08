#!/usr/bin/env python3
"""
CanopyRouter Phase 2 -- Automated upload to Canopy via local sync daemon API.

Usage:
    python canopy_upload_final.py                   # dry-run
    python canopy_upload_final.py --go              # upload all files
    python canopy_upload_final.py --go --start 5    # start from file #5
    python canopy_upload_final.py --go --overwrite  # overwrite existing files
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_config,
    find_mapping_csv,
    load_canopy_mapping,
)

import requests

GATEWAY_MOUNT = os.path.join(
    os.environ.get("LOCALAPPDATA", ""),
    "canopy", "Sync Dist", "gateway_shell", "mount.json"
)


class CanopyUploader:
    def __init__(self):
        with open(GATEWAY_MOUNT, "r") as f:
            mounts = json.load(f)
        self.base_url = mounts[0]["gateway.url"]
        self.token = mounts[0]["gateway.auth.access.token"]
        self.session = requests.Session()
        self.session.headers.update({
            "Gateway-Agent": "B542-R0fc7c08",
            "Authorization": f"Bearer {self.token}",
        })

    def authenticate(self):
        url = f"{self.base_url}/v2/gateway_auth"
        resp = self.session.post(url, json={
            "gateway.auth.access.token": self.token,
        }, timeout=10)
        if resp.ok:
            data = resp.json()
            self.token = data.get("gateway.auth.access.token", self.token)
            self.session.headers["Authorization"] = f"Bearer {self.token}"
            return True
        return False

    def folder_exists(self, remote_path):
        encoded = base64.b64encode(remote_path.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_children/{encoded}"
        resp = self.session.get(url, timeout=15)
        return resp.status_code == 200

    def create_folder(self, parent_path, folder_name):
        """Create a subfolder under parent_path.

        Returns True if created or already exists.
        """
        encoded = base64.b64encode(parent_path.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_folder/{encoded}"
        resp = self.session.post(url, json={
            "gateway.metadata.name": folder_name,
        }, timeout=15)
        return resp.status_code == 200

    def ensure_folder(self, remote_path):
        """Ensure the full folder path exists, creating segments as needed.

        e.g. /Clients/Name/2022/Tax/Workpapers
        Creates 2022, Tax, and Workpapers if they don't exist.
        """
        if self.folder_exists(remote_path):
            return True

        # Walk up to find the deepest existing folder, then create down
        parts = remote_path.strip("/").split("/")
        existing = ""
        create_from = 0

        for i in range(len(parts)):
            test_path = "/" + "/".join(parts[:i + 1])
            if self.folder_exists(test_path):
                existing = test_path
                create_from = i + 1
            else:
                break

        # Create missing folders
        for i in range(create_from, len(parts)):
            parent = "/" + "/".join(parts[:i]) if i > 0 else "/"
            folder_name = parts[i]
            if not self.create_folder(parent, folder_name):
                return False

        return self.folder_exists(remote_path)

    def upload_file(self, local_path, remote_folder, skip_existing=True):
        """Upload a single file to a Canopy remote folder.

        Returns (success: bool, message: str)
        """
        filename = os.path.basename(local_path)
        with open(local_path, "rb") as f:
            file_data = f.read()

        file_size = len(file_data)
        file_sha256 = hashlib.sha256(file_data).hexdigest()
        mtime_ms = int(os.path.getmtime(local_path) * 1000)

        encoded = base64.b64encode(remote_folder.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_file/{encoded}"

        metadata = {
            "gateway.metadata.name": filename,
            "gateway.metadata.file.size": file_size,
            "gateway.metadata.file.sha256": file_sha256,
            "gateway.metadata.modified": mtime_ms,
        }

        resp = self.session.post(
            url,
            data=file_data,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Gateway-Upload": json.dumps(metadata),
            },
            timeout=120,
        )

        if resp.status_code == 200:
            resp_data = resp.json()
            uploaded_name = resp_data.get("gateway.metadata.name", filename)
            if "(1)" in uploaded_name or "(2)" in uploaded_name:
                return True, f"OK (duplicate: {uploaded_name})"
            return True, "OK"
        elif resp.status_code == 409:
            return True, "SKIP (exists)"
        elif resp.status_code == 401:
            if self.authenticate():
                return self.upload_file(local_path, remote_folder, skip_existing)
            return False, "AUTH FAILED"
        elif resp.status_code == 403:
            # Folder probably doesn't exist -- try creating it
            if self.ensure_folder(remote_folder):
                # Retry upload
                resp = self.session.post(
                    url,
                    data=file_data,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "X-Gateway-Upload": json.dumps(metadata),
                    },
                    timeout=120,
                )
                if resp.status_code == 200:
                    return True, "OK (folder created)"
                reason = resp.headers.get("X-Reason", resp.text[:100])
                return False, f"HTTP {resp.status_code} after folder creation: {reason}"
            return False, "HTTP 403: Could not create folder"
        else:
            reason = resp.headers.get("X-Reason", resp.text[:100])
            return False, f"HTTP {resp.status_code}: {reason}"


def build_file_list(routed_dir, mapping):
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
        canopy_name = mapping[client_id].rstrip(".")
        for pdf in sorted(os.listdir(folder_path)):
            if not pdf.lower().endswith(".pdf"):
                continue
            year_match = re.search(r"\b(20\d{2})\b", pdf)
            if not year_match:
                continue
            tax_year = year_match.group(1)
            files.append({
                "local_path": os.path.join(folder_path, pdf),
                "remote_path": f"/Clients/{canopy_name}/{tax_year}/Tax/Tax Files",
                "filename": pdf,
                "client_id": client_id,
                "canopy_name": canopy_name,
            })
    return files


def main():
    parser = argparse.ArgumentParser(
        description="Upload routed PDFs to Canopy via sync daemon API."
    )
    parser.add_argument("--go", action="store_true", help="Execute uploads")
    parser.add_argument("--start", type=int, default=1, help="Start from file #")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--routed-dir", help="Path to Routed/ directory")
    parser.add_argument("--mapping-csv", help="Path to Canopy CSV")
    args = parser.parse_args()

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

    # Connect to Canopy
    uploader = CanopyUploader()
    print(f"CanopyRouter Upload")
    print(f"{'=' * 50}")
    print(f"  Daemon:   {uploader.base_url}")
    print(f"  Files:    {len(files)}")
    print(f"  Mode:     {'UPLOAD' if args.go else 'DRY RUN'}")
    print(f"{'=' * 50}")

    print("\nAuthenticating...", end=" ")
    if uploader.authenticate():
        print("OK")
    else:
        print("FAILED")
        sys.exit(1)

    if not args.go:
        for i, f in enumerate(files, 1):
            if i < args.start:
                continue
            print(f"  [{i}/{len(files)}] {f['filename']}")
            print(f"           -> {f['remote_path']}")
        print(f"\nDry run. Use --go to upload.")
        return

    print()
    success = 0
    failed = 0
    failed_list = []

    for i, f in enumerate(files, 1):
        if i < args.start:
            continue

        print(f"  [{i}/{len(files)}] {f['filename']} ... ", end="", flush=True)

        ok, msg = uploader.upload_file(f["local_path"], f["remote_path"])

        if ok:
            print(msg)
            success += 1
        else:
            print(msg)
            failed += 1
            failed_list.append((f["filename"], msg))

        time.sleep(0.5)

    print(f"\n{'=' * 50}")
    print(f"Uploaded:  {success}")
    print(f"Failed:    {failed}")

    if failed_list:
        print(f"\nFailed files:")
        for fname, msg in failed_list:
            print(f"  {fname}: {msg}")


if __name__ == "__main__":
    main()
