#!/usr/bin/env python3
"""
End-to-end pipeline test.

1. Picks one client from staging
2. Routes + renames the PDFs
3. Uploads to Canopy
4. Verifies the files appear in Canopy
5. Optionally cleans up test files

Usage:
    python test_pipeline.py                  # run test
    python test_pipeline.py --cleanup        # remove test files from Canopy
"""

import argparse
import base64
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_canopy_mapping,
    find_mapping_csv,
    extract_client_id,
    rename_for_canopy,
    sanitize_folder_name,
)
from canopy_upload_final import CanopyUploader

STAGING = "O:/IT/CanopyStaging"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def verify_in_canopy(uploader, remote_path, expected_files):
    """Check if files exist in Canopy."""
    encoded = base64.b64encode(remote_path.encode()).decode()
    url = f"{uploader.base_url}/v2/gateway_metadata_children/{encoded}"
    resp = uploader.session.get(url, timeout=15)
    if not resp.ok:
        return False, f"Could not list {remote_path}: HTTP {resp.status_code}"

    existing = [item["gateway.metadata.name"] for item in resp.json()]
    found = []
    missing = []
    for f in expected_files:
        if f in existing:
            found.append(f)
        else:
            missing.append(f)

    return len(missing) == 0, {"found": found, "missing": missing, "all_files": existing}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--client-id", default="ANDER019",
                        help="Client ID to test with (default: ANDER019 - Anderson, Jason)")
    args = parser.parse_args()

    mapping_csv = find_mapping_csv(STAGING)
    mapping = load_canopy_mapping(mapping_csv)

    test_id = args.client_id
    if test_id not in mapping:
        print(f"ERROR: {test_id} not found in Canopy mapping")
        sys.exit(1)

    canopy_name = mapping[test_id].rstrip(".")
    print(f"Test Pipeline")
    print(f"{'=' * 50}")
    print(f"  Client:     {test_id} - {canopy_name}")
    print(f"  Staging:    {STAGING}")
    print(f"{'=' * 50}")

    # Find test PDFs for this client
    test_pdfs = [f for f in os.listdir(STAGING)
                 if f.endswith(".pdf") and f"_{test_id}_" in f]
    if not test_pdfs:
        print(f"\nNo PDFs found for {test_id} in staging.")
        sys.exit(1)

    print(f"\nFound {len(test_pdfs)} PDF(s):")
    for f in test_pdfs:
        print(f"  {f}")

    # Connect to Canopy
    uploader = CanopyUploader()
    print("\nAuthenticating...", end=" ")
    if not uploader.authenticate():
        print("FAILED")
        sys.exit(1)
    print("OK")

    if args.cleanup:
        print(f"\nCleaning up test files from Canopy...")
        # List files in the client's Tax Files folder and remove test uploads
        year = "2022"
        remote = f"/Clients/{canopy_name}/{year}/Tax/Tax Files"
        encoded = base64.b64encode(remote.encode()).decode()
        resp = uploader.session.get(
            f"{uploader.base_url}/v2/gateway_metadata_children/{encoded}",
            timeout=15,
        )
        if resp.ok:
            for item in resp.json():
                name = item["gateway.metadata.name"]
                # Delete files that match our naming convention
                if name.startswith(("PC Tax Return -", "PC K1 -", "K1 -", "CC Tax Return -", "Amended")):
                    file_id = item["gateway.metadata.id"]
                    del_url = f"{uploader.base_url}/v2/gateway_metadata_file/{file_id}"
                    r = uploader.session.delete(del_url, timeout=15)
                    print(f"  DELETE {name}: {r.status_code}")
        print("Cleanup done.")
        return

    # Step 1: Route + Rename
    print(f"\n--- Step 1: Route + Rename ---")
    test_routed = tempfile.mkdtemp(prefix="canopy_test_")
    folder_name = sanitize_folder_name(f"{test_id} - {canopy_name}")
    dest_dir = os.path.join(test_routed, folder_name)
    os.makedirs(dest_dir)

    renamed_files = []
    for pdf in test_pdfs:
        new_name = rename_for_canopy(pdf, canopy_name)
        src = os.path.join(STAGING, pdf)
        dst = os.path.join(dest_dir, new_name)
        shutil.copy2(src, dst)
        renamed_files.append(new_name)
        print(f"  {pdf}")
        print(f"    -> {new_name}")

    # Step 2: Upload
    print(f"\n--- Step 2: Upload ---")
    year = "2022"
    remote_path = f"/Clients/{canopy_name}/{year}/Tax/Tax Files"

    for fname in renamed_files:
        local = os.path.join(dest_dir, fname)
        print(f"  {fname} ... ", end="", flush=True)
        ok, msg = uploader.upload_file(local, remote_path)
        print(msg)

    # Step 3: Verify
    print(f"\n--- Step 3: Verify ---")
    all_ok, details = verify_in_canopy(uploader, remote_path, renamed_files)

    if all_ok:
        print(f"  ALL FILES VERIFIED IN CANOPY")
    else:
        print(f"  MISSING FILES:")
        for f in details["missing"]:
            print(f"    {f}")

    print(f"\n  Files in Canopy {remote_path}:")
    for f in details.get("all_files", []):
        marker = " <-- NEW" if f in renamed_files else ""
        print(f"    {f}{marker}")

    # Cleanup temp dir
    shutil.rmtree(test_routed)

    print(f"\n{'=' * 50}")
    print(f"Test {'PASSED' if all_ok else 'FAILED'}")
    if all_ok:
        print(f"\nTo clean up test files: python test_pipeline.py --cleanup --client-id {test_id}")


if __name__ == "__main__":
    main()
