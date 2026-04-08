#!/usr/bin/env python3
"""
CanopyRouter Phase 2 -- Deploy renamed PDFs directly to the Canopy virtual drive.

Copies files from the Routed/ folder into the Canopy virtual drive filesystem.
The Canopy sync engine handles the upload to cloud.

Usage:
    python canopy_deploy.py                     # dry-run (default)
    python canopy_deploy.py --go                # copy files to Canopy drive
    python canopy_deploy.py --canopy-dir X      # override Canopy path

Run this from the SERVER where the Canopy virtual drive is mounted.
"""

import argparse
import os
import re
import shutil
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_config,
    find_mapping_csv,
    load_canopy_mapping,
    extract_client_id,
    parse_filename,
)

CANOPY_CLIENTS_DIR = r"C:\Users\Administrator\Canopy\Clients"


def deploy_to_canopy(routed_dir: str, mapping: dict, canopy_dir: str,
                     dry_run: bool = True) -> dict:
    """Copy renamed files from Routed/ into the Canopy virtual drive.

    Target path: <canopy_dir>/<Client Name>/<Year>/Tax/Tax Files/<filename>
    """
    results = {"success": [], "failed": [], "skipped": [], "missing_dir": []}

    for folder in sorted(os.listdir(routed_dir)):
        folder_path = os.path.join(routed_dir, folder)
        if not os.path.isdir(folder_path) or folder.startswith("_"):
            continue

        # Parse folder name: "CLIENTID - Client Name"
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

            # Extract year from filename
            year_match = re.search(r"\b(20\d{2})\b", pdf)
            if not year_match:
                results["failed"].append((pdf, client_id, "Could not determine tax year"))
                continue
            tax_year = year_match.group(1)

            # Build destination path
            dest_dir = os.path.join(canopy_dir, canopy_name, tax_year, "Tax", "Tax Files")
            dest_file = os.path.join(dest_dir, pdf)
            src_file = os.path.join(folder_path, pdf)

            if dry_run:
                exists = "EXISTS" if os.path.isdir(dest_dir) else "CREATE"
                print(f"  [{exists}] {canopy_name}/{tax_year}/Tax/Tax Files/")
                print(f"           {pdf}")
            else:
                try:
                    # Create the folder structure if it doesn't exist
                    os.makedirs(dest_dir, exist_ok=True)

                    if os.path.exists(dest_file):
                        results["skipped"].append((pdf, client_id, "Already exists"))
                        print(f"  [SKIP] {pdf}")
                        continue

                    shutil.copy2(src_file, dest_file)

                    # Verify
                    if os.path.exists(dest_file):
                        results["success"].append((pdf, client_id, canopy_name))
                        print(f"  [  OK] {pdf}")
                    else:
                        results["failed"].append((pdf, client_id, "Copy succeeded but file not found"))
                        print(f"  [FAIL] {pdf} - not found after copy")

                except OSError as e:
                    results["failed"].append((pdf, client_id, str(e)))
                    print(f"  [FAIL] {pdf} - {e}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Deploy renamed PDFs to the Canopy virtual drive."
    )
    parser.add_argument(
        "--routed-dir",
        help="Path to Routed/ directory (default: auto-detect)",
    )
    parser.add_argument(
        "--canopy-dir",
        default=CANOPY_CLIENTS_DIR,
        help=f"Canopy Clients directory (default: {CANOPY_CLIENTS_DIR})",
    )
    parser.add_argument(
        "--mapping-csv",
        help="Path to the Canopy client export CSV",
    )
    parser.add_argument(
        "--go",
        action="store_true",
        help="Actually copy files (default is dry-run)",
    )

    args = parser.parse_args()

    # Load config
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))

    base_staging = config.get("staging_dir") or os.path.dirname(script_dir)
    routed_dir = args.routed_dir or os.path.join(base_staging, "Routed")
    mapping_csv = args.mapping_csv or config.get("mapping_csv") or find_mapping_csv(base_staging)

    if not os.path.isdir(routed_dir):
        print(f"ERROR: Routed directory not found: {routed_dir}")
        sys.exit(1)

    if not os.path.isdir(args.canopy_dir):
        print(f"ERROR: Canopy Clients directory not found: {args.canopy_dir}")
        print("Are you running this on the server?")
        sys.exit(1)

    mapping = load_canopy_mapping(mapping_csv)

    dry_run = not args.go
    mode_label = "DRY RUN" if dry_run else "DEPLOYING"

    print(f"CanopyRouter -- Deploy to Virtual Drive")
    print("=" * 50)
    print(f"  Routed dir:   {routed_dir}")
    print(f"  Canopy dir:   {args.canopy_dir}")
    print(f"  Clients:      {len(mapping)}")
    print(f"  Mode:         {mode_label}")
    print("=" * 50)
    print()

    results = deploy_to_canopy(routed_dir, mapping, args.canopy_dir, dry_run)

    print()
    print("=" * 50)
    if dry_run:
        print("** DRY RUN -- no files copied. Run with --go to deploy. **")
    else:
        print(f"Deployed:   {len(results['success'])}")
        if results["skipped"]:
            print(f"Skipped:    {len(results['skipped'])} (already exist)")
        if results["failed"]:
            print(f"Failed:     {len(results['failed'])}")
            print("\nFailures:")
            for pdf, cid, err in results["failed"]:
                print(f"  [{cid}] {pdf}: {err}")

        # Write report
        report_path = os.path.join(routed_dir, "deploy_report.txt")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"Deploy Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'=' * 60}\n")
            f.write(f"Deployed: {len(results['success'])}\n")
            f.write(f"Skipped:  {len(results['skipped'])}\n")
            f.write(f"Failed:   {len(results['failed'])}\n\n")
            for pdf, cid, name in results["success"]:
                f.write(f"  OK  [{cid}] {pdf}\n")
            for pdf, cid, reason in results["skipped"]:
                f.write(f"  SKIP [{cid}] {pdf} - {reason}\n")
            for pdf, cid, err in results["failed"]:
                f.write(f"  FAIL [{cid}] {pdf} - {err}\n")
        print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
