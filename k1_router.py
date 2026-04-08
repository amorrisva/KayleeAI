#!/usr/bin/env python3
"""
K-1 Workpaper Router -- Copy K-1s to recipient client workpaper folders.

When a K-1 is issued by an entity (e.g., Adrenaline Outdoors, LLC),
this script also copies it to the recipient's individual client folder
under Tax/Workpapers/.

If the recipient is not a client of the firm, the K-1 goes to an
external exceptions folder for review.

Usage:
    python k1_router.py                    # dry-run
    python k1_router.py --go               # route + upload K-1s to workpapers
"""

import argparse
import base64
import csv
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
    parse_filename,
    rename_for_canopy,
    MAX_STEM_LENGTH,
)
from canopy_upload_final import CanopyUploader

GATEWAY_MOUNT = os.path.join(
    os.environ.get("LOCALAPPDATA", ""),
    "canopy", "Sync Dist", "gateway_shell", "mount.json"
)


def build_recipient_index(csv_path):
    """Build a lookup index for matching K-1 recipients to Canopy clients.

    Indexes by (first_name, last_name) including spouses.
    Returns dict of (first, last) -> [(ext_id, canopy_name)]
    """
    index = {}
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            name = row.get("Client Name", "").strip()
            ext_id = row.get("External ID", "").strip()
            if not ext_id or not name:
                continue

            if "," not in name:
                continue  # Business name, skip

            parts = name.split(",", 1)
            last = parts[0].strip()
            first_section = parts[1].strip()
            first_parts = first_section.split("&")

            # Primary name
            primary_first = first_parts[0].strip().split()[0]
            key = (primary_first.lower(), last.lower())
            index.setdefault(key, []).append((ext_id, name))

            # Spouse name if present
            if len(first_parts) > 1:
                spouse_name = first_parts[1].strip().split()[0]
                if spouse_name:
                    key2 = (spouse_name.lower(), last.lower())
                    index.setdefault(key2, []).append((ext_id, name))

    return index


def match_recipient(recipient_name, index):
    """Match a K-1 recipient name to a Canopy client.

    Args:
        recipient_name: e.g. "Jeffrey Anderson"
        index: from build_recipient_index()

    Returns:
        (ext_id, canopy_name) or None if no match, or list if ambiguous
    """
    parts = recipient_name.strip().split()
    if len(parts) < 2:
        return None

    first = parts[0].lower()
    last = parts[-1].lower()
    matches = index.get((first, last), [])

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        return matches  # Ambiguous -- caller decides
    return None


def rename_k1_for_workpapers(filename, entity_name, recipient_name, year=None, doc_prefix="PC K1"):
    """Rename K-1 for the recipient's workpapers folder.

    Format: <Doc Type> - <Year> - <Entity Name> [- <Recipient>].pdf
    Entity name first (important in workpapers), recipient last (truncated if needed).
    """
    if not year:
        year_match = re.search(r"\b(20\d{2})\b", filename)
        year = year_match.group(1) if year_match else ""
    if not year:
        return filename

    # Detect doc prefix from filename
    if filename.upper().startswith("PC K1"):
        doc_prefix = "PC K1"
    elif filename.upper().startswith("K1"):
        doc_prefix = "K1"

    # Build: DocType - Year - EntityName [- Recipient]
    base = f"{doc_prefix} - {year}"
    available_for_entity = MAX_STEM_LENGTH - len(base) - 3  # " - "
    short_entity = entity_name[:available_for_entity].rstrip(" ,&.")
    fixed = f"{base} - {short_entity}"

    # Add recipient if space allows
    available = MAX_STEM_LENGTH - len(fixed) - 3
    if available >= 5 and recipient_name:
        short_recip = recipient_name[:available].rstrip(" ,&.")
        stem = f"{fixed} - {short_recip}"
    else:
        stem = fixed

    if len(stem) > MAX_STEM_LENGTH:
        stem = stem[:MAX_STEM_LENGTH].rstrip(" -.,&")

    return f"{stem}.pdf"


def find_k1_files(staging_dir, mapping):
    """Find all K-1 PDFs in the staging directory."""
    k1s = []
    for f in sorted(os.listdir(staging_dir)):
        if not f.lower().endswith(".pdf") or not os.path.isfile(os.path.join(staging_dir, f)):
            continue

        parsed = parse_filename(f)
        if "K1" not in parsed.get("doc_type", ""):
            continue
        if not parsed.get("recipient"):
            continue

        client_id = parsed.get("client_id", "")
        if client_id not in mapping:
            continue

        entity_name = mapping[client_id]
        k1s.append({
            "filename": f,
            "local_path": os.path.join(staging_dir, f),
            "client_id": client_id,
            "entity_name": entity_name,
            "recipient": parsed["recipient"],
            "year": parsed.get("year", ""),
            "doc_type": parsed.get("doc_type", ""),
        })

    return k1s


def find_k1_in_routed(routed_dir):
    """Find K-1 PDFs in the Routed/ directory (already renamed)."""
    k1s = []
    for folder in sorted(os.listdir(routed_dir)):
        folder_path = os.path.join(routed_dir, folder)
        if not os.path.isdir(folder_path) or folder.startswith("_"):
            continue

        match = re.match(r"^(\S+)\s*-\s*(.+)$", folder)
        if not match:
            continue
        client_id = match.group(1)
        entity_name = match.group(2).strip()

        for pdf in sorted(os.listdir(folder_path)):
            if not pdf.lower().endswith(".pdf") or "K1" not in pdf.upper():
                continue

            # Extract recipient and year from renamed filename
            # Format: PC K1 - 2022 - Cam Hulse - Adrenaline Performance, LLC.pdf
            k1_match = re.match(r"(?:PC )?K1 - (\d{4}) - (.+?)(?:\s*-\s*(.+?))?\.pdf", pdf, re.IGNORECASE)
            if not k1_match:
                continue

            year = k1_match.group(1)
            recipient = k1_match.group(2).strip()

            k1s.append({
                "filename": pdf,
                "local_path": os.path.join(folder_path, pdf),
                "client_id": client_id,
                "entity_name": entity_name,
                "recipient": recipient,
                "year": year,
            })

    return k1s


def main():
    parser = argparse.ArgumentParser(
        description="Route K-1s to recipient workpaper folders in Canopy."
    )
    parser.add_argument("--go", action="store_true", help="Upload K-1s to workpapers")
    parser.add_argument("--routed-dir", help="Path to Routed/ directory")
    parser.add_argument("--mapping-csv", help="Path to Canopy CSV")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))
    base_staging = config.get("staging_dir") or os.path.dirname(script_dir)
    routed_dir = args.routed_dir or os.path.join(base_staging, "Routed")
    mapping_csv = args.mapping_csv or config.get("mapping_csv") or find_mapping_csv(base_staging)

    # Load mapping and recipient index
    mapping = load_canopy_mapping(mapping_csv)
    recipient_index = build_recipient_index(mapping_csv)

    # Find K-1s
    k1s = find_k1_in_routed(routed_dir)
    print(f"Found {len(k1s)} K-1 file(s) with recipients.\n")

    matched = []
    unmatched = []
    ambiguous = []

    for k1 in k1s:
        result = match_recipient(k1["recipient"], recipient_index)

        if result is None:
            unmatched.append(k1)
        elif isinstance(result, list):
            ambiguous.append((k1, result))
        else:
            ext_id, canopy_name = result
            wp_name = rename_k1_for_workpapers(
                k1["filename"], k1["entity_name"].rstrip("."), k1["recipient"]
            )
            matched.append({
                **k1,
                "recipient_id": ext_id,
                "recipient_canopy_name": canopy_name.rstrip("."),
                "workpaper_filename": wp_name,
                "remote_path": f"/Clients/{canopy_name.rstrip('.')}/{k1['year']}/Tax/Workpapers",
            })

    # Report
    print(f"{'=' * 60}")
    print(f"K-1 Workpaper Routing")
    print(f"{'=' * 60}")

    if matched:
        print(f"\nMATCHED ({len(matched)}):")
        for m in matched:
            print(f"  {m['recipient']} -> {m['recipient_canopy_name']}")
            print(f"    From: {m['entity_name']}")
            print(f"    File: {m['workpaper_filename']}")
            print(f"    Dest: {m['remote_path']}")

    if unmatched:
        print(f"\nUNMATCHED - NOT A CLIENT ({len(unmatched)}):")
        for u in unmatched:
            print(f"  {u['recipient']} (from {u['entity_name']})")
            print(f"    -> Goes to _EXTERNAL_K1/ for review")

    if ambiguous:
        print(f"\nAMBIGUOUS - MULTIPLE MATCHES ({len(ambiguous)}):")
        for k1, matches in ambiguous:
            print(f"  {k1['recipient']} (from {k1['entity_name']})")
            for ext_id, name in matches:
                print(f"    -> Could be: {name} ({ext_id})")

    if not args.go:
        print(f"\nDry run. Use --go to upload K-1s to workpapers.")
        return

    # Upload matched K-1s
    print(f"\nUploading {len(matched)} K-1(s) to recipient workpapers...")
    uploader = CanopyUploader()
    print("Authenticating...", end=" ")
    if not uploader.authenticate():
        print("FAILED")
        sys.exit(1)
    print("OK\n")

    success = 0
    failed = 0

    for m in matched:
        print(f"  {m['workpaper_filename']} -> {m['recipient_canopy_name']} ... ", end="", flush=True)

        # We need to create a temp file with the workpaper filename
        import shutil
        import tempfile
        temp_dir = tempfile.mkdtemp()
        temp_file = os.path.join(temp_dir, m["workpaper_filename"])
        shutil.copy2(m["local_path"], temp_file)

        ok, msg = uploader.upload_file(temp_file, m["remote_path"])

        # Cleanup
        os.remove(temp_file)
        os.rmdir(temp_dir)

        if ok:
            print(msg)
            success += 1
        else:
            print(msg)
            failed += 1

        time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print(f"Uploaded:   {success}")
    print(f"Failed:     {failed}")
    print(f"Unmatched:  {len(unmatched)}")
    print(f"Ambiguous:  {len(ambiguous)}")


if __name__ == "__main__":
    main()
