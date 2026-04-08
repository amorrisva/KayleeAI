#!/usr/bin/env python3
"""
CanopyRouter Production Processor

Watches a staging directory for UltraTax PDF prints, processes them:
1. Routes + renames files by Client ID
2. Uploads to Canopy entity Tax Files folder
3. Copies K-1s to recipient Workpapers (TIN-matched)
4. Moves processed files to Processed/ or Failed/ subdirectories

Preparers print to the staging directory. Files disappear once processed.
Failed files go to subdirectories with clear error reasons.

Usage:
    python canopy_process.py                    # process once
    python canopy_process.py --watch            # watch directory continuously
    python canopy_process.py --watch --interval 60  # check every 60 seconds
    python canopy_process.py --reprocess        # overwrite existing files in Canopy
"""

import argparse
import base64
import csv
import glob
import hashlib
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_config,
    find_mapping_csv,
    load_canopy_mapping,
    extract_client_id,
    parse_filename,
    rename_for_canopy,
    sanitize_folder_name,
)
from canopy_upload_final import CanopyUploader

try:
    import pdfplumber
    import openpyxl
    HAS_TIN_SUPPORT = True
except ImportError:
    HAS_TIN_SUPPORT = False


# Disposition directories
PROCESSED_DIR = "Processed"
FAILED_DIR = "Failed"
FAILED_UNMATCHED = "Failed/_Unmatched_Client"
FAILED_UPLOAD = "Failed/_Upload_Error"
FAILED_PARSE = "Failed/_Parse_Error"
FAILED_K1_EXTERNAL = "Failed/_External_K1"


def setup_dirs(staging_dir):
    """Create disposition directories."""
    for d in [PROCESSED_DIR, FAILED_DIR, FAILED_UNMATCHED,
              FAILED_UPLOAD, FAILED_PARSE, FAILED_K1_EXTERNAL]:
        os.makedirs(os.path.join(staging_dir, d), exist_ok=True)


def move_file(src, dest_dir, staging_dir):
    """Move a file to a disposition directory, handling name conflicts."""
    dest_base = os.path.join(staging_dir, dest_dir)
    os.makedirs(dest_base, exist_ok=True)
    fname = os.path.basename(src)
    dest = os.path.join(dest_base, fname)
    if os.path.exists(dest):
        # Add timestamp to avoid overwriting
        stem, ext = os.path.splitext(fname)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(dest_base, f"{stem}_{ts}{ext}")
    shutil.move(src, dest)
    return dest


def build_tin_index(staging_dir):
    """Load TIN index from any xlsx file with TIN in the name."""
    if not HAS_TIN_SUPPORT:
        return {}
    tin_files = glob.glob(os.path.join(staging_dir, "*TIN*.xlsx")) + \
                glob.glob(os.path.join(staging_dir, "*tin*.xlsx"))
    if not tin_files:
        return {}
    tin_file = tin_files[0]
    index = {}
    wb = openpyxl.load_workbook(tin_file, read_only=True)
    ws = wb.active
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i < 4:
            continue
        client_id = str(row[0] or "").strip()
        client_name = str(row[1] or "").strip()
        tp_ssn = str(row[5] or "").strip()
        sp_ssn = str(row[6] or "").strip()
        client_tin = str(row[3] or "").strip()
        if not client_id:
            continue
        if tp_ssn and tp_ssn != "None":
            index[tp_ssn] = (client_id, client_name)
        if sp_ssn and sp_ssn != "None":
            index[sp_ssn] = (client_id, client_name)
        if client_tin and client_tin != "None" and client_tin not in index:
            index[client_tin] = (client_id, client_name)
    wb.close()
    return index


def extract_recipient_tin(pdf_path):
    """Extract SSNs from a K-1 PDF."""
    ssns = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:4]:
                text = page.extract_text() or ""
                for ssn in re.findall(r"\d{3}-\d{2}-\d{4}", text):
                    ssns.add(ssn)
    except Exception:
        pass
    return ssns


def match_k1_recipient(pdf_path, tin_index, canopy_mapping, recipient_name, name_index):
    """Match a K-1 recipient using TIN (primary) or name (fallback).

    Returns (client_id, canopy_name, method) or None
    """
    # TIN matching
    if tin_index and HAS_TIN_SUPPORT:
        ssns = extract_recipient_tin(pdf_path)
        for ssn in ssns:
            if ssn in tin_index:
                client_id, ut_name = tin_index[ssn]
                canopy_name = canopy_mapping.get(client_id)
                if canopy_name:
                    return client_id, canopy_name, "TIN"

    # Name fallback
    parts = recipient_name.strip().split()
    if len(parts) >= 2:
        first = parts[0].lower()
        last = parts[-1].lower()
        matches = name_index.get((first, last), [])
        if len(matches) == 1:
            return matches[0][0], matches[0][1], "name"

    return None


def build_name_index(csv_path):
    """Build name lookup for K-1 recipient matching."""
    index = {}
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            name = row.get("Client Name", "").strip()
            ext_id = row.get("External ID", "").strip()
            if not ext_id or "," not in name:
                continue
            parts = name.split(",", 1)
            last = parts[0].strip()
            first_section = parts[1].strip()
            first_parts = first_section.split("&")
            primary_first = first_parts[0].strip().split()[0]
            key = (primary_first.lower(), last.lower())
            index.setdefault(key, []).append((ext_id, name))
            if len(first_parts) > 1:
                spouse = first_parts[1].strip().split()[0]
                if spouse:
                    key2 = (spouse.lower(), last.lower())
                    index.setdefault(key2, []).append((ext_id, name))
    return index


def rename_k1_for_workpapers(filename, entity_name, recipient_name, year=None):
    """Rename K-1 for recipient's workpapers folder."""
    from canopy_router import MAX_STEM_LENGTH
    if not year:
        m = re.search(r"\b(20\d{2})\b", filename)
        year = m.group(1) if m else ""
    if not year:
        return filename
    doc_prefix = "PC K1" if filename.upper().startswith("PC K1") else "K1"
    base = f"{doc_prefix} - {year}"
    available = MAX_STEM_LENGTH - len(base) - 3
    short_entity = entity_name[:available].rstrip(" ,&.")
    fixed = f"{base} - {short_entity}"
    available = MAX_STEM_LENGTH - len(fixed) - 3
    if available >= 5 and recipient_name:
        short_recip = recipient_name[:available].rstrip(" ,&.")
        stem = f"{fixed} - {short_recip}"
    else:
        stem = fixed
    if len(stem) > MAX_STEM_LENGTH:
        stem = stem[:MAX_STEM_LENGTH].rstrip(" -.,&")
    return f"{stem}.pdf"


def process_files(staging_dir, reprocess=False):
    """Process all PDFs in the staging directory.

    Returns dict with counts.
    """
    setup_dirs(staging_dir)
    mapping_csv = find_mapping_csv(staging_dir)
    mapping = load_canopy_mapping(mapping_csv)
    tin_index = build_tin_index(staging_dir)
    name_index = build_name_index(mapping_csv)

    # Find PDFs (only in root, not subdirectories)
    pdfs = sorted(
        f for f in os.listdir(staging_dir)
        if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(staging_dir, f))
    )

    if not pdfs:
        return {"total": 0}

    # Connect to Canopy
    uploader = CanopyUploader()
    if not uploader.authenticate():
        print("ERROR: Could not authenticate with Canopy")
        return {"total": len(pdfs), "errors": ["Auth failed"]}

    results = {
        "total": len(pdfs),
        "uploaded": 0,
        "k1_routed": 0,
        "unmatched": 0,
        "failed": 0,
        "parse_error": 0,
        "external_k1": 0,
    }

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{ts}] Processing {len(pdfs)} PDF(s)...")

    for pdf in pdfs:
        src = os.path.join(staging_dir, pdf)
        parsed = parse_filename(pdf)
        client_id = extract_client_id(pdf)

        # Parse error
        if not client_id or not parsed.get("doc_type"):
            print(f"  PARSE ERROR: {pdf}")
            move_file(src, FAILED_PARSE, staging_dir)
            results["parse_error"] += 1
            continue

        # Unmatched client
        if client_id not in mapping:
            print(f"  UNMATCHED: {pdf} (Client ID {client_id} not in Canopy)")
            move_file(src, FAILED_UNMATCHED, staging_dir)
            results["unmatched"] += 1
            continue

        canopy_name = mapping[client_id].rstrip(".")
        new_name = rename_for_canopy(pdf, canopy_name)
        year = parsed.get("year", "")

        if not year:
            print(f"  PARSE ERROR: {pdf} (no year)")
            move_file(src, FAILED_PARSE, staging_dir)
            results["parse_error"] += 1
            continue

        # Build remote path
        remote_path = f"/Clients/{canopy_name}/{year}/Tax/Tax Files"

        # Handle reprocessing -- check for existing file and delete it
        if reprocess:
            encoded = base64.b64encode(remote_path.encode()).decode()
            r = uploader.session.get(
                f"{uploader.base_url}/v2/gateway_metadata_children/{encoded}",
                timeout=15,
            )
            if r.ok:
                for item in r.json():
                    if item["gateway.metadata.name"] == new_name:
                        del_id = item["gateway.metadata.id"]
                        uploader.session.delete(
                            f"{uploader.base_url}/v2/gateway_metadata/{del_id}",
                            timeout=15,
                        )
                        print(f"  REPLACED: {new_name}")
                        break

        # Upload with renamed file
        import tempfile
        temp_dir = tempfile.mkdtemp()
        temp_file = os.path.join(temp_dir, new_name)
        shutil.copy2(src, temp_file)

        ok, msg = uploader.upload_file(temp_file, remote_path)

        os.remove(temp_file)
        os.rmdir(temp_dir)

        if ok:
            print(f"  OK: {new_name} -> {canopy_name}/{year}/Tax/Tax Files/")
            results["uploaded"] += 1

            # K-1 workpaper routing
            if "K1" in parsed.get("doc_type", "") and parsed.get("recipient"):
                recipient = parsed["recipient"]
                entity_name = canopy_name

                match_result = match_k1_recipient(
                    src, tin_index, mapping, recipient, name_index
                )

                if match_result:
                    recip_id, recip_canopy_name, method = match_result
                    recip_canopy_name = recip_canopy_name.rstrip(".")
                    wp_name = rename_k1_for_workpapers(
                        new_name, entity_name, recipient, year
                    )
                    wp_remote = f"/Clients/{recip_canopy_name}/{year}/Tax/Workpapers"

                    # Upload to workpapers
                    temp_dir2 = tempfile.mkdtemp()
                    temp_file2 = os.path.join(temp_dir2, wp_name)
                    shutil.copy2(src, temp_file2)

                    if reprocess:
                        enc2 = base64.b64encode(wp_remote.encode()).decode()
                        r2 = uploader.session.get(
                            f"{uploader.base_url}/v2/gateway_metadata_children/{enc2}",
                            timeout=15,
                        )
                        if r2.ok:
                            for item in r2.json():
                                if item["gateway.metadata.name"] == wp_name:
                                    uploader.session.delete(
                                        f"{uploader.base_url}/v2/gateway_metadata/{item['gateway.metadata.id']}",
                                        timeout=15,
                                    )
                                    break

                    ok2, msg2 = uploader.upload_file(temp_file2, wp_remote)
                    os.remove(temp_file2)
                    os.rmdir(temp_dir2)

                    if ok2:
                        print(f"    K1 -> {recip_canopy_name}/Workpapers/ [{method}]")
                        results["k1_routed"] += 1
                    else:
                        print(f"    K1 FAIL: {msg2}")
                else:
                    print(f"    K1 EXTERNAL: {recipient} (not a client)")
                    results["external_k1"] += 1

            # Move original to Processed
            move_file(src, PROCESSED_DIR, staging_dir)
        else:
            print(f"  FAIL: {pdf} ({msg})")
            move_file(src, FAILED_UPLOAD, staging_dir)
            results["failed"] += 1

        time.sleep(0.3)

    return results


def print_summary(results):
    """Print processing summary."""
    print(f"\n{'=' * 50}")
    print(f"Processing Complete")
    print(f"{'=' * 50}")
    print(f"  Total files:     {results.get('total', 0)}")
    print(f"  Uploaded:        {results.get('uploaded', 0)}")
    print(f"  K-1s routed:     {results.get('k1_routed', 0)}")
    print(f"  Unmatched:       {results.get('unmatched', 0)}")
    print(f"  Upload errors:   {results.get('failed', 0)}")
    print(f"  Parse errors:    {results.get('parse_error', 0)}")
    print(f"  External K-1s:   {results.get('external_k1', 0)}")


def main():
    parser = argparse.ArgumentParser(
        description="Production processor for UltraTax -> Canopy file routing."
    )
    parser.add_argument(
        "--staging-dir",
        help="Staging directory (default: parent of script dir)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch directory continuously for new files",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Seconds between checks in watch mode (default: 30)",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        help="Overwrite existing files in Canopy (for reprocessing)",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))
    staging_dir = args.staging_dir or config.get("staging_dir") or os.path.dirname(script_dir)

    print(f"CanopyRouter Production Processor")
    print(f"{'=' * 50}")
    print(f"  Staging:     {staging_dir}")
    print(f"  Mode:        {'watch' if args.watch else 'once'}")
    print(f"  Reprocess:   {'yes' if args.reprocess else 'no'}")
    print(f"{'=' * 50}")

    if args.watch:
        print(f"\nWatching for new files every {args.interval}s...")
        print(f"Press Ctrl+C to stop.\n")
        try:
            while True:
                pdfs = [f for f in os.listdir(staging_dir)
                        if f.lower().endswith(".pdf")
                        and os.path.isfile(os.path.join(staging_dir, f))]
                if pdfs:
                    results = process_files(staging_dir, args.reprocess)
                    print_summary(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        results = process_files(staging_dir, args.reprocess)
        print_summary(results)


if __name__ == "__main__":
    main()
