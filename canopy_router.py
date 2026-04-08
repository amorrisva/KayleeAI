#!/usr/bin/env python3
"""
CanopyRouter -- Route UltraTax PDF tax returns into per-client folders.

Matches the Client ID in each PDF filename against a Canopy client export CSV,
then copies (or moves) files into per-client subdirectories with Canopy-friendly
renamed filenames.

Usage:
    python canopy_router.py                     # dry-run (default)
    python canopy_router.py --copy              # copy + rename files
    python canopy_router.py --move              # move + rename files
    python canopy_router.py --no-rename         # keep original filenames
    python canopy_router.py --staging-dir X     # override staging path
"""

import argparse
import configparser
import csv
import glob
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

MAX_STEM_LENGTH = 56


def load_config(config_path: str) -> dict:
    """Load settings from config.ini if it exists."""
    defaults = {
        "staging_dir": "",
        "mapping_csv": "",
        "output_dir": "",
    }
    if os.path.isfile(config_path):
        cp = configparser.ConfigParser()
        cp.read(config_path)
        if cp.has_section("paths"):
            for key in defaults:
                if cp.has_option("paths", key):
                    defaults[key] = cp.get("paths", key)
    return defaults


def find_mapping_csv(staging_dir: str) -> str:
    """Auto-detect the Canopy export CSV in the staging directory."""
    pattern = os.path.join(staging_dir, "CanopyClientsExport*.csv")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(
            f"No Canopy CSV found matching {pattern}\n"
            "Export from Canopy and place in the staging directory."
        )
    # Use the most recently modified one
    return max(matches, key=os.path.getmtime)


def load_canopy_mapping(csv_path: str) -> dict:
    """Load External ID -> Client Name mapping from the Canopy CSV."""
    mapping = {}
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ext_id = row.get("External ID", "").strip()
            name = row.get("Client Name", "").strip()
            if ext_id:
                mapping[ext_id] = name
    return mapping


def extract_client_id(filename: str) -> str:
    """Extract Client ID from a PDF filename.

    Filename format: ClientName_ClientID_[Recipient_]DocType_Jurisdiction_Year.pdf
    The Client ID is always the second underscore-delimited segment.
    """
    stem = filename.rsplit(".pdf", 1)[0] if filename.lower().endswith(".pdf") else filename
    parts = stem.split("_")
    if len(parts) < 3:
        return ""
    return parts[1]


def parse_filename(filename: str) -> dict:
    """Parse a UltraTax PDF filename into its components.

    Returns dict with keys: client_name, client_id, recipient, doc_type,
    jurisdiction, year, copy_prefix, is_client_copy
    """
    stem = filename.rsplit(".pdf", 1)[0] if filename.lower().endswith(".pdf") else filename
    parts = stem.split("_")

    result = {
        "client_name": parts[0] if len(parts) > 0 else "",
        "client_id": parts[1] if len(parts) > 1 else "",
        "recipient": "",
        "doc_type": "",
        "jurisdiction": "",
        "year": "",
        "copy_prefix": "",
        "is_client_copy": False,
        "original": filename,
    }

    if len(parts) < 3:
        return result

    remaining = parts[2:]

    # Year is always the last part
    year_candidate = remaining[-1]
    if re.match(r"^\d{4}$", year_candidate):
        result["year"] = year_candidate
        remaining = remaining[:-1]

    # Identify doc type: contains TR, TxRtrn, or K1 as whole words
    # Must match word boundaries to avoid false positives like "Patrick"
    doc_type_idx = None
    for i, part in enumerate(remaining):
        if re.search(r"(?:^|\s)((?:Amended\s*)?(?:PC|CC)\s+(?:TR|TxRtrn|K1)|^(?:TR|TxRtrn|K1)$|^(?:PC|CC)\s+(?:TR|TxRtrn|K1))", part):
            doc_type_idx = i
            break

    if doc_type_idx is not None:
        raw_doc = remaining[doc_type_idx]
        result["doc_type"] = raw_doc

        # Parse copy prefix (CC, PC, AmendedPC, etc.)
        prefix_match = re.match(r"^(Amended\s*PC|CC|PC)\s+", raw_doc)
        if prefix_match:
            result["copy_prefix"] = prefix_match.group(1)
            result["is_client_copy"] = raw_doc.startswith("CC")

        # Everything before doc type is recipient
        if doc_type_idx > 0:
            result["recipient"] = " ".join(remaining[:doc_type_idx])

        # Everything after doc type is jurisdiction
        after_doc = remaining[doc_type_idx + 1:]
        if after_doc:
            result["jurisdiction"] = after_doc[0]
    else:
        # Fallback: no recognized doc type
        if remaining:
            result["doc_type"] = remaining[0]
            if len(remaining) > 1:
                result["jurisdiction"] = remaining[-1]

    return result


def normalize_doc_type(raw: str) -> str:
    """Normalize doc type abbreviations to readable names.

    PC TR -> PC Tax Return
    CC TxRtrn -> CC Tax Return
    AmendedPC TR -> Amended PC Tax Return
    K1 -> K1
    PC K1 -> PC K1
    """
    # Strip the prefix first
    cleaned = re.sub(r"^(Amended\s*PC|CC|PC)\s+", "", raw).strip()
    # Normalize the core type
    if cleaned in ("TR", "TxRtrn"):
        cleaned = "Tax Return"
    # Rebuild with cleaned prefix
    prefix_match = re.match(r"^(Amended\s*PC|CC|PC)\s+", raw)
    if prefix_match:
        prefix = prefix_match.group(1)
        # Normalize "AmendedPC" -> "Amended PC"
        if "Amended" in prefix and " " not in prefix:
            prefix = "Amended PC"
        return f"{prefix} {cleaned}"
    return cleaned


def rename_for_canopy(filename: str, canopy_client_name: str = "") -> str:
    """Rename a UltraTax PDF filename for Canopy.

    Client Copy (CC): <Year> - <Doc Type> - <Jurisdiction> [- <Recipient>] [- <Client>].pdf
    Preparer Copy (PC/other): <Doc Type> - <Year> - <Jurisdiction> [- <Recipient>] [- <Client>].pdf

    Client name is truncated to fit within MAX_STEM_LENGTH.
    """
    parsed = parse_filename(filename)

    if not parsed["doc_type"] or not parsed["year"]:
        return filename  # Can't rename, keep original

    doc_label = normalize_doc_type(parsed["doc_type"])
    year = parsed["year"]
    jurisdiction = parsed["jurisdiction"]
    recipient = parsed["recipient"]
    client_name = canopy_client_name or parsed["client_name"]

    # Build the fixed parts (everything before client name)
    if parsed["is_client_copy"]:
        # Client copy: Year - Doc Type - Jurisdiction [- Recipient]
        parts = [year, doc_label]
        if jurisdiction:
            parts.append(jurisdiction)
        if recipient:
            parts.append(recipient)
    else:
        # Preparer copy: Doc Type - Year - Jurisdiction [- Recipient]
        parts = [doc_label, year]
        if jurisdiction:
            parts.append(jurisdiction)
        if recipient:
            parts.append(recipient)

    fixed = " - ".join(parts)

    # Calculate remaining space for client name
    # Format: "<fixed> - <client>.pdf" = fixed + 3 (" - ") + client + 4 (.pdf)
    available = MAX_STEM_LENGTH - len(fixed) - 3  # 3 for " - " separator

    if available >= 8 and client_name:
        # Truncate client name to fit
        short_name = client_name[:available].rstrip(" ,&.")
        stem = f"{fixed} - {short_name}"
    else:
        stem = fixed

    # Ensure we don't exceed the limit
    if len(stem) > MAX_STEM_LENGTH:
        stem = stem[:MAX_STEM_LENGTH].rstrip(" -.,&")

    return f"{stem}.pdf"


def sanitize_folder_name(name: str) -> str:
    """Remove characters that are invalid in Windows folder names."""
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        name = name.replace(ch, "")
    return name.strip(". ")


def sanitize_remote_name(name: str) -> str:
    """Clean a client name for use in Canopy remote paths.

    Windows strips trailing periods from folder names, so the Canopy
    virtual drive folder won't have them even if the CSV does.
    """
    return name.rstrip(".")


def route_pdfs(staging_dir: str, mapping: dict, output_dir: str, mode: str,
               do_rename: bool = True) -> dict:
    """Route PDFs from staging into per-client folders.

    Args:
        staging_dir: directory containing source PDFs
        mapping: dict of External ID -> Canopy Client Name
        output_dir: root of the output folder tree
        mode: 'dry-run', 'copy', or 'move'
        do_rename: if True, rename files for Canopy conventions

    Returns:
        dict with routing results
    """
    results = {
        "matched": [],
        "unmatched": [],
        "errors": [],
        "skipped": [],
    }

    pdfs = sorted(
        f for f in os.listdir(staging_dir)
        if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(staging_dir, f))
    )

    if not pdfs:
        print("No PDF files found in staging directory.")
        return results

    print(f"Found {len(pdfs)} PDF(s) in staging.\n")

    for pdf in pdfs:
        src = os.path.join(staging_dir, pdf)
        client_id = extract_client_id(pdf)

        if not client_id:
            results["errors"].append((pdf, "Could not parse Client ID from filename"))
            continue

        if client_id in mapping:
            canopy_name = mapping[client_id]
            folder_name = sanitize_folder_name(f"{client_id} - {canopy_name}")
            dest_dir = os.path.join(output_dir, folder_name)
            dest_filename = rename_for_canopy(pdf, canopy_name) if do_rename else pdf
            results["matched"].append((pdf, client_id, canopy_name, dest_filename))
        else:
            dest_dir = os.path.join(output_dir, "_UNMATCHED")
            dest_filename = pdf  # Don't rename unmatched files
            results["unmatched"].append((pdf, client_id))

        dest_file = os.path.join(dest_dir, dest_filename)

        if mode == "dry-run":
            status = "MATCH" if client_id in mapping else "MISS "
            label = mapping.get(client_id, "(not in Canopy)")
            print(f"  [{status}] {client_id} -> {label}")
            if do_rename and client_id in mapping and dest_filename != pdf:
                print(f"           {pdf}")
                print(f"        -> {dest_filename}")
            else:
                print(f"           {pdf}")
        else:
            os.makedirs(dest_dir, exist_ok=True)
            try:
                if os.path.exists(dest_file):
                    results["skipped"].append((pdf, "Already exists in destination"))
                    continue
                if mode == "copy":
                    shutil.copy2(src, dest_file)
                elif mode == "move":
                    shutil.move(src, dest_file)
            except OSError as e:
                results["errors"].append((pdf, str(e)))

    return results


def write_report(output_dir: str, results: dict, mapping_csv: str, mode: str):
    """Write a summary report to the output directory."""
    report_path = os.path.join(output_dir, "route_report.txt")
    os.makedirs(output_dir, exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("CanopyRouter -- Routing Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Mapping CSV: {mapping_csv}\n")
        f.write(f"Mode: {mode}\n")
        f.write("=" * 70 + "\n\n")

        total = len(results["matched"]) + len(results["unmatched"])
        f.write(f"Total PDFs processed:  {total}\n")
        f.write(f"  Matched:             {len(results['matched'])}\n")
        f.write(f"  Unmatched:           {len(results['unmatched'])}\n")
        f.write(f"  Errors:              {len(results['errors'])}\n")
        f.write(f"  Skipped (existing):  {len(results['skipped'])}\n")
        f.write("\n")

        if results["matched"]:
            f.write("-" * 70 + "\n")
            f.write("MATCHED FILES\n")
            f.write("-" * 70 + "\n")
            # Group by client ID
            by_client = {}
            for entry in results["matched"]:
                pdf, cid, name = entry[0], entry[1], entry[2]
                dest = entry[3] if len(entry) > 3 else pdf
                by_client.setdefault(cid, {"name": name, "files": []})
                by_client[cid]["files"].append((pdf, dest))
            for cid in sorted(by_client):
                info = by_client[cid]
                f.write(f"\n  {cid} - {info['name']} ({len(info['files'])} files)\n")
                for pdf, dest in info["files"]:
                    if dest != pdf:
                        f.write(f"    {pdf}\n")
                        f.write(f"      -> {dest}\n")
                    else:
                        f.write(f"    {pdf}\n")

        if results["unmatched"]:
            f.write("\n" + "-" * 70 + "\n")
            f.write("UNMATCHED FILES (Client ID not found in Canopy)\n")
            f.write("-" * 70 + "\n")
            for pdf, cid in results["unmatched"]:
                f.write(f"  [{cid}] {pdf}\n")

        if results["errors"]:
            f.write("\n" + "-" * 70 + "\n")
            f.write("ERRORS\n")
            f.write("-" * 70 + "\n")
            for pdf, err in results["errors"]:
                f.write(f"  {pdf}: {err}\n")

        if results["skipped"]:
            f.write("\n" + "-" * 70 + "\n")
            f.write("SKIPPED (already in destination)\n")
            f.write("-" * 70 + "\n")
            for pdf, reason in results["skipped"]:
                f.write(f"  {pdf}\n")

    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="Route UltraTax PDF returns into per-client Canopy folders."
    )
    parser.add_argument(
        "--staging-dir",
        help="Path to the staging directory containing PDFs",
    )
    parser.add_argument(
        "--mapping-csv",
        help="Path to the Canopy client export CSV",
    )
    parser.add_argument(
        "--output-dir",
        help="Path for routed output folders (default: <staging>/Routed)",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Report only, no file operations (default)",
    )
    mode_group.add_argument(
        "--copy",
        action="store_true",
        help="Copy files into client folders (originals untouched)",
    )
    mode_group.add_argument(
        "--move",
        action="store_true",
        help="Move files into client folders",
    )
    parser.add_argument(
        "--no-rename",
        action="store_true",
        help="Keep original UltraTax filenames (skip Canopy rename)",
    )

    args = parser.parse_args()

    # Determine mode
    if args.move:
        mode = "move"
    elif args.copy:
        mode = "copy"
    else:
        mode = "dry-run"

    # Load config.ini defaults, then override with CLI args
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))

    staging_dir = args.staging_dir or config["staging_dir"] or os.path.dirname(script_dir)
    mapping_csv = args.mapping_csv or config["mapping_csv"] or find_mapping_csv(staging_dir)
    output_dir = args.output_dir or config["output_dir"] or os.path.join(staging_dir, "Routed")

    # Validate paths
    if not os.path.isdir(staging_dir):
        print(f"ERROR: Staging directory not found: {staging_dir}")
        sys.exit(1)
    if not os.path.isfile(mapping_csv):
        print(f"ERROR: Mapping CSV not found: {mapping_csv}")
        sys.exit(1)

    print("CanopyRouter Phase 1")
    print("=" * 50)
    print(f"  Staging dir:  {staging_dir}")
    print(f"  Mapping CSV:  {os.path.basename(mapping_csv)}")
    print(f"  Output dir:   {output_dir}")
    do_rename = not args.no_rename
    print(f"  Mode:         {mode}")
    print(f"  Rename:       {'yes' if do_rename else 'no'}")
    print("=" * 50)
    print()

    # Load mapping
    mapping = load_canopy_mapping(mapping_csv)
    print(f"Loaded {len(mapping)} clients from Canopy export.\n")

    # Route
    results = route_pdfs(staging_dir, mapping, output_dir, mode, do_rename)

    # Summary
    print()
    print("-" * 50)
    total = len(results["matched"]) + len(results["unmatched"])
    print(f"Matched:    {len(results['matched'])}/{total}")
    print(f"Unmatched:  {len(results['unmatched'])}/{total}")
    if results["errors"]:
        print(f"Errors:     {len(results['errors'])}")
    if results["skipped"]:
        print(f"Skipped:    {len(results['skipped'])}")

    # Write report (always, even in dry-run)
    report_path = write_report(output_dir, results, mapping_csv, mode)
    print(f"\nReport written to: {report_path}")

    if mode == "dry-run":
        print("\n** DRY RUN -- no files were copied or moved. **")
        print("   Re-run with --copy or --move to route files.")


if __name__ == "__main__":
    main()
