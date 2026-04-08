#!/usr/bin/env python3
"""
CanopyRouter Phase 2 -- Upload routed PDFs to Canopy via the Gateway Shell.

Reads the Routed/ folder structure produced by canopy_router.py, renames files
for Canopy conventions, and uploads via the Canopy Gateway Shell.

Usage:
    python canopy_upload.py                      # dry-run: show commands
    python canopy_upload.py --execute            # run uploads via gateway shell
    python canopy_upload.py --batch upload.bat   # write commands to batch file

Run this from the SERVER where the Canopy Gateway Shell is installed.
"""

import argparse
import configparser
import csv
import glob
import os
import re
import subprocess
import sys
from datetime import datetime

# Import rename logic from the router
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canopy_router import (
    load_config,
    find_mapping_csv,
    load_canopy_mapping,
    extract_client_id,
    parse_filename,
    rename_for_canopy,
    sanitize_remote_name,
)

GATEWAY_SHELL = r"C:\Program Files (x86)\CanopyDrive\609\Sync Dist\canopy_gateway_shell.exe"
CANOPY_REMOTE_ROOT = "sync/Clients"


def build_upload_commands(staging_dir: str, mapping: dict, year: str = "") -> list:
    """Build gateway upload commands for all PDFs in staging.

    Returns list of dicts with: original, renamed, local_path, remote_path, client_id, canopy_name
    """
    commands = []

    pdfs = sorted(
        f for f in os.listdir(staging_dir)
        if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(staging_dir, f))
    )

    for pdf in pdfs:
        client_id = extract_client_id(pdf)
        if not client_id or client_id not in mapping:
            continue

        canopy_name = mapping[client_id]
        parsed = parse_filename(pdf)
        tax_year = year or parsed.get("year", "")
        if not tax_year:
            continue

        renamed = rename_for_canopy(pdf, canopy_name)
        local_path = os.path.join(staging_dir, pdf)
        remote_path = f"{CANOPY_REMOTE_ROOT}/{sanitize_remote_name(canopy_name)}/{tax_year}/Tax/Tax Files/"

        commands.append({
            "original": pdf,
            "renamed": renamed,
            "local_path": local_path,
            "remote_path": remote_path,
            "client_id": client_id,
            "canopy_name": canopy_name,
            "tax_year": tax_year,
        })

    return commands


def build_routed_upload_commands(routed_dir: str, mapping: dict) -> list:
    """Build upload commands from the Routed/ folder structure.

    Reads already-routed (and possibly renamed) files from per-client folders.
    """
    commands = []

    # Reverse mapping: client name -> external ID (for looking up Canopy name)
    name_by_id = {v: v for v in mapping.values()}

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

            parsed = parse_filename(pdf)
            tax_year = parsed.get("year", "")

            # If the file is already renamed, extract year from filename
            if not tax_year:
                year_match = re.search(r"\b(20\d{2})\b", pdf)
                if year_match:
                    tax_year = year_match.group(1)

            if not tax_year:
                continue

            renamed = rename_for_canopy(pdf, canopy_name)
            local_path = os.path.join(folder_path, pdf)
            remote_path = f"{CANOPY_REMOTE_ROOT}/{sanitize_remote_name(canopy_name)}/{tax_year}/Tax/Tax Files/"

            commands.append({
                "original": pdf,
                "renamed": renamed,
                "local_path": local_path,
                "remote_path": remote_path,
                "client_id": client_id,
                "canopy_name": canopy_name,
                "tax_year": tax_year,
            })

    return commands


def format_gateway_command(cmd: dict) -> str:
    """Format a single gateway upload command string.

    The gateway shell uploads the file with its local filename,
    so --remote should be just the destination directory.
    Files must be renamed locally before upload.
    """
    return (
        f'upload --local "{cmd["local_path"]}" '
        f'--remote "{cmd["remote_path"]}" --skip'
    )


def write_batch_file(commands: list, batch_path: str):
    """Write gateway upload commands, one per line, no comments.

    Output is clean for pasting directly into the Canopy Gateway Shell.
    """
    with open(batch_path, "w", encoding="utf-8") as f:
        for cmd in commands:
            f.write(format_gateway_command(cmd) + "\n")

    return batch_path


def execute_uploads(commands: list) -> dict:
    """Execute upload commands via the gateway shell.

    Pipes each command to the gateway shell as a separate subprocess call.
    """
    results = {"success": [], "failed": [], "skipped": []}

    if not os.path.isfile(GATEWAY_SHELL):
        print(f"ERROR: Gateway shell not found: {GATEWAY_SHELL}")
        print("Are you running this on the server?")
        return results

    total = len(commands)
    for i, cmd in enumerate(commands, 1):
        gateway_cmd = format_gateway_command(cmd)
        print(f"  [{i}/{total}] {cmd['renamed']}")
        print(f"           -> {cmd['remote_path']}")

        try:
            proc = subprocess.run(
                [GATEWAY_SHELL],
                input=gateway_cmd + "\nexit\n",
                capture_output=True,
                text=True,
                timeout=60,
            )
            output = proc.stdout.strip()
            if proc.returncode != 0 or "error" in output.lower():
                results["failed"].append((cmd, output))
                print(f"           FAILED: {output}")
            else:
                results["success"].append(cmd)
                print(f"           OK")
        except subprocess.TimeoutExpired:
            results["failed"].append((cmd, "Timeout"))
            print(f"           FAILED: Timeout")
        except OSError as e:
            results["failed"].append((cmd, str(e)))
            print(f"           FAILED: {e}")

    return results


def write_upload_report(output_dir: str, commands: list, results: dict = None):
    """Write upload report."""
    report_path = os.path.join(output_dir, "upload_report.txt")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("CanopyRouter -- Upload Report\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Total files: {len(commands)}\n")
        if results:
            f.write(f"  Uploaded:  {len(results['success'])}\n")
            f.write(f"  Failed:    {len(results['failed'])}\n")
            f.write(f"  Skipped:   {len(results['skipped'])}\n")
        f.write("\n")

        current_client = ""
        for cmd in commands:
            if cmd["canopy_name"] != current_client:
                current_client = cmd["canopy_name"]
                f.write(f"\n{cmd['client_id']} - {current_client}\n")
                f.write(f"  Remote: {cmd['remote_path']}\n")
            f.write(f"  {cmd['original']}\n")
            if cmd['renamed'] != cmd['original']:
                f.write(f"    -> {cmd['renamed']}\n")

        if results and results["failed"]:
            f.write("\n" + "=" * 70 + "\n")
            f.write("FAILURES\n")
            f.write("=" * 70 + "\n")
            for cmd, error in results["failed"]:
                f.write(f"  {cmd['original']}: {error}\n")

    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="Upload routed PDFs to Canopy via the Gateway Shell."
    )
    parser.add_argument(
        "--staging-dir",
        help="Path to staging directory with source PDFs (uploads directly)",
    )
    parser.add_argument(
        "--routed-dir",
        help="Path to Routed/ directory from Phase 1 (default)",
    )
    parser.add_argument(
        "--mapping-csv",
        help="Path to the Canopy client export CSV",
    )
    parser.add_argument(
        "--year",
        help="Override tax year for all files (e.g., 2022)",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--execute",
        action="store_true",
        help="Execute uploads via the gateway shell",
    )
    mode_group.add_argument(
        "--batch",
        metavar="FILE",
        help="Write gateway commands to a batch file",
    )

    args = parser.parse_args()

    # Load config
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))

    base_staging = args.staging_dir or config.get("staging_dir") or os.path.dirname(script_dir)
    mapping_csv = args.mapping_csv or config.get("mapping_csv") or find_mapping_csv(base_staging)

    # Load mapping
    mapping = load_canopy_mapping(mapping_csv)
    print(f"Loaded {len(mapping)} clients from Canopy export.\n")

    # Build commands from either staging dir or routed dir
    if args.staging_dir:
        print(f"Building commands from staging: {args.staging_dir}")
        commands = build_upload_commands(args.staging_dir, mapping, args.year or "")
    else:
        routed_dir = args.routed_dir or os.path.join(base_staging, "Routed")
        if not os.path.isdir(routed_dir):
            print(f"ERROR: Routed directory not found: {routed_dir}")
            print("Run canopy_router.py --copy first, or use --staging-dir.")
            sys.exit(1)
        print(f"Building commands from routed: {routed_dir}")
        commands = build_routed_upload_commands(routed_dir, mapping)

    if not commands:
        print("No files to upload.")
        return

    print(f"\n{len(commands)} file(s) to upload.\n")

    if args.batch:
        # Write batch file
        batch_path = write_batch_file(commands, args.batch)
        print(f"Batch file written to: {batch_path}")
        print("Paste these commands into the Canopy Gateway Shell to upload.")

    elif args.execute:
        # Execute uploads
        print("Uploading via Gateway Shell...")
        print("=" * 50)
        results = execute_uploads(commands)
        print()
        print("=" * 50)
        print(f"Uploaded:  {len(results['success'])}/{len(commands)}")
        if results["failed"]:
            print(f"Failed:    {len(results['failed'])}")

        report_path = write_upload_report(
            os.path.dirname(commands[0]["local_path"]),
            commands,
            results,
        )
        print(f"\nReport: {report_path}")

    else:
        # Dry run -- just show what would happen
        print("DRY RUN -- showing upload plan:\n")
        current_client = ""
        for cmd in commands:
            if cmd["canopy_name"] != current_client:
                current_client = cmd["canopy_name"]
                print(f"\n  {cmd['client_id']} - {current_client}")
                print(f"  -> {cmd['remote_path']}")
            print(f"     {cmd['original']}")
            if cmd["renamed"] != cmd["original"]:
                print(f"       => {cmd['renamed']}")

        print(f"\n** DRY RUN -- no files uploaded. **")
        print(f"   --execute  to upload via gateway shell")
        print(f"   --batch F  to write commands to a file")

        # Still write the report
        report_path = write_upload_report(base_staging, commands)
        print(f"   Report: {report_path}")


if __name__ == "__main__":
    main()
