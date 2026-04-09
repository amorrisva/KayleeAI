#!/usr/bin/env python3
"""
CanopyRouter Production Processor

Watches a staging directory for UltraTax PDF prints, processes them:
1. Routes + renames files by Client ID
2. Uploads to Canopy entity Tax Files folder (replaces existing)
3. Copies K-1s to recipient Workpapers (TIN-matched)
4. Moves processed files to Processed/ or Failed/ subdirectories
5. Generates an actionable exception report after each run
6. Logs every file operation to a rotating log file

Usage:
    python canopy_process.py                    # process once
    python canopy_process.py --watch            # watch continuously
    python canopy_process.py --watch --interval 60
    python canopy_process.py --dry-run          # preview without uploading
"""

import argparse
import base64
import csv
import glob
import hashlib
import json
import logging
import logging.handlers
import os
import re
import shutil
import sys
import tempfile
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
    MAX_STEM_LENGTH,
)
from canopy_upload_final import CanopyUploader

try:
    import pdfplumber
    import openpyxl
    HAS_TIN_SUPPORT = True
except ImportError:
    HAS_TIN_SUPPORT = False

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------
CONFIG_DIR = "Config"           # CSV and XLSX exports go here
PROCESSED_DIR = "Processed"
FAILED_DIR = "Failed"
FAILED_UNMATCHED = "Failed/_Unmatched_Client"
FAILED_UPLOAD = "Failed/_Upload_Error"
FAILED_PARSE = "Failed/_Parse_Error"
LOG_DIR = "Logs"
REPORTS_DIR = "Reports"
LOCK_FILE = ".canopy_process.lock"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(staging_dir):
    """Configure rotating file logger + console output."""
    log_dir = os.path.join(staging_dir, LOG_DIR)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Rotating file handler -- 5MB per file, keep 10 files
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "canopy_process.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    return logger


# ---------------------------------------------------------------------------
# Lock file
# ---------------------------------------------------------------------------
def acquire_lock(staging_dir):
    """Acquire a lock file to prevent concurrent runs."""
    lock_path = os.path.join(staging_dir, LOCK_FILE)
    if os.path.isfile(lock_path):
        try:
            with open(lock_path, "r") as f:
                pid = int(f.read().strip())
            # Check if the process is still running (Windows)
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if handle:
                kernel32.CloseHandle(handle)
                logging.error(
                    f"Another instance is already running (PID {pid}). "
                    f"Delete {lock_path} if this is incorrect."
                )
                return False
        except (ValueError, OSError, AttributeError):
            pass  # Stale lock file -- remove it
        os.remove(lock_path)

    with open(lock_path, "w") as f:
        f.write(str(os.getpid()))
    return True


def release_lock(staging_dir):
    lock_path = os.path.join(staging_dir, LOCK_FILE)
    if os.path.isfile(lock_path):
        os.remove(lock_path)


# ---------------------------------------------------------------------------
# Config file discovery
# ---------------------------------------------------------------------------
def find_config_csv(staging_dir):
    """Find Canopy CSV in Config/ directory or staging root."""
    config_dir = os.path.join(staging_dir, CONFIG_DIR)
    for search_dir in [config_dir, staging_dir]:
        pattern = os.path.join(search_dir, "CanopyClientsExport*.csv")
        matches = glob.glob(pattern)
        if matches:
            best = max(matches, key=os.path.getmtime)
            age_days = (time.time() - os.path.getmtime(best)) / 86400
            if age_days > 7:
                logging.warning(
                    f"Canopy CSV is {int(age_days)} days old: {os.path.basename(best)}\n"
                    f"  Export a fresh copy from Canopy > Contacts > Export."
                )
            return best

    raise FileNotFoundError(
        f"Canopy client export CSV not found.\n"
        f"Export from Canopy > Contacts > Export and save to:\n"
        f"  {os.path.join(staging_dir, CONFIG_DIR)}"
    )


def find_tin_file(staging_dir):
    """Find TIN export XLSX in Config/ directory or staging root."""
    if not HAS_TIN_SUPPORT:
        return None
    config_dir = os.path.join(staging_dir, CONFIG_DIR)
    for search_dir in [config_dir, staging_dir]:
        matches = glob.glob(os.path.join(search_dir, "*TIN*.xlsx")) + \
                  glob.glob(os.path.join(search_dir, "*tin*.xlsx")) + \
                  glob.glob(os.path.join(search_dir, "*TIN*.XLSX"))
        if matches:
            best = max(matches, key=os.path.getmtime)
            age_days = (time.time() - os.path.getmtime(best)) / 86400
            if age_days > 30:
                logging.warning(
                    f"TIN export is {int(age_days)} days old: {os.path.basename(best)}\n"
                    f"  Run the UltraTax Data Mining report to refresh."
                )
            return best
    return None


# ---------------------------------------------------------------------------
# TIN matching
# ---------------------------------------------------------------------------
def build_tin_index(staging_dir):
    """Build TIN -> (client_id, client_name) lookup from BOTH UltraTax exports.

    Sources:
    1. taxandspouseTIN.XLSX -- individual clients (SSN + spouse SSN)
    2. UT25_GeneralClientInformation.xls -- all clients including businesses (EIN)
    """
    index = {}

    # Source 1: Individual TIN export (SSNs)
    config_dir = os.path.join(staging_dir, CONFIG_DIR)
    tin_files = []
    for search_dir in [config_dir, staging_dir]:
        tin_files += glob.glob(os.path.join(search_dir, "*TIN*.xlsx"))
        tin_files += glob.glob(os.path.join(search_dir, "*tin*.xlsx"))
        tin_files += glob.glob(os.path.join(search_dir, "*TIN*.XLSX"))

    if tin_files:
        xlsx_path = max(set(tin_files), key=os.path.getmtime)
        try:
            wb = openpyxl.load_workbook(xlsx_path, read_only=True)
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
            logging.info(f"Individual TINs: {len(index)} from {os.path.basename(xlsx_path)}")
        except PermissionError:
            logging.error(f"Cannot read TIN file (is it open in Excel?): {xlsx_path}")
        except Exception as e:
            logging.error(f"Cannot read TIN file: {e}")

    # Source 2: General Client Information (EINs for businesses)
    xls_files = []
    for search_dir in [config_dir, staging_dir]:
        xls_files += glob.glob(os.path.join(search_dir, "UT*GeneralClientInformation*.xls"))
        xls_files += glob.glob(os.path.join(search_dir, "UT*GeneralClientInformation*.XLS"))

    if xls_files:
        xls_path = max(set(xls_files), key=os.path.getmtime)
        try:
            import xlrd
            wb = xlrd.open_workbook(xls_path)
            sheet = wb.sheet_by_index(0)
            added = 0
            for i in range(3, sheet.nrows):
                client_id = str(sheet.cell_value(i, 0)).strip()
                tin = str(sheet.cell_value(i, 7)).strip()
                client_name = str(sheet.cell_value(i, 1)).strip()
                if client_id and tin and tin not in index:
                    index[tin] = (client_id, client_name)
                    added += 1
            logging.info(f"Business TINs: {added} from {os.path.basename(xls_path)}")
        except ImportError:
            logging.warning("xlrd not installed -- cannot read business TIN file")
        except PermissionError:
            logging.error(f"Cannot read business TIN file (is it open in Excel?): {xls_path}")
        except Exception as e:
            logging.error(f"Cannot read business TIN file: {e}")

    return index


def extract_recipient_tin(pdf_path):
    """Extract all TINs (SSNs and EINs) from a K-1 PDF."""
    tins = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:4]:
                text = page.extract_text() or ""
                # SSNs: XXX-XX-XXXX
                for ssn in re.findall(r"\d{3}-\d{2}-\d{4}", text):
                    tins.add(ssn)
                # EINs: XX-XXXXXXX
                for ein in re.findall(r"\d{2}-\d{7}", text):
                    tins.add(ein)
    except Exception as e:
        logging.warning(f"TIN extraction failed for {os.path.basename(pdf_path)}: {e}")
    return tins


def build_name_index(csv_path):
    """Build (first, last) -> [(ext_id, name)] lookup for K-1 name matching."""
    index = {}
    try:
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
    except PermissionError:
        logging.error(f"Cannot read CSV (is it open in Excel?): {csv_path}")
    return index


def _normalize_name(name):
    """Normalize a name for comparison: lowercase, strip punctuation."""
    return re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()


def find_possible_name_matches(recipient_name, canopy_mapping, name_index):
    """Find possible matches by name for the exception report.

    Does NOT auto-match -- only returns suggestions for human review.
    """
    suggestions = []

    # Check normalized name against all Canopy clients
    norm_recip = _normalize_name(recipient_name)
    for ext_id, canopy_name in canopy_mapping.items():
        if _normalize_name(canopy_name) == norm_recip:
            suggestions.append((ext_id, canopy_name, "exact name"))

    # Check first/last index for individuals
    parts = recipient_name.strip().split()
    if len(parts) >= 2:
        first = parts[0].lower()
        last = parts[-1].lower()
        matches = name_index.get((first, last), [])
        for ext_id, name in matches:
            if (ext_id, name, "exact name") not in suggestions:
                suggestions.append((ext_id, name, "first/last"))

    return suggestions


def match_k1_recipient(pdf_path, tin_index, canopy_mapping, recipient_name, name_index,
                       entity_client_id=""):
    """Match a K-1 recipient by TIN ONLY.

    Returns (client_id, canopy_name, method) or None.
    Name matching is never used for auto-upload -- only for suggestions
    in the exception report.

    The entity_client_id is excluded from matches to avoid matching
    the issuing entity's own EIN instead of the recipient's.
    """
    if tin_index and HAS_TIN_SUPPORT:
        tins = extract_recipient_tin(pdf_path)
        for tin in tins:
            if tin in tin_index:
                client_id, ut_name = tin_index[tin]
                # Skip if this TIN belongs to the issuing entity
                if client_id == entity_client_id:
                    continue
                canopy_name = canopy_mapping.get(client_id)
                if canopy_name:
                    return client_id, canopy_name, "TIN"

    return None


# ---------------------------------------------------------------------------
# K-1 workpaper rename
# ---------------------------------------------------------------------------
def rename_k1_for_workpapers(filename, entity_name, recipient_name,
                              year=None, entity_type=""):
    """Rename K-1 for recipient's workpapers.

    Format: K1 - <Year> <EntityType> <Entity> - <Recipient>.pdf
    """
    if not year:
        m = re.search(r"\b(20\d{2})\b", filename)
        year = m.group(1) if m else ""
    if not year:
        return filename

    et = f" {entity_type}" if entity_type else ""
    base = f"K1 - {year}{et}"
    available = MAX_STEM_LENGTH - len(base) - 1  # space before entity
    short_entity = entity_name[:available].rstrip(" ,&.")
    fixed = f"{base} {short_entity}"

    available = MAX_STEM_LENGTH - len(fixed) - 3
    if available >= 5 and recipient_name:
        short_recip = recipient_name[:available].rstrip(" ,&.")
        stem = f"{fixed} - {short_recip}"
    else:
        stem = fixed

    if len(stem) > MAX_STEM_LENGTH:
        stem = stem[:MAX_STEM_LENGTH].rstrip(" -.,&")
    return f"{stem}.pdf"


# ---------------------------------------------------------------------------
# File disposition
# ---------------------------------------------------------------------------
def setup_dirs(staging_dir):
    for d in [CONFIG_DIR, PROCESSED_DIR, FAILED_DIR, FAILED_UNMATCHED,
              FAILED_UPLOAD, FAILED_PARSE, LOG_DIR, REPORTS_DIR]:
        os.makedirs(os.path.join(staging_dir, d), exist_ok=True)


def move_file(src, dest_dir, staging_dir):
    """Move a file to a disposition directory."""
    dest_base = os.path.join(staging_dir, dest_dir)
    os.makedirs(dest_base, exist_ok=True)
    fname = os.path.basename(src)
    dest = os.path.join(dest_base, fname)
    if os.path.exists(dest):
        stem, ext = os.path.splitext(fname)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(dest_base, f"{stem}_{ts}{ext}")
    try:
        shutil.move(src, dest)
    except PermissionError:
        logging.error(f"Cannot move {fname} -- file may be open in another program.")
    return dest


# ---------------------------------------------------------------------------
# Exception report
# ---------------------------------------------------------------------------
def generate_report(staging_dir, results, start_time):
    """Generate a structured exception report for admins."""
    report_dir = os.path.join(staging_dir, REPORTS_DIR)
    os.makedirs(report_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(report_dir, f"processing_report_{ts}.txt")

    duration = time.time() - start_time
    has_issues = (results["unmatched"] + results["failed"] +
                  results["parse_error"] + results["external_k1"]) > 0

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("CANOPYROUTER PROCESSING REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration:  {int(duration)} seconds\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Total files:       {results['total']}\n")
        f.write(f"Uploaded:          {results['uploaded']}\n")
        f.write(f"K-1s routed:       {results['k1_routed']}\n")
        f.write(f"Unmatched clients: {results['unmatched']}\n")
        f.write(f"Upload errors:     {results['failed']}\n")
        f.write(f"Parse errors:      {results['parse_error']}\n")
        f.write(f"External K-1s:     {results['external_k1']}\n")
        f.write(f"Replaced existing: {results.get('replaced', 0)}\n\n")

        if not has_issues:
            f.write("No issues found. All files processed successfully.\n")
        else:
            f.write("-" * 70 + "\n")
            f.write("ACTION REQUIRED\n")
            f.write("-" * 70 + "\n\n")

        if results.get("unmatched_files"):
            f.write("UNMATCHED CLIENTS\n")
            f.write("These Client IDs are not in the Canopy export.\n")
            f.write("TO FIX: Export a fresh client list from Canopy and save\n")
            f.write(f"to the Config/ directory. Then move these files back to\n")
            f.write(f"the staging root directory.\n\n")
            for fname, cid in results["unmatched_files"]:
                f.write(f"  [{cid}] {fname}\n")
            f.write("\n")

        if results.get("failed_files"):
            f.write("UPLOAD ERRORS\n")
            f.write("These files could not be uploaded to Canopy.\n")
            f.write("TO FIX: Check that the Canopy service is running,\n")
            f.write("then move these files back to the staging root.\n\n")
            for fname, error in results["failed_files"]:
                f.write(f"  {fname}\n    Error: {error}\n")
            f.write("\n")

        if results.get("parse_errors"):
            f.write("PARSE ERRORS\n")
            f.write("These filenames don't match the UltraTax format.\n")
            f.write("Expected: ClientName_ClientID_DocType_Jurisdiction_Year.pdf\n")
            f.write("TO FIX: Reprint from UltraTax with Client ID enabled.\n\n")
            for fname in results["parse_errors"]:
                f.write(f"  {fname}\n")
            f.write("\n")

        if results.get("external_k1_files"):
            f.write("EXTERNAL K-1 RECIPIENTS\n")
            f.write("These K-1 recipients are not clients of the firm.\n")
            f.write("The K-1 was uploaded to the entity's Tax Files,\n")
            f.write("but no workpaper copy was created for the recipient.\n")
            f.write("TO FIX: No action needed unless the recipient should be a client.\n\n")
            for entry in results["external_k1_files"]:
                recip, entity = entry[0], entry[1]
                fname = entry[2] if len(entry) > 2 else ""
                suggestions = entry[3] if len(entry) > 3 else []
                f.write(f"  {recip} (from {entity})")
                if fname:
                    f.write(f" - {fname}")
                f.write("\n")
                if suggestions:
                    for sid, sname, smethod in suggestions:
                        f.write(f"    Possible match: {sname} ({sid})\n")
            f.write("\n")

        if results.get("k1_wp_failures"):
            f.write("K-1 WORKPAPER COPY FAILURES\n")
            f.write("The K-1 was uploaded to the entity, but the copy to\n")
            f.write("the recipient's workpapers folder failed.\n")
            f.write("TO FIX: Manually upload the K-1 to the recipient's\n")
            f.write("Workpapers folder in Canopy.\n\n")
            for fname, recip, error in results["k1_wp_failures"]:
                f.write(f"  {fname} -> {recip}\n    Error: {error}\n")
            f.write("\n")

    return report_path, has_issues


def send_teams_webhook(webhook_url, report_path, results, has_issues):
    """Send one Teams card per run.

    Color logic:
    - Red: real errors (unmatched, upload failures, parse errors)
    - Orange: no errors but has external K-1s to verify
    - Green: everything clean
    """
    if not webhook_url:
        return

    import requests as req

    has_real_errors = (results["unmatched"] + results["failed"] +
                       results["parse_error"] + len(results.get("k1_wp_failures", []))) > 0
    has_external_k1 = bool(results.get("external_k1_files"))

    if has_real_errors:
        color = "FF0000"
        title = "CanopyRouter: Action Required"
    elif has_external_k1:
        color = "FFA500"
        title = "CanopyRouter: Files Processed"
    else:
        color = "00FF00"
        title = "CanopyRouter: All Files Processed"

    # Summary facts
    facts = [{"name": "Total Files", "value": str(results["total"])}]
    if results["uploaded"]:
        facts.append({"name": "Uploaded", "value": str(results["uploaded"])})
    if results.get("replaced", 0):
        facts.append({"name": "Replaced", "value": str(results["replaced"])})
    if results["k1_routed"]:
        facts.append({"name": "K-1s Routed", "value": str(results["k1_routed"])})
    if results["unmatched"]:
        facts.append({"name": "Unmatched", "value": str(results["unmatched"])})
    if results["failed"]:
        facts.append({"name": "Errors", "value": str(results["failed"])})

    sections = [{"activityTitle": title, "facts": facts}]

    # Processed files log
    if results.get("processed_log"):
        log_lines = []
        for entry in results["processed_log"]:
            line = f"- **{entry['renamed']}** -> {entry['client']}/{entry['year']}/Tax/Tax Files"
            log_lines.append(line)
            if entry.get("k1_dest"):
                log_lines.append(f"  - K-1 copy -> {entry['k1_dest']}")
        sections.append({
            "activityTitle": "Processed Files",
            "text": "\n\n".join(log_lines),
        })

    # External K-1s (informational, not an error)
    if has_external_k1:
        ext_lines = []
        for entry in results["external_k1_files"]:
            recip = entry[0]
            entity = entry[1]
            fname = entry[2] if len(entry) > 2 else ""
            suggestions = entry[3] if len(entry) > 3 else []
            line = f"- **{recip}** (from {entity})"
            if fname:
                line += f" - {fname}"
            ext_lines.append(line)
            if suggestions:
                for sid, sname, smethod in suggestions:
                    ext_lines.append(f"  - Possible match: **{sname}** ({sid})")
        ext_lines.append("")
        ext_lines.append("*No workpaper copy was created. If a possible match is correct, the TIN export files may need updating.*")
        sections.append({
            "activityTitle": "External K-1 Recipients - Please Verify",
            "text": "\n\n".join(ext_lines),
        })

    # Real errors with fix instructions
    if has_real_errors:
        issue_lines = []
        if results.get("unmatched_files"):
            issue_lines.append("**Unmatched Clients**")
            issue_lines.append("*Fix: Export a fresh client list from Canopy > Contacts > Export, save to Config/ folder, then move these files back to staging.*")
            for fname, cid in results["unmatched_files"]:
                issue_lines.append(f"- [{cid}] {fname}")

        if results.get("failed_files"):
            issue_lines.append("**Upload Errors**")
            issue_lines.append("*Fix: Check that Canopy Drive is running on the server, then move these files back to staging.*")
            for fname, err in results["failed_files"]:
                issue_lines.append(f"- {fname}: {err}")

        if results.get("k1_wp_failures"):
            issue_lines.append("**K-1 Workpaper Copy Failures**")
            issue_lines.append("*Fix: Manually upload the K-1 to the recipient's Workpapers folder in Canopy.*")
            for fname, recip, err in results["k1_wp_failures"]:
                issue_lines.append(f"- {fname} -> {recip}: {err}")

        if results.get("parse_errors"):
            issue_lines.append("**Parse Errors (bad filename format)**")
            issue_lines.append("*Fix: Reprint from UltraTax with Client ID in Position 1.*")
            for fname in results["parse_errors"]:
                issue_lines.append(f"- {fname}")

        sections.append({
            "activityTitle": "Issues & How to Fix",
            "text": "\n\n".join(issue_lines),
        })

    try:
        req.post(webhook_url, json={
            "@type": "MessageCard",
            "themeColor": color,
            "summary": title,
            "sections": sections,
        }, timeout=10)
    except Exception as e:
        logging.warning(f"Teams notification failed: {e}")


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def process_files(staging_dir, dry_run=False, teams_webhook=None):
    """Process all PDFs in the staging directory."""
    start_time = time.time()
    setup_dirs(staging_dir)

    # Load config files
    try:
        mapping_csv = find_config_csv(staging_dir)
    except FileNotFoundError as e:
        logging.error(str(e))
        return {"total": 0, "errors": ["CSV not found"]}

    try:
        mapping = load_canopy_mapping(mapping_csv)
    except PermissionError:
        logging.error(
            f"Cannot read Canopy CSV -- is it open in Excel?\n"
            f"  Close the file and retry: {mapping_csv}"
        )
        return {"total": 0, "errors": ["CSV locked"]}

    tin_index = build_tin_index(staging_dir) if HAS_TIN_SUPPORT else {}
    name_index = build_name_index(mapping_csv)

    if tin_index:
        logging.info(f"TIN index: {len(tin_index)} total entries (individuals + businesses)")
    else:
        logging.warning("No TIN files found -- K-1 workpaper copies will not be created automatically")

    # Find PDFs (only in root, not subdirectories)
    pdfs = sorted(
        f for f in os.listdir(staging_dir)
        if f.lower().endswith(".pdf") and os.path.isfile(os.path.join(staging_dir, f))
    )

    if not pdfs:
        return {"total": 0}

    results = {
        "total": len(pdfs), "uploaded": 0, "k1_routed": 0,
        "unmatched": 0, "failed": 0, "parse_error": 0,
        "external_k1": 0, "replaced": 0,
        "processed_log": [],
        "unmatched_files": [], "failed_files": [], "parse_errors": [],
        "external_k1_files": [], "k1_wp_failures": [],
    }

    # Connect to Canopy
    if not dry_run:
        try:
            uploader = CanopyUploader()
        except FileNotFoundError as e:
            logging.error(str(e))
            return {"total": len(pdfs), "errors": ["Canopy not installed"]}

        if not uploader.authenticate():
            logging.error("Cannot authenticate with Canopy sync daemon.")
            return {"total": len(pdfs), "errors": ["Auth failed"]}
    else:
        uploader = None

    logging.info(f"Processing {len(pdfs)} PDF(s)..." + (" [DRY RUN]" if dry_run else ""))

    for idx, pdf in enumerate(pdfs, 1):
        src = os.path.join(staging_dir, pdf)
        parsed = parse_filename(pdf)
        client_id = extract_client_id(pdf)

        # Parse validation
        if not client_id:
            logging.warning(f"  [{idx}/{len(pdfs)}] PARSE ERROR: {pdf}")
            logging.warning(f"    Filename doesn't match UltraTax format")
            if not dry_run:
                move_file(src, FAILED_PARSE, staging_dir)
            results["parse_error"] += 1
            results["parse_errors"].append(pdf)
            continue

        if not parsed.get("doc_type"):
            logging.warning(f"  [{idx}/{len(pdfs)}] PARSE ERROR: {pdf} (no doc type)")
            if not dry_run:
                move_file(src, FAILED_PARSE, staging_dir)
            results["parse_error"] += 1
            results["parse_errors"].append(pdf)
            continue

        year = parsed.get("year", "")
        if not year or not (2015 <= int(year) <= 2030):
            logging.warning(f"  [{idx}/{len(pdfs)}] PARSE ERROR: {pdf} (invalid year: {year})")
            if not dry_run:
                move_file(src, FAILED_PARSE, staging_dir)
            results["parse_error"] += 1
            results["parse_errors"].append(pdf)
            continue

        # Client matching
        if client_id not in mapping:
            logging.warning(f"  [{idx}/{len(pdfs)}] UNMATCHED: {pdf} (Client ID {client_id})")
            if not dry_run:
                move_file(src, FAILED_UNMATCHED, staging_dir)
            results["unmatched"] += 1
            results["unmatched_files"].append((pdf, client_id))
            continue

        canopy_name = mapping[client_id].rstrip(".")
        new_name = rename_for_canopy(pdf, canopy_name)
        remote_path = f"/Clients/{canopy_name}/{year}/Tax/Tax Files"

        if dry_run:
            logging.info(f"  [{idx}/{len(pdfs)}] {new_name} -> {canopy_name}/{year}/Tax/Tax Files/")
            results["uploaded"] += 1
            continue

        # Upload with renamed file
        try:
            with tempfile.TemporaryDirectory() as td:
                temp_file = os.path.join(td, new_name)
                shutil.copy2(src, temp_file)
                ok, msg = uploader.upload_file(temp_file, remote_path)
        except (OSError, PermissionError) as e:
            ok, msg = False, f"File error: {e}"

        if ok:
            if "Replaced" in msg or "replaced" in msg:
                results["replaced"] += 1
            logging.info(f"  [{idx}/{len(pdfs)}] OK: {new_name} -> {canopy_name}/{year}/Tax/Tax Files/")
            results["uploaded"] += 1

            # Track for notification
            log_entry = {
                "original": pdf,
                "renamed": new_name,
                "client": canopy_name,
                "year": year,
                "k1_dest": None,
                "k1_external": None,
            }

            # K-1 workpaper routing
            if "K1" in parsed.get("doc_type", "") and parsed.get("recipient"):
                recipient = parsed["recipient"]
                try:
                    match_result = match_k1_recipient(
                        src, tin_index, mapping, recipient, name_index,
                        entity_client_id=client_id
                    )
                except Exception as e:
                    logging.warning(f"    K1 match error: {e}")
                    match_result = None

                if match_result:
                    recip_id, recip_canopy_name, method = match_result
                    recip_canopy_name = recip_canopy_name.rstrip(".")
                    entity_type = parsed.get("entity_type", "")
                    wp_name = rename_k1_for_workpapers(new_name, canopy_name, recipient, year, entity_type)
                    wp_remote = f"/Clients/{recip_canopy_name}/{year}/Tax/Workpapers"

                    try:
                        with tempfile.TemporaryDirectory() as td2:
                            temp_file2 = os.path.join(td2, wp_name)
                            shutil.copy2(src, temp_file2)
                            ok2, msg2 = uploader.upload_file(temp_file2, wp_remote)
                    except Exception as e:
                        ok2, msg2 = False, str(e)

                    if ok2:
                        logging.info(f"    K1 -> {recip_canopy_name}/Workpapers/ [{method}]")
                        results["k1_routed"] += 1
                        log_entry["k1_dest"] = f"{recip_canopy_name}/Workpapers [{method}]"
                    else:
                        logging.warning(f"    K1 FAIL -> {recip_canopy_name}: {msg2}")
                        results["k1_wp_failures"].append((new_name, recip_canopy_name, msg2))
                else:
                    # Find possible name matches for the exception report
                    suggestions = find_possible_name_matches(
                        recipient, mapping, name_index
                    )
                    if suggestions:
                        suggest_str = ", ".join(f"{n} ({i})" for i, n, _ in suggestions)
                        logging.info(f"    K1 EXTERNAL: {recipient} -- possible match: {suggest_str}")
                    else:
                        logging.info(f"    K1 EXTERNAL: {recipient} (not a client)")
                    results["external_k1"] += 1
                    results["external_k1_files"].append((recipient, canopy_name, new_name, suggestions))
                    log_entry["k1_external"] = recipient

            results["processed_log"].append(log_entry)

            # Move to Processed
            move_file(src, PROCESSED_DIR, staging_dir)
        else:
            logging.error(f"  [{idx}/{len(pdfs)}] FAIL: {pdf} ({msg})")
            move_file(src, FAILED_UPLOAD, staging_dir)
            results["failed"] += 1
            results["failed_files"].append((pdf, msg))

        time.sleep(0.3)

    # Generate report
    if results["total"] > 0:
        report_path, has_issues = generate_report(staging_dir, results, start_time)
        logging.info(f"\nReport: {report_path}")

        if teams_webhook:
            send_teams_webhook(teams_webhook, report_path, results, has_issues)

    return results


def print_summary(results):
    logging.info("")
    logging.info("=" * 50)
    logging.info("Processing Complete")
    logging.info("=" * 50)
    logging.info(f"  Total:           {results.get('total', 0)}")
    logging.info(f"  Uploaded:        {results.get('uploaded', 0)}")
    logging.info(f"  Replaced:        {results.get('replaced', 0)}")
    logging.info(f"  K-1s routed:     {results.get('k1_routed', 0)}")
    logging.info(f"  Unmatched:       {results.get('unmatched', 0)}")
    logging.info(f"  Upload errors:   {results.get('failed', 0)}")
    logging.info(f"  Parse errors:    {results.get('parse_error', 0)}")
    logging.info(f"  External K-1s:   {results.get('external_k1', 0)}")


def main():
    parser = argparse.ArgumentParser(
        description="CanopyRouter Production Processor"
    )
    parser.add_argument("--staging-dir")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--teams-webhook", help="Teams incoming webhook URL")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir, "config.ini"))
    staging_dir = args.staging_dir or config.get("staging_dir") or os.path.dirname(script_dir)
    teams_webhook = args.teams_webhook or config.get("teams_webhook", "")

    setup_dirs(staging_dir)
    logger = setup_logging(staging_dir)

    logger.info("CanopyRouter Production Processor")
    logger.info("=" * 50)
    logger.info(f"  Staging:     {staging_dir}")
    logger.info(f"  Mode:        {'watch' if args.watch else 'once'}")
    logger.info(f"  Dry run:     {'yes' if args.dry_run else 'no'}")
    logger.info("=" * 50)

    if not acquire_lock(staging_dir):
        sys.exit(1)

    try:
        if args.watch:
            logger.info(f"\nWatching every {args.interval}s. Ctrl+C to stop.\n")
            while True:
                pdfs = [f for f in os.listdir(staging_dir)
                        if f.lower().endswith(".pdf")
                        and os.path.isfile(os.path.join(staging_dir, f))]
                if pdfs:
                    results = process_files(staging_dir, args.dry_run, teams_webhook)
                    print_summary(results)
                time.sleep(args.interval)
        else:
            results = process_files(staging_dir, args.dry_run, teams_webhook)
            if results.get("total", 0) > 0:
                print_summary(results)
            else:
                logger.info("No PDF files to process.")
    except KeyboardInterrupt:
        logger.info("\nStopped.")
    finally:
        release_lock(staging_dir)


if __name__ == "__main__":
    main()
