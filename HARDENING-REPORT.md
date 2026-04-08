# CanopyRouter Production Hardening Report

Generated 2026-04-08

---

## Top 5 Priorities

1. **Add file-based logging** -- No audit trail exists. All output is print() to stdout. If run as a scheduled task, everything is lost. Need rotating log files with timestamps.

2. **Add a lock file** -- No protection against concurrent runs. If the scheduled task fires while a previous run is still processing, both process the same files. Race conditions and duplicates.

3. **Wrap all I/O in try/except** -- Network errors, file locks (Excel has CSV open), disk space, missing mount.json all crash the entire batch. Individual file failures should not kill the run.

4. **Log when TIN extraction fails** -- Currently swallows all exceptions silently and falls back to name matching. The admin has no idea if a K-1 was matched by bulletproof TIN or by a guess based on "Jeffrey Anderson."

5. **Handle duplicates explicitly** -- When Canopy returns filename "(1)", the script says "OK (duplicate)" and moves on. The admin never knows their client folder now has two copies of the same return.

---

## CRITICAL Issues

### C1. No audit trail
Every operation is `print()`. No log file, no record of which files were uploaded, to which client, at what time. If someone asks "was this return delivered?" -- there's no evidence.

**Fix:** Add `logging` module with `RotatingFileHandler`. Log every file disposition.

### C2. Silent duplicate uploads
When Canopy returns 200 but the filename has "(1)", the code treats it as success. The admin never sees this. Client folders accumulate duplicate files silently.

**Fix:** Flag duplicates in a separate report file. Or when `--reprocess` is used, match by doc type + year + jurisdiction rather than exact filename.

### C3. K-1 workpaper copy fails but file moves to Processed/
If the primary upload succeeds but the K-1 workpaper copy fails, the original file still moves to `Processed/`. The recipient's workpaper copy is missing with no way to recover.

**Fix:** Move to `Failed/_K1_Workpaper_Partial/` or write a recovery manifest.

### C4. TIN extraction silently swallows exceptions
If pdfplumber can't open a K-1 (corrupted, encrypted, scanned), it returns empty. Falls to name matching which may match the wrong person. No warning logged.

**Fix:** Log the exception. Flag name-matched K-1s for human review.

### C5. mount.json read fails with no clear error
If Canopy isn't installed or the service isn't running, `__init__` crashes with `FileNotFoundError`. A 20-year-old admin sees a Python traceback.

**Fix:** Try/except with clear message: "Canopy Drive is not installed or the service is not running."

### C6. Race condition in K-1 workpaper processing
The K-1 copy reads `src` (the original staging file) after the primary upload. If an exception occurs, `move_file` never runs and the file is stuck.

**Fix:** Wrap K-1 block in try/except to ensure disposition always happens.

---

## HIGH Issues

### H1. Daemon port changes after restart
The sync daemon port is read once from `mount.json`. If Canopy restarts, the port changes. All API calls fail with connection refused -- unhandled crash.

**Fix:** On `ConnectionError`, re-read `mount.json` and retry.

### H2. No concurrency control
No lock file. Two instances processing the same files = duplicates + race conditions.

**Fix:** PID lock file at startup. Check and fail fast if another instance is running.

### H3. Excel locks crash the batch
If someone has the CSV or TIN XLSX open in Excel, the open call may fail. Crashes the entire run with no clear message.

**Fix:** Try/except with message: "Close the file in Excel and retry."

### H4. CSV not found gives Python traceback
If the Canopy export CSV is missing, `find_mapping_csv` raises `FileNotFoundError`. Admin sees traceback, not instructions.

**Fix:** Clear message with instructions to export from Canopy.

### H5. Network errors crash the batch
`requests.ConnectionError` and `requests.Timeout` are not caught. A network blip kills the entire run.

**Fix:** Add retry logic with exponential backoff.

### H6. ensure_folder makes redundant API calls
For each file, `ensure_folder` checks every path segment. 200 files = hundreds of redundant calls.

**Fix:** Cache folder existence in a set within the uploader instance.

---

## MEDIUM Issues

### M1. Stale CSV warning
No check on CSV age. If the CSV is 30 days old, new clients silently go to `_Unmatched`.

**Fix:** Warn if CSV is older than 7 days.

### M2. Client name path mismatches
If Canopy name has trailing space or non-breaking space, path won't match. `ensure_folder` may create orphaned folders at the top level.

**Fix:** Validate client folder exists before upload. Warn if creating `/Clients/<name>`.

### M3. Name-based K-1 matching can match wrong person
Common names ("Jeffrey Anderson") may match the wrong client. Name parsing assumes "First Last" -- won't handle "Anderson, Jeffrey" or "Jr" suffixes.

**Fix:** Always prefer TIN. Log when name matching is used.

### M4. TIN collision between records
Spouse SSNs can appear in multiple client records. First record wins silently.

**Fix:** Store as lists and resolve ambiguity.

### M5. Amended returns coexist with originals
Both go to the same folder. No logic to replace or flag.

### M6. Year extraction could grab wrong number
If client name contains "1040" or "2023", parser might grab it as year.

**Fix:** Validate year is in range 2018-2030.

### M7. Filename collisions from truncation
Two entities with names differing only past character 30 produce the same truncated filename.

---

## Operational Procedures Needed

### Refreshing the Canopy CSV
1. Export from Canopy > Contacts > Export
2. Save to `O:\IT\CanopyStaging\` (overwrite the old one)
3. Frequency: whenever new clients are added, or at minimum weekly during tax season

### Refreshing the TIN Export
1. Run Data Mining report "client both tin" in UltraTax
2. Export as XLSX
3. Save to `O:\IT\CanopyStaging\taxandspouseTIN.XLSX`
4. Frequency: whenever new individual clients are added

### Recovering failed files
1. Check `O:\IT\CanopyStaging\Failed\` subdirectories
2. Fix the issue (update CSV, etc.)
3. Move files back to `O:\IT\CanopyStaging\` root
4. They'll be picked up on the next scheduled run

### Checking for duplicates
1. Look in `O:\IT\CanopyStaging\Processed\` for files with timestamps in the name
2. Check Canopy for files with "(1)" or "(2)" suffixes
3. Delete the duplicate in Canopy if needed

---

## Scenario Assessment

| Scenario | Result | Severity |
|----------|--------|----------|
| Stale CSV | New clients -> `_Unmatched`. Clear and recoverable. | MEDIUM |
| Stale TIN | K-1s fall back to name matching. May match wrong person. | CRITICAL |
| Simultaneous prints | Race condition. Duplicates possible. | HIGH |
| Reprint (duplicate) | Silent "(1)" duplicate in Canopy. | CRITICAL |
| Daemon port change | Unhandled crash. | HIGH |
| Token expiry | Handled (401 retry). | LOW |
| Network interruption | Unhandled crash. Partial batch. | HIGH |
| Unusual filenames | Commas/ampersands work. Accents untested. | MEDIUM |
| Name mismatches | Uses Canopy name. Correct. | OK |
| Unparseable K-1 PDFs | Silently falls to name matching. | CRITICAL |
| Service not running | Unhandled crash. | HIGH |
| Disk space | Unhandled crash. | HIGH |
| File locking | Move fails, file stays in staging, re-uploaded next run. | MEDIUM |
| Overlapping runs | No protection. Race conditions. | HIGH |
| Non-UltraTax PDFs | Go to `_Parse_Error`. Correct. | LOW |
| CSV locked by Excel | Unhandled crash. | HIGH |
| Multiple tax years | Handled correctly. | OK |
| Amended returns | Both versions coexist. No replacement logic. | MEDIUM |
| Audit trail | None. | CRITICAL |
| Recovery | Manual. Move from Failed/ to root. | LOW |
