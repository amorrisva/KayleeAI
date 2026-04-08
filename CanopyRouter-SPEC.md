# CanopyRouter Spec

Route UltraTax-printed PDF tax returns into per-client folders using the
Client ID embedded in each filename, matched against a Canopy client export CSV.

---

## Filename Convention (set in UltraTax PDF File Options)

```
<Client Name>_<Client ID>_[Recipient Name_]<Doc Type>_<Jurisdiction>_<Year>.pdf
```

| Segment        | Example                          | Notes                                    |
|----------------|----------------------------------|------------------------------------------|
| Client Name    | `Aiken, Benjamin & Paula`        | May contain commas, ampersands, periods  |
| Client ID      | `AIKEN001`                       | UltraTax Client ID = Canopy External ID |
| Recipient Name | `Jeffrey Anderson` *(optional)*  | Present on K-1 documents only            |
| Doc Type       | `PC TR`, `CC TxRtrn`, `PC K1`    | Personal Copy, Client Copy, K-1, etc.   |
| Jurisdiction   | `US`, `ID`, `UT`, `AZ`, `CO`     | Federal or state abbreviation            |
| Year           | `2022`                           | Tax year                                 |

The **Client ID** is always the second underscore-delimited field.

## Data Sources

| File                                             | Purpose                            |
|--------------------------------------------------|-------------------------------------|
| `CanopyClientsExport - Active Clients (18).csv`  | External ID -> Canopy Client Name   |
| `UT25_GeneralClientInformation.xls`              | UltraTax client metadata (reference)|

### Canopy CSV Columns Used

- **External ID** -- the matching key (e.g. `AIKEN001`)
- **Client Name** -- used to name the destination folder

---

## Phase 1 -- Local File Router (this build)

### Goal

Parse PDFs in a staging directory, match each to a Canopy client via
External ID, and copy them into per-client subfolders. Generate a report.

### Inputs

| Input        | Default                                      |
|--------------|----------------------------------------------|
| Staging dir  | `O:/IT/CanopyStaging`                         |
| Mapping CSV  | `<staging>/CanopyClientsExport - *.csv`       |
| Output dir   | `<staging>/Routed`                            |

All paths are configurable via `config.ini` or CLI args.

### Behavior

1. Load the Canopy CSV into a dict keyed by `External ID`.
2. Scan the staging directory for `*.pdf` files (non-recursive).
3. For each PDF:
   a. Split filename on `_`, take index 1 as the Client ID.
   b. Look up Client ID in the Canopy dict.
   c. If matched: copy to `<output>/<ClientID> - <CanopyClientName>/`.
   d. If unmatched: copy to `<output>/_UNMATCHED/`.
4. Write a summary report (`route_report.txt`) with counts and details.

### Output Folder Structure

```
Routed/
  AIKEN001 - Aiken, Benjamin & Paula/
    Aiken, Benjamin & Paula_AIKEN001_PC TR_US_2022.pdf
    Aiken, Benjamin & Paula_AIKEN001_PC TR_UT_2022.pdf
  ADVAN003 - Advanced Foot & Ankle, LTD/
    Advanced Foot and Ankle LTD_ADVAN003_CC TxRtrn_AZ_2022.pdf
    ...
  _UNMATCHED/
    Albertson, Kylan_ALBER007_PC TR_US_2022.pdf
    ...
  route_report.txt
```

### Modes

- `--dry-run` (default): report only, no file copies
- `--copy`: copy files (originals untouched)
- `--move`: move files out of staging

### Drive Mapping Note

Paths default to `O:/IT/CanopyStaging` (mapped drive from workstation).
On the server this may be `C:/...` or another mount. Override via
`config.ini` or `--staging-dir` CLI arg.

---

## Canopy Virtual Drive

The Canopy desktop app syncs a virtual drive on the server:

```
C:\Users\Administrator\Canopy\Clients\
  2J Bee Storage, LLC\
  Alder, Gregory & Glenda\
    2022\
      Tax\
        Tax Files\
          CLIENT COPY - ID.pdf
          US TAX RETURN.pdf
          ...
    2023\
    2024\
    ...
```

### Key behaviors

- Folders are named by **client name only** (no Client ID prefix).
- Year subfolders contain `Tax\Tax Files\` for returns.
- Some client folders exist but are **empty** -- containing only a sync
  marker file (`ignore_this_file.sync`). Year folders may not exist yet.
- **Canopy truncates filenames at ~56 characters** (stem before .pdf).
- Never delete or move `ignore_this_file.sync`.

### Gateway Shell (proven working 2026-04-07)

The Canopy desktop app includes a gateway shell at:
`C:\Program Files (x86)\CanopyDrive\609\Sync Dist\canopy_gateway_shell.exe`

Upload command:
```
upload --local "<local_path>" --remote "sync/Clients/<Name>/<Year>/Tax/Tax Files/" --skip
```

- Paths with commas/ampersands must be double-quoted
- `mkdir` is broken but `upload` works without it
- `--skip` prevents overwriting existing files
- `--merge` skips if content is identical
- `--overwrite` replaces existing

---

## Filename Rename Convention

Files are renamed before upload. The format differs by copy type:

### Client Copy (CC) -- Year first

```
<Year> - <Doc Type> - <Jurisdiction> [- <Recipient>] [- <Client Short>].pdf
```

Examples:
```
BEFORE: Advanced Foot and Ankle LTD_ADVAN003_CC TxRtrn_US_2022.pdf
AFTER:  2022 - CC Tax Return - US - Advanced Foot.pdf

BEFORE: Advanced Foot and Ankle LTD_ADVAN003_CC TxRtrn_AZ_2022.pdf
AFTER:  2022 - CC Tax Return - AZ - Advanced Foot.pdf
```

### Preparer Copy (PC / AmendedPC / other) -- Doc type first

```
<Doc Type> - <Year> - <Jurisdiction> [- <Recipient>] [- <Client Short>].pdf
```

Examples:
```
BEFORE: Alder, Gregory & Glenda_ALDER001_PC TR_US_2022.pdf
AFTER:  PC Tax Return - 2022 - US - Alder, Gregory.pdf

BEFORE: Anderson Life & Health Agency, Inc_ANDER021_Jeffrey Anderson_PC K1_2022.pdf
AFTER:  PC K1 - 2022 - Jeffrey Anderson - Anderson Life.pdf

BEFORE: All Enterprises Ltd_ALLEN013_AmendedPC TR_US_2022.pdf
AFTER:  Amended PC Tax Return - 2022 - US - All Enterprises.pdf

BEFORE: Advanced Foot and Ankle LTD_ADVAN003_Ron Olsen_K1_2022.pdf
AFTER:  K1 - 2022 - Ron Olsen - Advanced Foot.pdf
```

### Rules

- Detection: doc type starting with `CC` -> client copy format; everything else -> preparer format
- Recipient name (K-1s): always included in full, never truncated
- Client name: fills remaining space up to 56-char stem limit, truncated if needed
- Doc type normalization: `TR` / `TxRtrn` -> `Tax Return`, `K1` stays `K1`

---

## Phase 2 -- Gateway Upload with Rename

Upload routed files to Canopy via the gateway shell, renaming per the
convention above. Uses `upload --local ... --remote ... --skip`.

## Phase 3 (future) -- Watch Mode

Monitor the staging folder and auto-route new PDFs as they land.
Notify via Teams webhook when exceptions occur.
