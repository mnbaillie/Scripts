#!/usr/bin/env python3
"""
lak_listing_budget_reader.py

Extract MODFLOW LAK package hydrologic budget summaries from a model listing
file and write a flat CSV with one row per lake per time step.

The parser is designed for classic MODFLOW LAK listing-file summaries that
start with text such as:

    HYDROLOGIC BUDGET SUMMARIES FOR SIMULATED LAKES

It handles observed formatting variants, including:
  * STAGE, PRECIP, EVAP, RUNOFF blocks
  * optional VOLUME and VOL. CHANGE values
  * optional UPDATED VOLUME values
  * GROUND WATER / SURFACE WATER inflow and outflow values
  * WATER USE, CONNECTED LAKE INFLUX, SURFACE AREA, STAGE CHANGE
  * optional PERCENT DISCREPANCY values
  * N/A (SS) values in steady-state output

Notes on LAK output:
  * For MODFLOW-2005/NWT-style LAK output, the complete hydrologic lake budget
    summaries are normally printed in the main listing file. Cell-by-cell budget
    output is commonly the lake-groundwater seepage term, not the complete lake
    hydrologic budget table.
  * MODFLOW 6 LAK is different and can write LAK budget CSV files directly via
    the BUDGETCSV FILEOUT option. This script targets listing-file summaries.

Typical use:
    python lak_listing_budget_reader.py --listing test2.lst --output lake_budget.csv

Edit the USER CONFIGURATION section below if running directly from an IDE.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# =============================================================================
# USER CONFIGURATION - edit these values if you prefer to run from an IDE
# =============================================================================
LISTING_FILE = r"Y:\mbaillie\SFRZB\Test Models\test3\test3.lst"
OUTPUT_CSV = r"Y:\mbaillie\SFRZB\Test Models\test3\test3_lakbud.csv"

# Text used to identify the start of each instantaneous lake-budget block.
# The search is case-insensitive and ignores leading/trailing whitespace.
BLOCK_START_TEXT = "HYDROLOGIC BUDGET SUMMARIES FOR SIMULATED LAKES"

# By default, skip cumulative lake-budget summaries. This keeps the output at
# one row per lake per model time step for the present time-step budget.
INCLUDE_CUMULATIVE_BLOCKS = False

# If True, print a short run summary to the console.
VERBOSE = True

# ---------------------------------------------------------------------
# LISTING FILE CONFIGURATION
# ---------------------------------------------------------------------

# False = single listing file only
# True  = listing file split into sequential files
LISTING_IS_SPLIT = False

# Highest split index if LISTING_IS_SPLIT = True
#
# Example:
#   SVIHM.lst
#   SVIHM01.lst
#   SVIHM02.lst
#   ...
#   SVIHM241.lst
#
# Then set:
#   SPLIT_LAST_INDEX = 241
#
# Ignored if LISTING_IS_SPLIT = False
SPLIT_LAST_INDEX = 0
CHECK_STRESS_PERIOD_COVERAGE = True

# =============================================================================

OUTPUT_COLUMNS = [
    "source_file",
    "block_type",
    "per",
    "stp",
    "delt",
    "pertim",
    "totim",
    "lake",
    "stage",
    "volume",
    "volume_change",
    "updated_volume",
    "precip",
    "evap",
    "runoff",
    "gw_inflow",
    "gw_outflow",
    "sw_inflow",
    "sw_outflow",
    "water_use",
    "connected_lake_influx",
    "surface_area",
    "stage_change_timestep",
    "stage_change_cumulative",
    "percent_discrepancy",
]

NUM_RE = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[EeDd][-+]?\d+)?")
PER_STP_RE = re.compile(
    r"PERIOD\s+(?P<per>\d+)\s+TIME\s+STEP\s+(?P<stp>\d+)"
    r"(?:\s+TIME\s+STEP\s+LENGTH\s+(?P<delt>[-+0-9.EeDd]+))?",
    re.IGNORECASE,
)
TIME_RE = re.compile(
    r"PERIOD\s+TIME\s+(?P<pertim>[-+0-9.EeDd]+)\s+TOTAL\s+SIMULATION\s+TIME\s+(?P<totim>[-+0-9.EeDd]+)",
    re.IGNORECASE,
)


def build_listing_file_list(base_listing_file,
                            listing_is_split=False,
                            split_last_index=0):

    base_path = Path(base_listing_file)

    if not listing_is_split:
        return [base_path]

    listing_files = [base_path]

    stem = base_path.stem
    suffix = base_path.suffix
    parent = base_path.parent

    for i in range(1, split_last_index + 1):
        split_name = f"{stem}{i:02d}{suffix}"
        listing_files.append(parent / split_name)

    return listing_files


def parse_number(token: str) -> Optional[float | str]:
    """Convert MODFLOW numeric text to float; preserve N/A values as text."""
    token = token.strip()
    if not token:
        return None
    if token.upper().startswith("N/A"):
        return token
    token = token.replace("D", "E").replace("d", "e")
    try:
        return float(token)
    except ValueError:
        return token


def tokenize_values(line: str) -> List[str]:
    """Split a data line while keeping 'N/A (SS)' together as one token."""
    parts = line.strip().split()
    out: List[str] = []
    i = 0
    while i < len(parts):
        if parts[i].upper() == "N/A" and i + 1 < len(parts) and parts[i + 1].startswith("("):
            out.append(parts[i] + " " + parts[i + 1])
            i += 2
        else:
            out.append(parts[i])
            i += 1
    return out


def is_lake_data_line(line: str) -> bool:
    return bool(re.match(r"^\s*\d+\s+", line))


def find_previous_metadata(lines: List[str], start_index: int, lookback: int = 12) -> Dict[str, Any]:
    """Find stress-period/time-step metadata above a LAK summary block."""
    meta: Dict[str, Any] = {"per": None, "stp": None, "delt": None, "pertim": None, "totim": None}
    lo = max(0, start_index - lookback)
    for line in lines[lo:start_index]:
        m = PER_STP_RE.search(line)
        if m:
            meta["per"] = int(m.group("per"))
            meta["stp"] = int(m.group("stp"))
            if m.group("delt"):
                meta["delt"] = parse_number(m.group("delt"))
        m = TIME_RE.search(line)
        if m:
            meta["pertim"] = parse_number(m.group("pertim"))
            meta["totim"] = parse_number(m.group("totim"))
    return meta


def empty_record(source_file: str, block_type: str, meta: Dict[str, Any], lake: int) -> Dict[str, Any]:
    row = {col: None for col in OUTPUT_COLUMNS}
    row.update(meta)
    row["source_file"] = source_file
    row["block_type"] = block_type
    row["lake"] = lake
    return row


def get_record(records: Dict[int, Dict[str, Any]], source_file: str, block_type: str, meta: Dict[str, Any], lake: int) -> Dict[str, Any]:
    if lake not in records:
        records[lake] = empty_record(source_file, block_type, meta, lake)
    return records[lake]


def assign_table_values(context: str, values: List[str], rec: Dict[str, Any]) -> None:
    """Assign values from one LAK sub-table row into the output record."""
    if not values:
        return
    c = context.upper()
    vals = [parse_number(v) for v in values]

    if "STAGE" in c and "PRECIP" in c:
        # Variants:
        #   lake stage precip evap runoff
        #   lake stage volume vol_change precip evaporation runoff
        rec["stage"] = vals[0] if len(vals) > 0 else None
        if "VOLUME" in c and "VOL" in c:
            if len(vals) > 1:
                rec["volume"] = vals[1]
            if len(vals) > 2:
                rec["volume_change"] = vals[2]
            if len(vals) > 3:
                rec["precip"] = vals[3]
            if len(vals) > 4:
                rec["evap"] = vals[4]
            if len(vals) > 5:
                rec["runoff"] = vals[5]
        else:
            if len(vals) > 1:
                rec["precip"] = vals[1]
            if len(vals) > 2:
                rec["evap"] = vals[2]
            if len(vals) > 3:
                rec["runoff"] = vals[3]
        return

    if "GROUND WATER" in c and "SURFACE WATER" in c:
        # Variants:
        #   lake gw_in gw_out sw_in sw_out
        #   lake gw_in gw_out sw_in sw_out water_use
        keys = ["gw_inflow", "gw_outflow", "sw_inflow", "sw_outflow", "water_use"]
        for key, val in zip(keys, vals):
            rec[key] = val
        return

    if "CONNECTED LAKE" in c or "SURFACE AREA" in c or "STAGE" in c or "DISCREPANCY" in c:
        # Variants:
        #   lake water_use connected_influx updated_volume surface_area stage_dt stage_cum
        #   lake connected_influx surface_area stage_dt stage_cum percent_discrepancy
        if "WATER" in c and "USE" in c and "UPDATED" in c:
            keys = [
                "water_use",
                "connected_lake_influx",
                "updated_volume",
                "surface_area",
                "stage_change_timestep",
                "stage_change_cumulative",
            ]
        elif "WATER" in c and "USE" in c and "CHANGE" in c and "VOL" in c:
            keys = ["water_use", "connected_lake_influx", "volume_change", "percent_discrepancy"]
        else:
            keys = [
                "connected_lake_influx",
                "surface_area",
                "stage_change_timestep",
                "stage_change_cumulative",
                "percent_discrepancy",
            ]
        for key, val in zip(keys, vals):
            rec[key] = val
        return


def parse_lak_block(
    lines: List[str],
    start_index: int,
    source_file: str,
    include_cumulative: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    """Parse one LAK summary block starting at start_index."""
    block_type = "cumulative" if "CUMULATIVE" in lines[start_index].upper() else "instantaneous"
    meta = find_previous_metadata(lines, start_index)
    records: Dict[int, Dict[str, Any]] = {}

    # End at next non-cumulative lake block, volumetric budget, UBUDSV, or long divider after data.
    i = start_index + 1
    last_header_context: Optional[str] = None
    prior_nonblank: List[str] = []

    while i < len(lines):
        line = lines[i]
        upper = line.upper()
        if i > start_index + 1 and "HYDROLOGIC BUDGET SUMMARIES FOR SIMULATED LAKES" in upper:
            break
        if i > start_index + 1 and ("VOLUMETRIC BUDGET FOR ENTIRE MODEL" in upper or upper.startswith("UBUDSV") or upper.startswith("HEAD WILL BE SAVED")):
            break

        stripped = line.strip()
        if not stripped:
            i += 1
            continue

        if stripped.startswith("-") and records:
            # A dashed line after we have data usually ends this block.
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j >= len(lines) or "HYDROLOGIC BUDGET" in lines[j].upper() or "UBUDSV" in lines[j].upper() or "VOLUMETRIC BUDGET" in lines[j].upper():
                i = j
                break

        if re.match(r"^\s*LAKE\b", line, flags=re.IGNORECASE):
            # Build the table context from the LAKE column-header line plus
            # immediately adjacent nonblank, non-data header lines above it.
            # This avoids carrying header text forward from earlier sub-tables.
            context_lines: List[str] = []
            j = i - 1
            while j > start_index and len(context_lines) < 3:
                prev = lines[j].strip()
                if not prev:
                    j -= 1
                    if context_lines:
                        break
                    continue
                if prev.startswith("-") or is_lake_data_line(lines[j]):
                    break
                context_lines.insert(0, lines[j])
                j -= 1
            context_lines.append(line)
            last_header_context = " ".join(s.strip() for s in context_lines)
            i += 1
            while i < len(lines) and is_lake_data_line(lines[i]):
                tokens = tokenize_values(lines[i])
                lake = int(tokens[0])
                rec = get_record(records, source_file, block_type, meta, lake)
                if last_header_context:
                    assign_table_values(last_header_context, tokens[1:], rec)
                i += 1
            continue

        prior_nonblank.append(line)
        if len(prior_nonblank) > 5:
            prior_nonblank = prior_nonblank[-5:]
        i += 1

    if block_type == "cumulative" and not include_cumulative:
        return [], i
    return [records[k] for k in sorted(records)], i


def parse_listing(
    listing_file: str | Path,
    block_start_text: str = BLOCK_START_TEXT,
    include_cumulative: bool = INCLUDE_CUMULATIVE_BLOCKS,
    listing_is_split: bool = LISTING_IS_SPLIT,
    split_last_index: int = SPLIT_LAST_INDEX,
) -> List[Dict[str, Any]]:
    """
    Parse all LAK hydrologic budget summary blocks from one or more
    MODFLOW listing files.
    """

    listing_files = build_listing_file_list(
        listing_file,
        listing_is_split,
        split_last_index,
    )

    rows: List[Dict[str, Any]] = []
    expected_nper = None
    found_periods = set()

    marker = block_start_text.strip().upper()

    for path in listing_files:

        if VERBOSE:
            print(f"Reading listing file: {path}")

        if not Path(path).exists():
            print(f"WARNING: Listing file not found: {path}")
            continue

        text = Path(path).read_text(errors="replace")
        lines = text.splitlines()

        #
        # Try to determine expected number of stress periods
        #
        if expected_nper is None:

            for line in lines:

                upper = line.upper()

                #
                # Common DIS echo formats
                #
                if "STRESS PERIODS" in upper:

                    nums = re.findall(r"\d+", line)

                    if nums:
                        expected_nper = int(nums[-1])

                        if VERBOSE:
                            print(
                                f"Detected expected number of stress periods: "
                                f"{expected_nper}"
                            )

                        break

        i = 0

        while i < len(lines):

            upper = lines[i].upper().strip()

            if marker in upper:

                block_rows, next_i = parse_lak_block(
                    lines,
                    i,
                    Path(path).name,
                    include_cumulative,
                )

                rows.extend(block_rows)

                #
                # Track discovered stress periods
                #
                for row in block_rows:

                    if row.get("per") is not None:
                        found_periods.add(row["per"])

                i = max(next_i, i + 1)

            else:
                i += 1

    #
    # Optional coverage warning
    #
    if (
        CHECK_STRESS_PERIOD_COVERAGE
        and expected_nper is not None
    ):

        missing = sorted(
            set(range(1, expected_nper + 1)) - found_periods
        )

        if missing:

            print(
                "WARNING: Did not find LAK budget summaries for "
                f"stress periods: {missing}"
            )

            print(
                "This may be normal for partial simulations "
                "(e.g. MODFLOW-OWHM restart/continuation runs)."
            )

    return rows


def write_csv(rows: Iterable[Dict[str, Any]], output_csv: str | Path) -> None:
    path = Path(output_csv)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract MODFLOW LAK listing-file budget summaries to CSV.")
    parser.add_argument("--listing", default=LISTING_FILE, help="Path to the MODFLOW listing file.")
    parser.add_argument(
        "--listing-is-split",
        action="store_true",
        default=LISTING_IS_SPLIT,
        help="Read a split listing-file sequence such as model.lst, model01.lst, model02.lst, etc.",
    )   
    parser.add_argument(
        "--split-last-index",
        type=int,
        default=SPLIT_LAST_INDEX,
        help="Highest split listing-file index to read when --listing-is-split is used.",
    )
    parser.add_argument("--output", default=OUTPUT_CSV, help="Path to the output CSV file.")
    parser.add_argument(
        "--block-start-text",
        default=BLOCK_START_TEXT,
        help="Case-insensitive text identifying the start of a lake-budget block.",
    )
    parser.add_argument(
        "--include-cumulative",
        action="store_true",
        default=INCLUDE_CUMULATIVE_BLOCKS,
        help="Also include cumulative lake-budget summaries when present.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress console summary.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    rows = parse_listing(
        args.listing,
        args.block_start_text,
        args.include_cumulative,
        args.listing_is_split,
        args.split_last_index,
    )
    write_csv(rows, args.output)
    if not args.quiet:
        n_blocks = len({(r.get("block_type"), r.get("per"), r.get("stp"), r.get("totim")) for r in rows})
        print(f"Wrote {len(rows)} lake-budget row(s) from {n_blocks} parsed block(s) to {args.output}")


if __name__ == "__main__":
    main()
