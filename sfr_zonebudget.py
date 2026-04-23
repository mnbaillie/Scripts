"""
SFR ZoneBudget v0.3

This script generates a ZoneBudget-style water budget of the surface water
network of a MODFLOW-OWHMv2 model. It takes as input the SFR input file (to
generate a network routing diagram), a user-supplied zone file, and the reach-
by-reach streamflow output file. It generates a spreadsheet with the surface
water budget for each zone each model timestep, accounting for the following
components:
    - FLOW_HEAD: Streamflow entering the zone from outside of the model
    - FLOW_SEEPAGE: Groundwater-surface water interaction (positive is loss to
                    groundwater to match groundwater ZoneBudget sign convention)
    - FLOW_OUTOFMODEL: Streamflow leaving the model domain from the zone
    - RUNOFF: Land surface runoff entering the stream network
    - FARM_DIVERSION_NET_QC: FMP diversions leaving the stream network
    - DIVERSION_INTERNAL_QC: SFR diversions that stay within the zone (not
                             included in the water budget calculation)
    - PRECIP: Direct precipitation into the stream network
    - STREAM_ET: Direct ET from the stream network
    - IN_FROM_ZONE_N: Inflow to this zone from zone N
    - OUT_TO_ZONE_N: Outflow from this zone to zone N
    - MASS_BALANCE_RESIDUAL: Difference between total inflows and outflows.
                             Because there is no storage in the stream network
                             in SFR, this should always be extremely small,
                             representing the effects of rounding of the
                             various water budget components. If the mass
                             balance residual is not insignificant, this
                             likely indicates an issue in the script.
The script generates a surface water budget for the entire stream network (tab
SFR_TOTAL), and automatically assigns any portion of the stream network not
given a zone number to zone 0.

Created by M. Baillie, West Yost, on 30 Dec 2025 with AI assistance. Please
contact mbaillie@westyost.com with questions, issues, and suggestions.

sfr_zonebudget_runfile.py

ZoneBudget-style surface-water balance for MODFLOW (MF-2005 lineage) SFR "DB" output.

Key features
------------
- Reads SFR input file (SFR2-style) to build a segment routing table (stress period 1),
  and writes a routing CSV for QC.
- Reads SFR output file:
    * ASCII table (whitespace- or comma-delimited), OR
    * Binary fixed-length record table like USGS "DB" binary (DATE_START + ints + doubles).
      (Zipped binary also supported.)
- Reads a zone configuration file with TWO options:
    * by-segment: Segment, Zone
    * by-reach: Segment, Reach, Zone
  Anything unspecified is assigned Zone = 0 (QC).
- Produces an Excel workbook:
    * README_METADATA tab
    * One tab per zone with a timeseries (one row per model timestep)
      and interzone exchange columns.
- Diversions:
    * Many SFR models report diversions as negative RUNOFF on the SOURCE reach.
    * RUNOFF at the source can be the sum of natural runoff (+) and diversion (-),
      so we compute a NET diversion indicator:
          DIVERSION_NET = max(0, -RUNOFF_reported)
    * We map diversion SOURCE->DESTINATION using SFR input (IUPSEG relationships),
      allocate DIVERSION_NET to destination segment(s), and treat cross-zone diversions
      as interzone transfers.
    * Internal (same-zone) net diversions are reported in DIVERSION_INTERNAL_QC.

IMPORTANT NOTES
---------------
- Diversion transfers are NET indicators (derived from negative RUNOFF), not guaranteed
  to equal "gross diversion". See README_METADATA in the output workbook.
- Interzone downstream transfers are computed at the SEGMENT level using segment outflow
  (FLOW_OUT at last reach). This is typically correct for routing boundaries; if you
  need reach-level boundary routing, we can extend using reach connectivity.

USER SETTINGS (EDIT THESE)
--------------------------
Set file paths below, then run this script (e.g., in Spyder) WITHOUT command-line args.
"""

from __future__ import annotations

import os
import re
import zipfile
import struct
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

# =========================
# USER SETTINGS (EDIT ME)
# =========================
SFR_INPUT_PATH = r"path.sfr"          # SFR input file (text)
SFR_OUTPUT_PATH = r"path\SFRDB.txt"  # ASCII or binary DB output; can be .zip
ZONE_CONFIG_PATH = r"path.csv"       # by-segment or by-reach
OUT_EXCEL_PATH = r"path.xlsx"

# Optional: choose where to write routing CSV (QC). If blank, writes next to OUT_EXCEL_PATH.
OUT_ROUTING_CSV_PATH = r"path\RoutingQC.csv"  # e.g. r"Y:\path\to\routing.csv"

# If your binary output is zipped and contains multiple files, set this to the member name.
# If blank, the script will use the first member in the zip.
ZIP_MEMBER_NAME = r""

# Unit conversion and output basis
# MODFLOW SFR output fluxes are in MODEL_VOLUME / MODEL_TIME.
# This script can report either:
#   - integrated volume per timestep ("PER_STRESS_PERIOD"), or
#   - average rate as volume-per-day ("PER_DAY").
#
# Volume units
# ------------
# Choose a MODEL_LENGTH_UNIT (what the model uses internally) and an OUTPUT_VOLUME_UNIT.
# Common OUTPUT_VOLUME_UNIT options include "ft3", "m3", and "acft" (acre-feet).
# If you prefer, set OUTPUT_VOLUME_UNIT="custom" and provide VOLUME_CONV_FACTOR.
#
# Notes:
# - MODFLOW outputs are always in cubic length units (e.g., ft^3 or m^3), not mixed units like acre-feet.
# - Conversion to acre-feet is therefore one-way (cubic length -> acre-feet).

MODEL_LENGTH_UNIT = "ft"          # "ft", "m", or "custom"
OUTPUT_VOLUME_UNIT = "acft"        # "ft3", "m3", "acft", or "custom"

# If MODEL_LENGTH_UNIT is "custom", provide the number of feet per model length unit.
# Example: inches -> FT_PER_MODEL_LEN = 1/12
FT_PER_MODEL_LEN = 1.0

# If OUTPUT_VOLUME_UNIT is "custom", provide conversion from model volume units to desired output volume units:
#   output_volume = model_volume * VOLUME_CONV_FACTOR
VOLUME_CONV_FACTOR = 1.0

# Time units
# ----------
# MODEL_TIME_UNIT_IN_DAYS: length of one model time unit in days.
#   e.g., model time unit is days  -> 1.0
#         model time unit is hours -> 1.0 / 24.0
#         model time unit is years -> 365.25
MODEL_TIME_UNIT_IN_DAYS = 1.0

# OUTPUT_BASIS controls how fluxes are reported in Excel:
#   "PER_STRESS_PERIOD" -> integrated volume over each timestep (volume per stress period)
#   "PER_DAY"           -> average rate over the timestep (volume per day)
OUTPUT_BASIS = "PER_STRESS_PERIOD"  # or "PER_DAY"

# Optional label for the output workbook metadata.
VOLUME_UNIT_LABEL = "ac-ft"  # e.g., "ac-ft", "m^3"


# =========================
# INTERNALS (no edits needed)
# =========================
INT_PAT = re.compile(r'^[+-]?\d+$')
FLOAT_PAT = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)([Ee][+-]?\d+)?$')


def _strip_inline_comments(line: str) -> str:
    for sep in ("#", ";", "!"):
        if sep in line:
            line = line.split(sep, 1)[0]
    # allow comma-delimited numeric fields in SFR input
    line = line.replace(",", " ")
    return line.rstrip("\n")


def _is_comment_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s.startswith(("#", ";", "!")):
        return True
    if line and line[0] in ("C", "c", "*"):
        return True
    return False


def _tokens(line: str) -> List[str]:
    return _strip_inline_comments(line).strip().split()


def infer_counts_sfr(lines: List[str], max_lines: int = 50000) -> Tuple[int, int, int, str]:
    # 1) annotated line containing NSTRM
    for i, line in enumerate(lines[:max_lines]):
        if "NSTRM" in line.upper() and not line.strip().startswith(("#", ";", "!", "C", "c", "*")):
            tk = _tokens(line)
            if len(tk) >= 2 and INT_PAT.match(tk[0]) and INT_PAT.match(tk[1]):
                return i, int(tk[0]), int(tk[1]), line.strip()

    # 2) fallback: plausible dataset 1c
    best = None
    for i, line in enumerate(lines[:max_lines]):
        if _is_comment_line(line):
            continue
        tk = _tokens(line)
        if len(tk) < 5:
            continue
        if not (INT_PAT.match(tk[0]) and INT_PAT.match(tk[1])):
            continue
        nstrm = int(tk[0])
        nss = int(tk[1])
        if nss <= 0 or abs(nstrm) < nss or nss > 500000 or abs(nstrm) > 50000000:
            continue
        if not all(FLOAT_PAT.match(t) or INT_PAT.match(t) for t in tk[: min(len(tk), 12)]):
            continue
        score = len(tk) + nss / 1000.0
        if best is None or score > best[0]:
            best = (score, i, nstrm, nss, line.strip())

    if best is None:
        raise ValueError("Could not infer NSTRM/NSS from the SFR input.")
    _, i, nstrm, nss, raw = best
    return i, nstrm, nss, raw


def _looks_like_segment_header(tk: List[str], expected_seg: int) -> bool:
    if len(tk) < 4:
        return False
    if not all(INT_PAT.match(x) for x in tk[:4]):
        return False
    seg = int(tk[0])
    icalc = int(tk[1])
    if seg != expected_seg:
        return False
    if icalc not in (0, 1, 2, 3, 4):
        return False
    return True


def _detect_reachinput(lines: List[str], search_lines: int = 100) -> bool:
    return any("REACHINPUT" in l.upper() for l in lines[:search_lines])


def _find_first_reach_record_line(lines: List[str], start_idx: int, lookahead: int = 500) -> Optional[int]:
    """
    Reach records (dataset 2) typically begin with at least 5 integers: K I J SEG RCH ...
    Search forward for the first non-comment line matching that shape.
    """
    for j in range(start_idx, min(len(lines), start_idx + lookahead)):
        if _is_comment_line(lines[j]):
            continue
        tk = _tokens(lines[j])
        if len(tk) >= 5 and all(INT_PAT.match(x) for x in tk[:5]):
            return j
    return None


def find_segment_block_start(lines: List[str], nstrm: int, counts_idx: int) -> int:
    """
    Locate the first stress-period segment block start.

    QC fix:
      - Many SFR files have the reach list immediately after the NSTRM/NSS line even without the
        REACHINPUT keyword. To avoid false positives, ALWAYS skip abs(NSTRM) reach records
        after the counts line (starting at the first reach-like record) before searching for segment headers.
    """
    scan_start = 0

    reach_start = _find_first_reach_record_line(lines, counts_idx + 1)
    if reach_start is not None:
        reach_count = 0
        j = reach_start
        while j < len(lines) and reach_count < abs(int(nstrm)):
            if _is_comment_line(lines[j]):
                j += 1
                continue
            tk = _tokens(lines[j])
            if len(tk) >= 5 and all(INT_PAT.match(x) for x in tk[:5]):
                reach_count += 1
            j += 1
        scan_start = max(scan_start, j)

    for i in range(scan_start, len(lines)):
        if _is_comment_line(lines[i]):
            continue
        tk = _tokens(lines[i])
        if _looks_like_segment_header(tk, 1):
            nonc = 0
            for j in range(i + 1, min(len(lines), i + 12000)):
                if _is_comment_line(lines[j]):
                    continue
                nonc += 1
                tk2 = _tokens(lines[j])
                if _looks_like_segment_header(tk2, 2):
                    return i
                if nonc > 2000:
                    break
    raise ValueError("Could not locate the start of the segment data block (stress period 1).")


def parse_sfr_routing_table(sfr_input_path: str) -> Dict:
    with open(sfr_input_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    idx_counts, nstrm, nss, counts_raw = infer_counts_sfr(lines)
    start = find_segment_block_start(lines, nstrm=nstrm, counts_idx=idx_counts)

    rows = []
    expected = 1
    i = start
    while expected <= nss and i < len(lines):
        if _is_comment_line(lines[i]):
            i += 1
            continue
        tk = _tokens(lines[i])
        if _looks_like_segment_header(tk, expected):
            seg = int(tk[0])
            icalc = int(tk[1])
            outseg = int(tk[2])
            iupseg = int(tk[3])
            rows.append(
                dict(
                    segment=seg,
                    icalc=icalc,
                    outseg=outseg,
                    iupseg=iupseg,
                    header_line_no=i + 1,
                    header_raw=_strip_inline_comments(lines[i]).strip(),
                )
            )
            expected += 1
        i += 1

    if expected <= nss:
        raise ValueError(f"Parsed only segments 1..{expected-1} of NSS={nss}.")

    rt = pd.DataFrame(rows)

    rt["outseg_norm"] = rt["outseg"].where(rt["outseg"].between(1, nss), 0).astype(int)
    rt["iupseg_norm"] = rt["iupseg"].where(rt["iupseg"].between(1, nss), 0).astype(int)
    rt["is_out_of_model"] = ~rt["outseg"].between(1, nss)
    rt["is_diversion_segment"] = rt["iupseg"].between(1, nss)

    inflow_targets = set(rt.loc[rt["outseg_norm"] > 0, "outseg_norm"].astype(int)).union(
        set(rt.loc[rt["iupseg_norm"] > 0, "segment"].astype(int))
    )
    rt["is_head_segment"] = ~rt["segment"].astype(int).isin(inflow_targets)

    return dict(
        nstrm=nstrm,
        nss=nss,
        counts_line_no=idx_counts + 1,
        counts_line=counts_raw,
        segblock_start_line_no=start + 1,
        routing_table=rt,
    )


def read_zone_config(zone_path: str) -> Tuple[str, pd.DataFrame]:
    """
    Read zone configuration.

    Supported:
      - by-segment: Segment, Zone
      - by-reach:   Segment, Reach, Zone

    Notes:
      - Column names are case-insensitive and whitespace-insensitive.
      - Blank lines and rows with missing required values are dropped.
      - Anything not specified later receives Zone=0 (QC).
    """
    # CSV first, else whitespace-delimited
    try:
        z = pd.read_csv(zone_path, comment="#")
    except Exception:
        z = pd.read_table(zone_path, sep=r"\s+", engine="python", comment="#")

    # normalize column names
    z.columns = [str(c).strip().lower() for c in z.columns]

    # drop fully empty rows
    z = z.dropna(how="all").copy()

    def _coerce_int(col: str) -> None:
        z[col] = pd.to_numeric(z[col], errors="coerce")

    if "reach" in z.columns:
        required = ["segment", "reach", "zone"]
        missing = [c for c in required if c not in z.columns]
        if missing:
            raise ValueError(f"By-reach zone file must have columns: Segment, Reach, Zone (missing: {missing})")

        for c in required:
            _coerce_int(c)

        z = z.dropna(subset=required).copy()
        if len(z) == 0:
            raise ValueError("Zone config has no valid rows after parsing (check headers/blank lines).")

        z["segment"] = z["segment"].astype(int)
        z["reach"] = z["reach"].astype(int)
        z["zone"] = z["zone"].astype(int)

        if (z["segment"] <= 0).any() or (z["reach"] <= 0).any():
            raise ValueError("Zone config contains non-positive Segment/Reach values.")
        return "reach", z[required].copy()

    else:
        required = ["segment", "zone"]
        missing = [c for c in required if c not in z.columns]
        if missing:
            raise ValueError(f"By-segment zone file must have columns: Segment, Zone (missing: {missing})")

        for c in required:
            _coerce_int(c)

        z = z.dropna(subset=required).copy()
        if len(z) == 0:
            raise ValueError("Zone config has no valid rows after parsing (check headers/blank lines).")

        z["segment"] = z["segment"].astype(int)
        z["zone"] = z["zone"].astype(int)

        if (z["segment"] <= 0).any():
            raise ValueError("Zone config contains non-positive Segment values.")
        return "segment", z[required].copy()


def _read_binary_member(path: str, member_name: str = "") -> Tuple[str, bytes]:
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path, "r") as zf:
            members = zf.namelist()
            if member_name and member_name in members:
                name = member_name
            else:
                name = members[0]
            return name, zf.read(name)
    else:
        with open(path, "rb") as f:
            return os.path.basename(path), f.read()


def read_sfr_output(path: str, zip_member: str = "") -> Tuple[str, str, pd.DataFrame]:
    """
    Returns (format, source_name, dataframe)
    format: 'binary_db' or 'ascii'
    """
    name, raw = _read_binary_member(path, zip_member)

    # Detect binary DB: first 19 bytes look like ASCII date with 'T'
    head = raw[:64]
    try:
        s19 = raw[:19].decode("ascii")
    except Exception:
        s19 = ""

    is_date19 = bool(re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", s19))
    if is_date19:
        # infer fixed record size by finding the next occurrence of the first timestamp
        ts0 = raw[:19]
        rec_size = raw.find(ts0, 19)
        if rec_size <= 0:
            raise ValueError("Binary DB detected but could not infer fixed record size.")
        header_fmt = "<19s i i d d i i"
        header = struct.Struct(header_fmt)
        header_size = header.size
        tail_bytes = rec_size - header_size
        if tail_bytes % 8 != 0:
            raise ValueError("Binary DB record tail is not a whole number of doubles.")
        n_tail = tail_bytes // 8
        tail_struct = struct.Struct("<" + "d" * n_tail)

        nrec = len(raw) // rec_size

        date_bytes = []
        per = np.empty(nrec, dtype=np.int32)
        stp = np.empty(nrec, dtype=np.int32)
        delt = np.empty(nrec, dtype=np.float64)
        simtime = np.empty(nrec, dtype=np.float64)
        seg = np.empty(nrec, dtype=np.int32)
        rch = np.empty(nrec, dtype=np.int32)
        tail = np.empty((nrec, n_tail), dtype=np.float64)

        mv = memoryview(raw)
        off = 0
        for i in range(nrec):
            chunk = mv[off:off + rec_size]
            ds, per[i], stp[i], delt[i], simtime[i], seg[i], rch[i] = header.unpack(chunk[:header_size])
            date_bytes.append(ds)
            tail[i, :] = tail_struct.unpack(chunk[header_size:])
            off += rec_size

        # column names from your ASCII header; extras preserved
        base_cols = ["DATE_START", "PER", "STP", "DELT", "SIMTIME", "SEG", "RCH"]
        known_tail = [
            "FLOW_IN", "FLOW_SEEPAGE", "FLOW_OUT", "RUNOFF", "PRECIP", "STREAM_ET",
            "HEAD_STREAM", "HEAD_AQUIFER", "DEPTH_STREAM", "WIDTH_STREAM", "LENGTH_STREAM",
            "HEAD_GRADIENT", "COND_STREAM", "ELEV_UP_STREAM"
        ]
        extra_cols = [f"EXTRA_{i+1}" for i in range(max(0, n_tail - len(known_tail)))]
        tail_cols = known_tail + extra_cols

        df = pd.DataFrame({
            "DATE_START": [b.decode("ascii", errors="ignore") for b in date_bytes],
            "PER": per,
            "STP": stp,
            "DELT": delt,
            "SIMTIME": simtime,
            "SEG": seg,
            "RCH": rch,
        })
        for j, c in enumerate(tail_cols):
            df[c] = tail[:, j]

        df["DATE_TIME"] = pd.to_datetime(df["DATE_START"].str.replace("T", " ", regex=False), errors="coerce")
        return "binary_db", name, df

    # Otherwise treat as ASCII text
    try:
        text = raw.decode("utf-8")
    except Exception:
        text = raw.decode("cp1252", errors="ignore")

    # Try CSV first, fallback to whitespace
    try:
        df = pd.read_csv(pd.io.common.StringIO(text), engine="python")
        fmt = "ascii"
    except Exception:
        df = pd.read_table(pd.io.common.StringIO(text), sep=r"\s+", engine="python")
        fmt = "ascii"

    df.columns = [c.strip() for c in df.columns]
    # normalize expected columns
    if "DATE_START" in df.columns and "DATE_TIME" not in df.columns:
        df["DATE_TIME"] = pd.to_datetime(df["DATE_START"].astype(str).str.replace("T", " ", regex=False), errors="coerce")
    # enforce ints
    for c in ("PER", "STP", "SEG", "RCH"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64").astype(int)
    for c in ("DELT", "SIMTIME"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return fmt, name, df


def build_zonebudget_excel(
    df: pd.DataFrame,
    routing: pd.DataFrame,
    zone_mode: str,
    zones: pd.DataFrame,
    out_xlsx: str,
    meta: Dict,
) -> None:
    # Build zone mapping
    if zone_mode == "segment":
        seg_to_zone = dict(zip(zones["segment"], zones["zone"]))
        df["ZONE"] = df["SEG"].map(seg_to_zone).fillna(0).astype(int)
        seg_zone = lambda s: seg_to_zone.get(int(s), 0)
        reach_zone = None
    else:
        key_to_zone = {(int(r.segment), int(r.reach)): int(r.zone) for r in zones.itertuples(index=False)}
        df["ZONE"] = [key_to_zone.get((int(s), int(r)), 0) for s, r in zip(df["SEG"].values, df["RCH"].values)]
        seg_zone = None
        reach_zone = lambda s, r: key_to_zone.get((int(s), int(r)), 0)

    # Zones list (include 0)
    zones_list = sorted(set(df["ZONE"].unique().tolist()) | {0})

    # Timestep keys
    tkeys = df[["DATE_TIME", "PER", "STP", "DELT", "SIMTIME"]].drop_duplicates().sort_values(["PER", "STP"]).reset_index(drop=True)

    # Segment-level routing dicts
    routing = routing.copy()
    routing["segment"] = routing["segment"].astype(int)
    seg_outseg = dict(zip(routing["segment"], routing["outseg_norm"].astype(int)))
    seg_iupseg = dict(zip(routing["segment"], routing["iupseg_norm"].astype(int)))
    seg_is_outmodel = dict(zip(routing["segment"], routing["is_out_of_model"].astype(bool)))

    # Diversion DEST mapping using IUPSEG: for each destination D with iupseg=S => S->D
    src_to_dests: Dict[int, List[int]] = {}
    for r in routing.itertuples(index=False):
        s = int(r.iupseg_norm)
        d = int(r.segment)
        if s > 0:
            src_to_dests.setdefault(s, []).append(d)

    # Segment inflow/outflow per timestep (use reach ordering)
    grp = ["PER", "STP", "SEG"]
    seg_in = df.sort_values(grp + ["RCH"]).groupby(grp, as_index=False).first()[["PER", "STP", "SEG", "FLOW_IN"]].rename(columns={"FLOW_IN": "SEG_FLOW_IN"})
    seg_out = df.sort_values(grp + ["RCH"]).groupby(grp, as_index=False).last()[["PER", "STP", "SEG", "FLOW_OUT"]].rename(columns={"FLOW_OUT": "SEG_FLOW_OUT"})
    seg_flow = seg_in.merge(seg_out, on=grp, how="outer").fillna(0.0)

    # Segment zones
    if zone_mode == "segment":
        seg_flow["ZONE"] = seg_flow["SEG"].map(seg_to_zone).fillna(0).astype(int)
    else:
        # approximate segment zone as the most common reach zone in that segment (timestep-invariant)
        seg_zone_mode = df.groupby("SEG")["ZONE"].agg(lambda s: int(s.value_counts().idxmax())).to_dict()
        seg_flow["ZONE"] = seg_flow["SEG"].map(seg_zone_mode).fillna(0).astype(int)

    # Add routing columns
    seg_flow["OUTSEG"] = seg_flow["SEG"].map(seg_outseg).fillna(0).astype(int)
    seg_flow["IS_OUTMODEL"] = seg_flow["SEG"].map(seg_is_outmodel).fillna(True).astype(bool)

    # Inbound from upstream routing (downstream edges)
    up_to_down = seg_flow[["PER", "STP", "SEG", "OUTSEG", "SEG_FLOW_OUT"]].copy()
    up_to_down = up_to_down[up_to_down["OUTSEG"] > 0]
    inbound = up_to_down.groupby(["PER", "STP", "OUTSEG"], as_index=False)["SEG_FLOW_OUT"].sum().rename(columns={"OUTSEG": "SEG", "SEG_FLOW_OUT": "INBOUND_FROM_UPSTREAM"})
    seg_flow = seg_flow.merge(inbound, on=["PER", "STP", "SEG"], how="left")
    seg_flow["INBOUND_FROM_UPSTREAM"] = seg_flow["INBOUND_FROM_UPSTREAM"].fillna(0.0)    # ---------------- Diversions ----------------
    # Two diversion mechanisms are handled:
    #
    # (A) SFR-defined diversions (defined in the SFR input file):
    #     These are not reported as outflow from the source segment. Instead, they appear as FLOW_IN
    #     to the diversion (destination) segment. The diversion source is given by IUPSEG for the
    #     destination segment.
    #     For diversion destination segments (IUPSEG>0), we estimate diverted inflow as:
    #         DIV_SFR_TO_DEST = max(0, SEG_FLOW_IN(dest) - INBOUND_FROM_UPSTREAM(dest))
    #
    # (B) Farm Process semi-routed diversions:
    #     These may be indicated by negative RUNOFF at the diversion SOURCE reach/segment.
    #     Because RUNOFF can include natural runoff (+) and diversion (-), this is a NET indicator:
    #         DIV_FARM_NET_SRC = max(0, -RUNOFF_reported)
    #
    # For zone-boundary accounting we compute:
    #   - Cross-zone diversion transfers from (A) using IUPSEG mapping.
    #   - Additional cross-zone diversion transfers from (B) as residual when not already represented by (A).

    # --- (B) Farm net diversion indicator at SOURCE segment ---
    df["DIV_FARM_NET_SRC_REACH"] = np.where(df["RUNOFF"] < 0, -df["RUNOFF"], 0.0)
    div_farm_src_seg = df.groupby(["PER", "STP", "SEG"], as_index=False)["DIV_FARM_NET_SRC_REACH"].sum().rename(
        columns={"DIV_FARM_NET_SRC_REACH": "DIV_FARM_NET_SRC_SEG"}
    )
    seg_flow = seg_flow.merge(div_farm_src_seg, on=["PER", "STP", "SEG"], how="left")
    seg_flow["DIV_FARM_NET_SRC_SEG"] = seg_flow["DIV_FARM_NET_SRC_SEG"].fillna(0.0)

    # --- (A) SFR-defined diversion inflow to destination segments (IUPSEG>0) ---
    seg_flow["IUPSEG"] = seg_flow["SEG"].map(seg_iupseg).fillna(0).astype(int)
    is_div_dest = seg_flow["IUPSEG"] > 0
    seg_flow["DIV_SFR_TO_DEST"] = 0.0
    seg_flow.loc[is_div_dest, "DIV_SFR_TO_DEST"] = (seg_flow.loc[is_div_dest, "SEG_FLOW_IN"] - seg_flow.loc[is_div_dest, "INBOUND_FROM_UPSTREAM"]).clip(lower=0.0)

    # Transfer table (SFR-defined): source=IUPSEG(dest) -> dest=SEG
    div_sfr_tr = seg_flow[is_div_dest & (seg_flow["DIV_SFR_TO_DEST"] > 0)][
        ["PER", "STP", "IUPSEG", "SEG", "DIV_SFR_TO_DEST"]
    ].copy()
    div_sfr_tr = div_sfr_tr.rename(columns={"IUPSEG": "FROM_SEG", "SEG": "TO_SEG", "DIV_SFR_TO_DEST": "Q"})
    if div_sfr_tr.empty:
        div_sfr_tr = pd.DataFrame(columns=["PER", "STP", "FROM_SEG", "TO_SEG", "Q"])

    # Totals per diversion SOURCE segment (used to avoid double-counting in downstream transfers)
    div_sfr_from = div_sfr_tr.groupby(["PER", "STP", "FROM_SEG"], as_index=False)["Q"].sum().rename(
        columns={"FROM_SEG": "SEG", "Q": "DIV_SFR_FROM_SOURCE"}
    )
    seg_flow = seg_flow.merge(div_sfr_from, on=["PER", "STP", "SEG"], how="left")
    seg_flow["DIV_SFR_FROM_SOURCE"] = seg_flow["DIV_SFR_FROM_SOURCE"].fillna(0.0)


    # Totals per source represented by SFR-defined diversions
    sfr_by_source = div_sfr_tr.groupby(["PER", "STP", "FROM_SEG"], as_index=False)["Q"].sum().rename(columns={"Q": "DIV_SFR_FROM_SOURCE"})

    # --- Residual FARM diversions ---
    # NOTE: FMP semi-routed diversions (negative RUNOFF) are treated as OUTFLOW from the stream system
    # and are NOT included in interzone transfers (they should never appear in IN_FROM_ZONE_* / OUT_TO_ZONE_*).
    #
    # Therefore, interzone diversion transfers include ONLY SFR-defined diversions from div_sfr_tr.
    div_tr = div_sfr_tr.copy()
    if div_tr.empty:
        div_tr = pd.DataFrame(columns=["PER", "STP", "FROM_SEG", "TO_SEG", "Q"])

    # Determine zone of segments
    if zone_mode == "segment":
        div_tr["FROM_ZONE"] = div_tr["FROM_SEG"].map(seg_to_zone).fillna(0).astype(int)
        div_tr["TO_ZONE"] = div_tr["TO_SEG"].map(seg_to_zone).fillna(0).astype(int)
    else:
        # use seg_zone_mode computed above
        div_tr["FROM_ZONE"] = div_tr["FROM_SEG"].map(seg_zone_mode).fillna(0).astype(int)
        div_tr["TO_ZONE"] = div_tr["TO_SEG"].map(seg_zone_mode).fillna(0).astype(int)

    # Split into internal diversion QC vs interzone diversion transfer
    internal_div = div_tr[div_tr["FROM_ZONE"] == div_tr["TO_ZONE"]].groupby(["PER", "STP", "FROM_ZONE"], as_index=False)["Q"].sum()
    internal_div = internal_div.rename(columns={"FROM_ZONE": "ZONE", "Q": "DIVERSION_INTERNAL_QC"})
    div_xzone = div_tr[div_tr["FROM_ZONE"] != div_tr["TO_ZONE"]].copy()

    # Downstream interzone transfers (segment outflow)
    # If segment routes to OUTSEG within model and zones differ, transfer = SEG_FLOW_OUT
    if zone_mode == "segment":
        out_zone = seg_flow["OUTSEG"].map(seg_to_zone).fillna(0).astype(int)
    else:
        out_zone = seg_flow["OUTSEG"].map(seg_zone_mode).fillna(0).astype(int)
    seg_flow["OUT_ZONE"] = out_zone

    down_tr = seg_flow[(seg_flow["OUTSEG"] > 0) & (seg_flow["ZONE"] != seg_flow["OUT_ZONE"])][
        ["PER", "STP", "SEG", "OUTSEG", "SEG_FLOW_OUT", "DIV_SFR_FROM_SOURCE", "ZONE", "OUT_ZONE"]
    ].copy()
    # IMPORTANT: SEG_FLOW_OUT at diversion sources can include water diverted to SFR diversion segments.
    # To avoid double-counting, subtract SFR-defined diversions taken from the source when computing
    # downstream interzone transfers.
    down_tr["Q"] = (down_tr["SEG_FLOW_OUT"] - down_tr["DIV_SFR_FROM_SOURCE"].fillna(0.0)).clip(lower=0.0)
    down_tr = down_tr.rename(columns={"SEG": "FROM_SEG", "OUTSEG": "TO_SEG", "ZONE": "FROM_ZONE", "OUT_ZONE": "TO_ZONE"})
    down_tr["TYPE"] = "DOWNSTREAM"

    div_xzone["TYPE"] = "DIVERSION_NET"

    # Combine transfers (for IN/OUT columns)
    all_tr = pd.concat([
        down_tr[["PER","STP","FROM_ZONE","TO_ZONE","Q","TYPE"]],
        div_xzone[["PER","STP","FROM_ZONE","TO_ZONE","Q","TYPE"]]
    ], ignore_index=True)
    tr_sum = all_tr.groupby(["PER","STP","FROM_ZONE","TO_ZONE"], as_index=False)["Q"].sum()

    # Approx diverted inflow to destination segments for headwater separation:
    # sum of allocated Q for each TO_SEG
    div_in_seg = div_tr.groupby(["PER","STP","TO_SEG"], as_index=False)["Q"].sum().rename(columns={"TO_SEG":"SEG","Q":"DIV_NET_IN_SEG"})
    seg_flow = seg_flow.merge(div_in_seg, on=["PER","STP","SEG"], how="left")
    seg_flow["DIV_NET_IN_SEG"] = seg_flow["DIV_NET_IN_SEG"].fillna(0.0)

    # Headwater external flow per segment: inflow - inbound_from_upstream - diverted_in (net indicator)
    seg_flow["HEAD_EXT"] = seg_flow["SEG_FLOW_IN"] - seg_flow["INBOUND_FROM_UPSTREAM"] - seg_flow["DIV_NET_IN_SEG"]
    seg_flow["HEAD_EXT"] = seg_flow["HEAD_EXT"].where(seg_flow["HEAD_EXT"] > 1e-9, 0.0)

    # Reach-level terms by zone
    df["RUNOFF_POS"] = df["RUNOFF"].where(df["RUNOFF"] > 0, 0.0)
    df["FARM_DIV_OUT_REACH"] = np.where(df["RUNOFF"] < 0, -df["RUNOFF"], 0.0)
    zone_reach_terms = df.groupby(["PER","STP","ZONE"], as_index=False).agg(
        FLOW_SEEPAGE=("FLOW_SEEPAGE","sum"),
        RUNOFF=("RUNOFF_POS","sum"),
        PRECIP=("PRECIP","sum"),
        STREAM_ET=("STREAM_ET","sum"),
    )
    farm_div_zone = df.groupby(["PER","STP","ZONE"], as_index=False)["FARM_DIV_OUT_REACH"].sum().rename(columns={"FARM_DIV_OUT_REACH":"FARM_DIVERSION_NET_QC"})


    zone_head = seg_flow.groupby(["PER","STP","ZONE"], as_index=False)["HEAD_EXT"].sum().rename(columns={"HEAD_EXT":"FLOW_HEAD"})

    zone_outmodel = seg_flow[seg_flow["IS_OUTMODEL"]].groupby(["PER","STP","ZONE"], as_index=False)["SEG_FLOW_OUT"].sum().rename(columns={"SEG_FLOW_OUT":"FLOW_OUTOFMODEL"})

    # helper to build per-zone timeseries
    def zone_ts(z: int) -> pd.DataFrame:
        ts = tkeys.copy()
        ts["ZONE"] = z

        sub = zone_reach_terms[zone_reach_terms["ZONE"] == z].drop(columns=["ZONE"])
        ts = ts.merge(sub, on=["PER","STP"], how="left")
        fd = farm_div_zone[farm_div_zone["ZONE"] == z].drop(columns=["ZONE"])
        ts = ts.merge(fd, on=["PER","STP"], how="left")

        zh = zone_head[zone_head["ZONE"] == z].drop(columns=["ZONE"])
        ts = ts.merge(zh, on=["PER","STP"], how="left")

        zom = zone_outmodel[zone_outmodel["ZONE"] == z].drop(columns=["ZONE"])
        ts = ts.merge(zom, on=["PER","STP"], how="left")

        idv = internal_div[internal_div["ZONE"] == z].drop(columns=["ZONE"])
        ts = ts.merge(idv, on=["PER","STP"], how="left")

        for c in ["FLOW_SEEPAGE","RUNOFF","FARM_DIVERSION_NET_QC","PRECIP","STREAM_ET","FLOW_HEAD","FLOW_OUTOFMODEL","DIVERSION_INTERNAL_QC"]:
            if c not in ts.columns:
                ts[c] = 0.0
            ts[c] = ts[c].fillna(0.0)

        for k in zones_list:
            ts[f"IN_FROM_ZONE_{k}"] = 0.0
            ts[f"OUT_TO_ZONE_{k}"] = 0.0

        inz = tr_sum[tr_sum["TO_ZONE"] == z]
        for _, r in inz.iterrows():
            ts.loc[(ts["PER"] == r["PER"]) & (ts["STP"] == r["STP"]), f"IN_FROM_ZONE_{int(r['FROM_ZONE'])}"] = float(r["Q"])

        outz = tr_sum[tr_sum["FROM_ZONE"] == z]
        for _, r in outz.iterrows():
            ts.loc[(ts["PER"] == r["PER"]) & (ts["STP"] == r["STP"]), f"OUT_TO_ZONE_{int(r['TO_ZONE'])}"] = float(r["Q"])

        in_cols = [f"IN_FROM_ZONE_{k}" for k in zones_list]
        out_cols = [f"OUT_TO_ZONE_{k}" for k in zones_list]
        ts["MASS_BALANCE_RESIDUAL"] = (
            ts["FLOW_HEAD"] + ts["RUNOFF"] + ts["PRECIP"] + ts[in_cols].sum(axis=1)
            - (ts["FLOW_SEEPAGE"] + ts["STREAM_ET"] + ts["FLOW_OUTOFMODEL"] + ts["FARM_DIVERSION_NET_QC"] + ts[out_cols].sum(axis=1))
        )

        core = ["DATE_TIME","PER","STP","DELT","SIMTIME","ZONE",
                "FLOW_HEAD","FLOW_SEEPAGE","FLOW_OUTOFMODEL","RUNOFF","FARM_DIVERSION_NET_QC","DIVERSION_INTERNAL_QC","PRECIP","STREAM_ET"]
        inter = []
        for k in zones_list:
            inter += [f"IN_FROM_ZONE_{k}", f"OUT_TO_ZONE_{k}"]
        final_cols = core + inter + ["MASS_BALANCE_RESIDUAL"]
        return ts[final_cols]

    zone_tabs = {z: zone_ts(z) for z in zones_list}

    # ===== System-wide SFR balance (all zones combined) =====
    # Interzone transfers should cancel at the system scale; we report a total-balance tab for comparison
    # against groundwater budget SFR leakage term.
    sys_ts = tkeys.copy()
    sys_ts["SFR_SYSTEM"] = "ALL"

    # Sum reach-based terms across all zones
    sys_reach = df.groupby(["PER","STP"], as_index=False).agg(
        FLOW_SEEPAGE=("FLOW_SEEPAGE","sum"),
        RUNOFF=("RUNOFF_POS","sum"),
        PRECIP=("PRECIP","sum"),
        STREAM_ET=("STREAM_ET","sum"),
    )
    sys_ts = sys_ts.merge(sys_reach, on=["PER","STP"], how="left")

    # Sum headwater external inflow across all segments
    sys_head = seg_flow.groupby(["PER","STP"], as_index=False)["HEAD_EXT"].sum().rename(columns={"HEAD_EXT":"FLOW_HEAD"})
    sys_ts = sys_ts.merge(sys_head, on=["PER","STP"], how="left")

    # Out-of-model outflow across all segments
    sys_out = seg_flow[seg_flow["IS_OUTMODEL"]].groupby(["PER","STP"], as_index=False)["SEG_FLOW_OUT"].sum().rename(columns={"SEG_FLOW_OUT":"FLOW_OUTOFMODEL"})
    sys_ts = sys_ts.merge(sys_out, on=["PER","STP"], how="left")

    # Total net diversion indicator across all diversion sources (QC)
    sys_div = df.groupby(["PER","STP"], as_index=False)["FARM_DIV_OUT_REACH"].sum().rename(columns={"FARM_DIV_OUT_REACH":"FARM_DIVERSION_NET_TOTAL_QC"})
    sys_ts = sys_ts.merge(sys_div, on=["PER","STP"], how="left")

    # Interzone transfer total (QC only; this is sum of all cross-zone transfers and does not enter the system balance)
    sys_xfer = tr_sum.groupby(["PER","STP"], as_index=False)["Q"].sum().rename(columns={"Q":"INTERZONE_TOTAL_QC"})
    sys_ts = sys_ts.merge(sys_xfer, on=["PER","STP"], how="left")

    for c in ["FLOW_SEEPAGE","RUNOFF","PRECIP","STREAM_ET","FLOW_HEAD","FLOW_OUTOFMODEL","FARM_DIVERSION_NET_TOTAL_QC","INTERZONE_TOTAL_QC"]:
        if c not in sys_ts.columns:
            sys_ts[c] = 0.0
        sys_ts[c] = sys_ts[c].fillna(0.0)

    # System mass balance residual (no interzone terms; they cancel at system scale)
    sys_ts["MASS_BALANCE_RESIDUAL_SYSTEM"] = (
        sys_ts["FLOW_HEAD"] + sys_ts["RUNOFF"] + sys_ts["PRECIP"]
        - (sys_ts["FLOW_SEEPAGE"] + sys_ts["STREAM_ET"] + sys_ts["FLOW_OUTOFMODEL"] + sys_ts["FARM_DIVERSION_NET_TOTAL_QC"])
    )
    sys_cols = [
        "DATE_TIME","PER","STP","DELT","SIMTIME","SFR_SYSTEM",
        "FLOW_HEAD","FLOW_SEEPAGE","FLOW_OUTOFMODEL","RUNOFF",
        "FARM_DIVERSION_NET_TOTAL_QC","PRECIP","STREAM_ET",
        "INTERZONE_TOTAL_QC","MASS_BALANCE_RESIDUAL_SYSTEM"
    ]
    sys_ts = sys_ts[sys_cols]

    
    # =========================
    # Apply unit/time scaling
    # =========================
    # Compute model_volume -> output_volume factor (model_volume is in model_length^3)
    FT_PER_M = 3.280839895013123
    FT3_PER_M3 = FT_PER_M ** 3
    FT3_PER_ACFT = 43560.0  # 1 acre-foot = 43,560 ft^3

    if OUTPUT_VOLUME_UNIT.lower() == "custom":
        vol_factor = float(VOLUME_CONV_FACTOR)
    else:
        ml = MODEL_LENGTH_UNIT.lower()
        if ml == "ft":
            model_ft3_per_model_vol = 1.0
        elif ml == "m":
            model_ft3_per_model_vol = FT3_PER_M3
        elif ml == "custom":
            model_ft3_per_model_vol = float(FT_PER_MODEL_LEN) ** 3
        else:
            raise ValueError(f"Unrecognized MODEL_LENGTH_UNIT: {MODEL_LENGTH_UNIT}")

        outu = OUTPUT_VOLUME_UNIT.lower()
        if outu == "ft3":
            vol_factor = model_ft3_per_model_vol
        elif outu == "m3":
            vol_factor = model_ft3_per_model_vol / FT3_PER_M3
        elif outu in ("acft", "acre-ft", "acreft"):
            vol_factor = model_ft3_per_model_vol / FT3_PER_ACFT
        else:
            raise ValueError(f"Unrecognized OUTPUT_VOLUME_UNIT: {OUTPUT_VOLUME_UNIT}")

    def _scale_df(df_in: pd.DataFrame, is_system: bool = False) -> pd.DataFrame:
        df = df_in.copy()
        base_cols = [
            "FLOW_HEAD", "FLOW_SEEPAGE", "FLOW_OUTOFMODEL",
            "RUNOFF", "PRECIP", "STREAM_ET",
        ]
        extra_cols = [
            "FARM_DIVERSION_NET_QC",
            "DIVERSION_INTERNAL_QC",
            "FARM_DIVERSION_NET_TOTAL_QC",
            "INTERZONE_TOTAL_QC",
        ]
        flux_cols = [c for c in base_cols + extra_cols if c in df.columns]
        inter_cols = [c for c in df.columns if c.startswith("IN_FROM_ZONE_") or c.startswith("OUT_TO_ZONE_")]

        all_scale_cols = flux_cols + inter_cols
        if is_system and "MASS_BALANCE_RESIDUAL_SYSTEM" in df.columns:
            all_scale_cols.append("MASS_BALANCE_RESIDUAL_SYSTEM")
        if (not is_system) and "MASS_BALANCE_RESIDUAL" in df.columns:
            all_scale_cols.append("MASS_BALANCE_RESIDUAL")

        if OUTPUT_BASIS.upper() == "PER_STRESS_PERIOD":
            factor = df["DELT"].astype(float) * vol_factor
        elif OUTPUT_BASIS.upper() == "PER_DAY":
            if MODEL_TIME_UNIT_IN_DAYS == 0:
                raise ValueError("MODEL_TIME_UNIT_IN_DAYS must be non-zero for OUTPUT_BASIS='PER_DAY'.")
            factor = pd.Series(vol_factor / MODEL_TIME_UNIT_IN_DAYS, index=df.index)
        else:
            raise ValueError(f"Unrecognized OUTPUT_BASIS: {OUTPUT_BASIS}")

        for c in all_scale_cols:
            df[c] = df[c].astype(float) * factor

        return df

    # Scale zone tabs
    for z in zone_tabs:
        zone_tabs[z] = _scale_df(zone_tabs[z], is_system=False)

    # Scale system tab
    sys_ts = _scale_df(sys_ts, is_system=True)

# Write workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "README_METADATA"

    meta_rows = [
        ("Generated", datetime.now().isoformat(timespec="seconds")),
        ("SFR input", meta.get("sfr_input","")),
        ("SFR output", meta.get("sfr_output","")),
        ("SFR output member", meta.get("sfr_output_member","")),
        ("SFR output format", meta.get("sfr_output_format","")),
        ("Zone config", meta.get("zone_config","")),
        ("Zone mode", zone_mode),
        ("Default zone for unspecified", 0),
        ("Routing CSV (QC)", meta.get("routing_csv","")),
        ("NSTRM", meta.get("nstrm","")),
        ("NSS", meta.get("nss","")),
        ("Timestep count", len(tkeys)),
        ("Zones present (including 0)", ", ".join(str(z) for z in zones_list)),
        ("Diversion method", "SFR-defined diversions stay in the stream system (accounted via IUPSEG). FMP semi-routed diversions (negative RUNOFF) leave the stream system and are treated as FARM_DIVERSION_NET."),
        ("Diversion caveat", "RUNOFF at diversion sources may include natural runoff (+) and diversion (-); FARM_DIVERSION_NET=max(0,-RUNOFF) is a net indicator, not guaranteed gross diversion."),
        ("Output basis", OUTPUT_BASIS),
        ("Model length unit", MODEL_LENGTH_UNIT),
        ("Output volume unit", OUTPUT_VOLUME_UNIT),
        ("Custom volume factor", VOLUME_CONV_FACTOR if OUTPUT_VOLUME_UNIT.lower()=="custom" else ""),
                ("Model time unit in days", MODEL_TIME_UNIT_IN_DAYS),
        ("Volume units label", VOLUME_UNIT_LABEL),
        ("Notes", "DATE_START parsed to DATE_TIME after replacing 'T' with space. All reported flux terms are scaled to either integrated volume per stress period or average rate in volume-per-day, depending on OUTPUT_BASIS. MASS_BALANCE_RESIDUAL is in the same units."),
    ]

    ws.append(["Field", "Value"])
    ws["A1"].font = Font(bold=True)
    ws["B1"].font = Font(bold=True)
    for k, v in meta_rows:
        ws.append([k, v])

    ws.freeze_panes = "A2"
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 120

    # System-wide tab
    ws_sys = wb.create_sheet(title="SFR_TOTAL")
    for r in dataframe_to_rows(sys_ts, index=False, header=True):
        ws_sys.append(r)
    for cell in ws_sys[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws_sys.freeze_panes = "A2"
    ws_sys.auto_filter.ref = ws_sys.dimensions
    ws_sys.column_dimensions["A"].width = 20
    ws_sys.column_dimensions["B"].width = 6
    ws_sys.column_dimensions["C"].width = 6
    ws_sys.column_dimensions["D"].width = 10
    ws_sys.column_dimensions["E"].width = 12
    ws_sys.column_dimensions["F"].width = 12
    for col in range(7, ws_sys.max_column + 1):
        ws_sys.column_dimensions[ws_sys.cell(row=1, column=col).column_letter].width = 20

    for z, zdf in zone_tabs.items():
        title = f"Zone_{z}"[:31]
        wz = wb.create_sheet(title=title)
        for r in dataframe_to_rows(zdf, index=False, header=True):
            wz.append(r)
        for cell in wz[1]:
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        wz.freeze_panes = "A2"
        wz.auto_filter.ref = wz.dimensions
        # basic widths
        wz.column_dimensions["A"].width = 20
        wz.column_dimensions["B"].width = 6
        wz.column_dimensions["C"].width = 6
        wz.column_dimensions["D"].width = 10
        wz.column_dimensions["E"].width = 12
        wz.column_dimensions["F"].width = 6
        for col in range(7, wz.max_column + 1):
            wz.column_dimensions[wz.cell(row=1, column=col).column_letter].width = 16

    os.makedirs(os.path.dirname(out_xlsx) or ".", exist_ok=True)
    wb.save(out_xlsx)


def run():
    # Output paths
    routing_csv = OUT_ROUTING_CSV_PATH.strip()
    if not routing_csv:
        base = os.path.splitext(OUT_EXCEL_PATH)[0]
        routing_csv = base + "_routing.csv"

    # Parse routing and write QC CSV
    routing_res = parse_sfr_routing_table(SFR_INPUT_PATH)
    routing = routing_res["routing_table"].copy()
    routing.to_csv(routing_csv, index=False)

    # Read zones
    zone_mode, zones = read_zone_config(ZONE_CONFIG_PATH)

    # Read SFR output
    fmt, member, df = read_sfr_output(SFR_OUTPUT_PATH, ZIP_MEMBER_NAME)

    # Validate required columns
    needed = ["DATE_TIME","PER","STP","DELT","SIMTIME","SEG","RCH","FLOW_IN","FLOW_OUT","RUNOFF","PRECIP","STREAM_ET","FLOW_SEEPAGE"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"SFR output missing required columns: {missing}")

    meta = dict(
        sfr_input=SFR_INPUT_PATH,
        sfr_output=SFR_OUTPUT_PATH,
        sfr_output_member=member,
        sfr_output_format=fmt,
        zone_config=ZONE_CONFIG_PATH,
        routing_csv=routing_csv,
        nstrm=routing_res["nstrm"],
        nss=routing_res["nss"],
    )

    build_zonebudget_excel(df, routing, zone_mode, zones, OUT_EXCEL_PATH, meta)

    print("=== SFR ZoneBudget ===")
    print(f"Wrote routing CSV: {routing_csv}")
    print(f"Wrote Excel:       {OUT_EXCEL_PATH}")


if __name__ == "__main__":
    run()
