#!/usr/bin/env python3
"""
This script provides limited QC of the SFR routing of a MODFLOW model. It
inspects the segment block of an SFR input file, and provides a summary of how
each segment is connected to the rest of the stream network, including
indicating whether each segment is a head segment (i.e., no other SFR segment
flows into this segment), a diversion segment, or a segment that flows out of
the model (or otherwise does not have an outflow segment defined).

Created by M. Baillie, West Yost, on 30 Dec 2025 with AI assistance.

sfr_routing_qc.py

Parse an SFR2-style input file and produce a segment routing table (stress period 1),
including:
  - downstream segment (OUTSEG)
  - diversion source segment (IUPSEG)
  - flags: is_out_of_model, is_diversion_segment, is_head_segment

Also optionally writes a Graphviz .dot network diagram (topology only; no physical geometry).
If you cannot visualize a .dot network diagram locally, use
https://dreampuf.github.io/GraphvizOnline/?engine=dot and output an image.

Works with many MF-2005 lineage variants (MF-NWT, MF-OWHM) that use numeric segment
header lines beginning with: NSEG ICALC OUTSEG IUPSEG ...

Notes:
- Some models use sentinel OUTSEG values like 9999 to indicate flow leaving the model.
  This script treats any OUTSEG not in [1..NSS] as "out of model" (outseg_norm = 0).
- Head segments are inferred: segments that are not the target of any downstream edge,
  and are not diversion-receiving segments (iupseg between 1..NSS implies it receives diversion flow).

Usage:
  python sfr_routing_qc.py --sfr-input path/to/file.sfr --out-csv routing.csv --out-dot routing.dot
"""

from __future__ import annotations

import argparse
import sys
import re
from typing import List, Optional, Tuple

import pandas as pd

INT_PAT = re.compile(r'^[+-]?\d+$')
FLOAT_PAT = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)([Ee][+-]?\d+)?$')

def _strip_inline_comments(line: str) -> str:
    # remove common inline comment separators
    for sep in ("#", ";", "!"):
        if sep in line:
            line = line.split(sep, 1)[0]
    return line.rstrip("\n")


def _is_comment_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s.startswith(("#", ";", "!")):
        return True
    # MODFLOW often uses "C" in col 1 for comments
    if line and line[0] in ("C", "c", "*"):
        return True
    return False


def _tokens(line: str) -> List[str]:
    """Tokenize a line, handling both whitespace- and comma-separated SFR files."""
    s = _strip_inline_comments(line)
    # Many legacy SFR2 files use comma-separated values.
    s = s.replace(',', ' ')
    return s.strip().split()


def infer_counts_sfr(lines: List[str], max_lines: int = 50000) -> Tuple[int, int, int, str]:
    """
    Infer NSTRM and NSS from the SFR input.

    Preferred:
      - a numeric line that includes an inline comment mentioning NSTRM

    Fallback:
      - a numeric line with >=5 numeric tokens where token0 and token1 are ints and appear plausible
    """
    # 1) Preferred: annotated line containing 'NSTRM'
    for i, line in enumerate(lines[:max_lines]):
        if "NSTRM" in line.upper() and not line.strip().startswith(("#", ";", "!", "C", "c", "*")):
            tk = _tokens(line)
            if len(tk) >= 2 and INT_PAT.match(tk[0]) and INT_PAT.match(tk[1]):
                return i, int(tk[0]), int(tk[1]), line.strip()

    # 2) Fallback: score plausible dataset-1c lines
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
        raise ValueError("Could not infer NSTRM/NSS (dataset 1c) from the SFR input.")
    _, i, nstrm, nss, raw = best
    return i, nstrm, nss, raw


def _looks_like_segment_header_relaxed(tk: List[str], expected_seg: int) -> bool:
    """
    Segment header must begin with 4 ints: NSEG ICALC OUTSEG IUPSEG.
    We validate NSEG==expected and ICALC in 0..4.
    OUTSEG/IUPSEG bounds are handled later (some models use OUTSEG=9999).
    """
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


def find_segment_block_start(lines: List[str], nss: int, idx_counts: int, nstrm: int) -> int:
    """
    Locate the first stress period segment block start by finding a segment-1 header
    and confirming a segment-2 header nearby.
    """
    # Dataset 2 has one reach line per reach; segment data begins after those reach records.
    search_start = max(0, idx_counts + 1 + abs(int(nstrm)))
    for i in range(search_start, len(lines)):
        line = lines[i]
        if _is_comment_line(line):
            continue
        tk = _tokens(line)
        if _looks_like_segment_header_relaxed(tk, 1):
            nonc = 0
            for j in range(i + 1, min(len(lines), i + 5000)):
                if _is_comment_line(lines[j]):
                    continue
                nonc += 1
                tk2 = _tokens(lines[j])
                if _looks_like_segment_header_relaxed(tk2, 2):
                    return i
                if nonc > 500:
                    break
    raise ValueError("Could not locate the start of the segment data block (stress period 1).")


def parse_routing_table(sfr_input_path: str) -> dict:
    with open(sfr_input_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    idx_counts, nstrm, nss, counts_raw = infer_counts_sfr(lines)
    start = find_segment_block_start(lines, nss, idx_counts, nstrm)

    rows = []
    expected = 1
    i = start
    while expected <= nss and i < len(lines):
        if _is_comment_line(lines[i]):
            i += 1
            continue
        tk = _tokens(lines[i])
        if _looks_like_segment_header_relaxed(tk, expected):
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
        raise ValueError(f"Parsed only segments 1..{expected-1} of NSS={nss} (segment {expected} not found).")

    rt = pd.DataFrame(rows)

    # Normalize routing IDs:
    rt["outseg_norm"] = rt["outseg"].where(rt["outseg"].between(1, nss), 0)
    rt["iupseg_norm"] = rt["iupseg"].where(rt["iupseg"].between(1, nss), 0)

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


def write_dot(rt: pd.DataFrame, nss: int, out_dot: str) -> None:
    """
    Write a Graphviz DOT file for the segment network.
    Nodes:
      - rounded style for head segments
      - double border for out-of-model segments
    Edges:
      - downstream routing: segment -> outseg_norm (label downstream)
      - diversions: iupseg_norm -> segment (label diversion)
    """
    edges_down = [(int(r.segment), int(r.outseg_norm)) for r in rt.itertuples() if int(r.outseg_norm) > 0]
    edges_div = [(int(r.iupseg_norm), int(r.segment)) for r in rt.itertuples() if int(r.iupseg_norm) > 0]

    with open(out_dot, "w", encoding="utf-8") as f:
        f.write("digraph SFR_Segments {\n")
        f.write('  rankdir="LR";\n')
        f.write('  node [shape=box];\n')

        for r in rt.itertuples():
            attrs = []
            if bool(r.is_head_segment):
                attrs.append('style="rounded"')
            if bool(r.is_out_of_model):
                attrs.append("peripheries=2")
            label = str(int(r.segment))
            if attrs:
                f.write(f'  "{label}" [{", ".join(attrs)}];\n')
            else:
                f.write(f'  "{label}";\n')

        for a, b in edges_down:
            f.write(f'  "{a}" -> "{b}" [label="downstream"];\n')
        for a, b in edges_div:
            f.write(f'  "{a}" -> "{b}" [label="diversion"];\n')

        f.write("}\n")


'''def main():
    ap = argparse.ArgumentParser(description="SFR segment routing QC: extract segment routing table from SFR input.")
    ap.add_argument("--sfr-input", required=True, help="Path to SFR input file.")
    ap.add_argument("--out-csv", required=True, help="Path to output CSV routing table.")
    ap.add_argument("--out-dot", default=None, help="Optional Graphviz DOT output path.")
    args = ap.parse_args()

    res = parse_routing_table(args.sfr_input)
    rt = res["routing_table"]

    rt.to_csv(args.out_csv, index=False)

    print(f"NSTRM: {res['nstrm']}")
    print(f"NSS: {res['nss']}")
    print(f"Counts line: {res['counts_line_no']}: {res['counts_line']}")
    print(f"Segment block start line: {res['segblock_start_line_no']}")
    print(f"Out-of-model segments: {int(rt['is_out_of_model'].sum())}")
    print(f"Diversion segments: {int(rt['is_diversion_segment'].sum())}")
    print(f"Head segments: {int(rt['is_head_segment'].sum())}")
    print(f"Wrote CSV: {args.out_csv}")

    if args.out_dot:
        write_dot(rt, res["nss"], args.out_dot)
        print(f"Wrote DOT: {args.out_dot}")'''


# =========================
# USER SETTINGS (EDIT ME)
# =========================
SFR_INPUT_PATH = r"Y:\mbaillie\SFRZB\CVHMSFR.txt"  # e.g. r"Y:\path\to\model.sfr"
OUT_CSV_PATH   = r"Y:\mbaillie\SFRZB\CVHM_RoutingQC.csv"  # e.g. r"Y:\path\to\routing.csv"
OUT_DOT_PATH   = r"Y:\mbaillie\SFRZB\CVHM_RoutingQC.dot"  # optional: r"Y:\path\to\routing.dot" (leave blank to skip)

# =========================
# RUN
# =========================


# =========================
# RUN
# =========================
if __name__ == "__main__":
    # If you pass CLI args, use them (keeps the script usable from a terminal).
    if len(sys.argv) > 1:
        main()
    else:
        # Spyder/runfile mode: set the USER SETTINGS above.
        if not SFR_INPUT_PATH or not OUT_CSV_PATH:
            raise SystemExit(
                "Set SFR_INPUT_PATH and OUT_CSV_PATH near the bottom of this script, "
                "or run from the command line with --sfr-input and --out-csv."
            )

        res = parse_routing_table(SFR_INPUT_PATH)
        rt = res["routing_table"]
        rt.to_csv(OUT_CSV_PATH, index=False)
        print(f"Wrote CSV: {OUT_CSV_PATH}")

        if OUT_DOT_PATH and OUT_DOT_PATH.strip():
            write_dot(rt, res["nss"], OUT_DOT_PATH)
            print(f"Wrote DOT: {OUT_DOT_PATH}")
