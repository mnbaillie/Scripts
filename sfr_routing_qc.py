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

Also optionally writes a Graphviz .dot network diagram (topology only; no physical geometry)
and renders it to PNG.

Works with many MF-2005 lineage variants (MF-NWT, MF-OWHM) that use numeric segment
header lines beginning with: NSEG ICALC OUTSEG IUPSEG ...
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Dict, List, Set, Tuple

import pandas as pd

INT_PAT = re.compile(r'^[+-]?\d+$')
FLOAT_PAT = re.compile(r'^[+-]?(\d+(\.\d*)?|\.\d+)([Ee][+-]?\d+)?$')


def _strip_inline_comments(line: str) -> str:
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
    if line and line[0] in ("C", "c", "*"):
        return True
    return False


def _tokens(line: str) -> List[str]:
    s = _strip_inline_comments(line)
    s = s.replace(",", " ")
    return s.strip().split()


def infer_counts_sfr(lines: List[str], max_lines: int = 50000) -> Tuple[int, int, int, str]:
    for i, line in enumerate(lines[:max_lines]):
        if "NSTRM" in line.upper() and not line.strip().startswith(("#", ";", "!", "C", "c", "*")):
            tk = _tokens(line)
            if len(tk) >= 2 and INT_PAT.match(tk[0]) and INT_PAT.match(tk[1]):
                return i, int(tk[0]), int(tk[1]), line.strip()

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
    search_start = max(0, idx_counts + 1 + abs(int(nstrm)))
    for i in range(search_start, len(lines)):
        if _is_comment_line(lines[i]):
            continue
        tk = _tokens(lines[i])
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


def _build_undirected_adjacency(rt: pd.DataFrame) -> Dict[int, Set[int]]:
    adj: Dict[int, Set[int]] = {int(seg): set() for seg in rt["segment"].tolist()}
    for r in rt.itertuples(index=False):
        a = int(r.segment)
        b = int(r.outseg_norm)
        c = int(r.iupseg_norm)
        if b > 0:
            adj[a].add(b)
            adj[b].add(a)
        if c > 0:
            adj[a].add(c)
            adj[c].add(a)
    return adj


def _connected_components(adj: Dict[int, Set[int]]) -> List[Set[int]]:
    seen: Set[int] = set()
    comps: List[Set[int]] = []
    for start in adj:
        if start in seen:
            continue
        stack = [start]
        comp: Set[int] = set()
        seen.add(start)
        while stack:
            cur = stack.pop()
            comp.add(cur)
            for nxt in adj[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        comps.append(comp)
    return comps


def identify_hanging_subnetworks(rt: pd.DataFrame) -> Set[int]:
    n_total = len(rt)
    if n_total <= 2:
        return set()
    adj = _build_undirected_adjacency(rt)
    comps = _connected_components(adj)
    if len(comps) <= 1:
        return set()

    largest = max(len(c) for c in comps)
    flagged: Set[int] = set()
    for comp in comps:
        size = len(comp)
        if size == largest:
            continue
        if size == 1 or size == 2 or size < 0.10 * n_total:
            flagged.update(comp)
    return flagged


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
            rows.append(
                dict(
                    segment=int(tk[0]),
                    icalc=int(tk[1]),
                    outseg=int(tk[2]),
                    iupseg=int(tk[3]),
                    header_line_no=i + 1,
                    header_raw=_strip_inline_comments(lines[i]).strip(),
                )
            )
            expected += 1
        i += 1

    if expected <= nss:
        raise ValueError(f"Parsed only segments 1..{expected-1} of NSS={nss} (segment {expected} not found).")

    rt = pd.DataFrame(rows)
    rt["outseg_norm"] = rt["outseg"].where(rt["outseg"].between(1, nss), 0)
    rt["iupseg_norm"] = rt["iupseg"].where(rt["iupseg"].between(1, nss), 0)

    rt["is_out_of_model"] = ~rt["outseg"].between(1, nss)
    rt["is_diversion_segment"] = rt["iupseg"].between(1, nss)

    inflow_targets = set(rt.loc[rt["outseg_norm"] > 0, "outseg_norm"].astype(int)).union(
        set(rt.loc[rt["iupseg_norm"] > 0, "segment"].astype(int))
    )
    rt["is_head_segment"] = ~rt["segment"].astype(int).isin(inflow_targets)

    hanging_segments = identify_hanging_subnetworks(rt)
    rt["is_hanging_subnetwork"] = rt["segment"].astype(int).isin(hanging_segments)
    rt["has_reverse_downstream_connection"] = (
        (rt["outseg_norm"] > 0) & (rt["segment"] > rt["outseg_norm"])
    )
    rt["has_reverse_diversion_connection"] = (
        (rt["iupseg_norm"] > 0) & (rt["iupseg_norm"] > rt["segment"])
    )

    return dict(
        nstrm=nstrm,
        nss=nss,
        counts_line_no=idx_counts + 1,
        counts_line=counts_raw,
        segblock_start_line_no=start + 1,
        routing_table=rt,
    )


def write_qc_log(rt: pd.DataFrame, out_log: str) -> None:
    os.makedirs(os.path.dirname(out_log) or ".", exist_ok=True)
    messages: List[str] = []

    for r in rt.itertuples(index=False):
        seg = int(r.segment)
        outseg = int(r.outseg_norm)
        iupseg = int(r.iupseg_norm)

        if bool(r.has_reverse_downstream_connection) and outseg > 0:
            messages.append(
                f"Segment {seg} flows into Segment {outseg}: higher-numbered segment flowing into a lower-numbered segment."
            )

        if bool(r.has_reverse_diversion_connection) and iupseg > 0:
            messages.append(
                f"Segment {seg} receives diversion from Segment {iupseg}: higher-numbered segment connected to a lower-numbered diversion segment."
            )

    hanging = sorted(rt.loc[rt["is_hanging_subnetwork"], "segment"].astype(int).tolist())
    if hanging:
        messages.append(
            "Potential hanging or disconnected subnetwork segments identified: "
            + ", ".join(str(s) for s in hanging)
            + "."
        )

    if not messages:
        messages = [
            "No potential QC issues identified, the user should ensure that all routing connections are as intended."
        ]

    with open(out_log, "w", encoding="utf-8") as f:
        for msg in messages:
            f.write(msg + "\n")


def write_dot(rt: pd.DataFrame, out_dot: str, network_direction: str = "LR") -> None:
    os.makedirs(os.path.dirname(out_dot) or ".", exist_ok=True)

    diversion_segments = set(rt.loc[rt["is_diversion_segment"], "segment"].astype(int))
    hanging_segments = set(rt.loc[rt["is_hanging_subnetwork"], "segment"].astype(int))

    with open(out_dot, "w", encoding="utf-8") as f:
        f.write("digraph SFR_Segments {\n")
        network_direction = str(network_direction).upper().strip()
        if network_direction not in {"LR", "RL", "TB", "BT"}:
            raise ValueError("NETWORK_DIRECTION must be one of: LR, RL, TB, BT")
        f.write(f'  rankdir="{network_direction}";\n')
        f.write('  node [shape=box, fontname="Helvetica"];\n')
        f.write('  edge [fontname="Helvetica"];\n')

        # Legend
        f.write("  subgraph cluster_legend {\n")
        f.write('    label="Legend";\n')
        f.write('    fontsize=12;\n')
        f.write('    color="gray60";\n')
        f.write('    style="rounded";\n')
        f.write('    legend_normal [label="Normal segment", shape=box];\n')
        f.write('    legend_div [label="Diversion segment", shape=diamond];\n')
        f.write('    legend_head [label="Head segment", shape=box, style="rounded"];\n')
        f.write('    legend_out [label="Out of model", shape=box, peripheries=2];\n')
        f.write('    legend_qc [label="Potential QC issue", shape=box, style="filled", fillcolor="#f4cccc", color="#cc0000"];\n')
        f.write('    legend_a [label="", shape=point, width=0.01];\n')
        f.write('    legend_b [label="", shape=point, width=0.01];\n')
        f.write('    legend_c [label="", shape=point, width=0.01];\n')
        f.write('    legend_a -> legend_b [label="Downstream connection"];\n')
        f.write('    legend_b -> legend_c [label="Diversion or diversion-adjacent connection", style=dashed];\n')
        f.write("  }\n")

        for r in rt.itertuples(index=False):
            seg = int(r.segment)
            attrs = []
            attrs.append('shape=diamond' if bool(r.is_diversion_segment) else 'shape=box')

            styles = []
            if bool(r.is_head_segment):
                styles.append("rounded")
            if bool(r.is_hanging_subnetwork) or bool(r.has_reverse_downstream_connection) or bool(r.has_reverse_diversion_connection):
                styles.append("filled")
                attrs.append('fillcolor="#f4cccc"')
                attrs.append('color="#cc0000"')
            if styles:
                attrs.append(f'style="{",".join(styles)}"')
            if bool(r.is_out_of_model):
                attrs.append("peripheries=2")

            f.write(f'  "{seg}" [{", ".join(attrs)}];\n')

        # downstream edges: no labels
        for r in rt.itertuples(index=False):
            a = int(r.segment)
            b = int(r.outseg_norm)
            if b <= 0:
                continue
            attrs = []
            if a in diversion_segments or b in diversion_segments:
                attrs.append("style=dashed")
            if a > b or (a in hanging_segments and b in hanging_segments):
                attrs.append('color="#cc0000"')
                attrs.append("penwidth=2")
            if attrs:
                f.write(f'  "{a}" -> "{b}" [{", ".join(attrs)}];\n')
            else:
                f.write(f'  "{a}" -> "{b}";\n')

        # diversion edges
        for r in rt.itertuples(index=False):
            a = int(r.iupseg_norm)
            b = int(r.segment)
            if a <= 0:
                continue
            attrs = ['label="diversion"', "style=dashed"]
            if a > b or (a in hanging_segments and b in hanging_segments):
                attrs.append('color="#cc0000"')
                attrs.append("penwidth=2")
            f.write(f'  "{a}" -> "{b}" [{", ".join(attrs)}];\n')

        f.write("}\n")


def render_png_from_dot(out_dot: str, out_png: str) -> None:
    try:
        import graphviz  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Python package 'graphviz' is not installed. Install it with 'pip install graphviz' or 'conda install python-graphviz'."
        ) from exc

    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)

    try:
        src = graphviz.Source.from_file(out_dot)
        src.render(outfile=out_png, format="png", cleanup=False)
    except Exception as exc:
        raise RuntimeError(
            "Failed to render PNG with Graphviz. Make sure the Graphviz executables (especially 'dot') are installed and available on PATH."
        ) from exc


def main() -> None:
    ap = argparse.ArgumentParser(description="SFR segment routing QC: extract segment routing table from SFR input.")
    ap.add_argument("--sfr-input", required=True, help="Path to SFR input file.")
    ap.add_argument("--out-csv", required=True, help="Path to output CSV routing table.")
    ap.add_argument("--out-dot", default=None, help="Optional Graphviz DOT output path.")
    ap.add_argument("--out-png", default=None, help="Optional rendered PNG output path.")
    ap.add_argument("--out-log", default=None, help="Optional text QC log output path.")
    args = ap.parse_args()

    res = parse_routing_table(args.sfr_input)
    rt = res["routing_table"]

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    rt.to_csv(args.out_csv, index=False)
    print(f"Wrote CSV: {args.out_csv}")

    if args.out_dot:
        write_dot(rt, args.out_dot, NETWORK_DIRECTION)
        print(f"Wrote DOT: {args.out_dot}")

    if args.out_png:
        dot_for_render = args.out_dot
        if not dot_for_render:
            root, _ = os.path.splitext(args.out_png)
            dot_for_render = root + ".dot"
            write_dot(rt, dot_for_render, NETWORK_DIRECTION)
            print(f"Wrote DOT: {dot_for_render}")
        render_png_from_dot(dot_for_render, args.out_png)
        print(f"Wrote PNG: {args.out_png}")

    if args.out_log:
        write_qc_log(rt, args.out_log)
        print(f"Wrote QC log: {args.out_log}")


# =========================
# USER SETTINGS (EDIT ME)
# =========================
SFR_INPUT_PATH = r"Y:\mbaillie\SFRZB\yucaipa.sfr"
# Provide desired path for QC routing CSV file, leave blank to not print
OUT_CSV_PATH = r"Y:\mbaillie\SFRZB\yucaipa_RoutingQCv2.csv"
# Provide desired path for QC routing DOT file, leave blank to not print
OUT_DOT_PATH = r"Y:\mbaillie\SFRZB\yucaipa_RoutingQCv2.dot"
# NOTE that you must have the Graphviz system executable installed and available on your PATH to render the PNG.
# Download at https://www.graphviz.org/download/
# Otherwise, copy the contents of the .dot file into the input pane of https://dreampuf.github.io/GraphvizOnline/?engine=dot
OUT_PNG_PATH = r""
# Provide desired path for QC log text file, leave blank to not print
OUT_LOG_PATH = r"Y:\mbaillie\SFRZB\yucaipa_RoutingQCLogv2.txt"
# Network diagram direction:
#   "LR" = left to right
#   "RL" = right to left
#   "TB" = top to bottom
#   "BT" = bottom to top
NETWORK_DIRECTION = "TB"


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        if not SFR_INPUT_PATH or not OUT_CSV_PATH:
            raise SystemExit(
                "Set SFR_INPUT_PATH and OUT_CSV_PATH near the bottom of this script, or run from the command line with --sfr-input and --out-csv."
            )

        res = parse_routing_table(SFR_INPUT_PATH)
        rt = res["routing_table"]

        os.makedirs(os.path.dirname(OUT_CSV_PATH) or ".", exist_ok=True)
        rt.to_csv(OUT_CSV_PATH, index=False)
        print(f"Wrote CSV: {OUT_CSV_PATH}")

        dot_written = False
        dot_path = OUT_DOT_PATH.strip() if OUT_DOT_PATH else ""

        if dot_path:
            write_dot(rt, dot_path, NETWORK_DIRECTION)
            dot_written = True
            print(f"Wrote DOT: {dot_path}")

        if OUT_PNG_PATH and OUT_PNG_PATH.strip():
            png_path = OUT_PNG_PATH.strip()
            if not dot_written:
                root, _ = os.path.splitext(png_path)
                dot_path = root + ".dot"
                write_dot(rt, dot_path, NETWORK_DIRECTION)
                print(f"Wrote DOT: {dot_path}")
            render_png_from_dot(dot_path, png_path)
            print(f"Wrote PNG: {png_path}")

        if OUT_LOG_PATH and OUT_LOG_PATH.strip():
            log_path = OUT_LOG_PATH.strip()
            write_qc_log(rt, log_path)
            print(f"Wrote QC log: {log_path}")
