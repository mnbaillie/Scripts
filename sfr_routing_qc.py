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
    """Tokenize a line, handling both whitespace- and comma-separated SFR files."""
    s = _strip_inline_comments(line)
    s = s.replace(",", " ")
    return s.strip().split()


def infer_counts_sfr(lines: List[str], max_lines: int = 50000) -> Tuple[int, int, int, str]:
    """Infer NSTRM and NSS from the SFR input."""
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


def _build_undirected_adjacency(rt: pd.DataFrame, lak_info: dict | None = None) -> Dict[str, Set[str]]:
    """Build an undirected topology graph for hanging-subnetwork QC.

    Include virtual LAKE_n nodes so streams that drain to or receive flow from
    lakes are not falsely identified as disconnected SFR-only subnetworks. When
    LAK sublake-system information is available, connect lakes in the same
    sublake system as well.
    """
    adj: Dict[str, Set[str]] = {f"SFR_{int(seg)}": set() for seg in rt["segment"].tolist()}

    def add_edge(a: str, b: str) -> None:
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    for r in rt.itertuples(index=False):
        a = f"SFR_{int(r.segment)}"
        outseg = int(r.outseg)
        iupseg = int(r.iupseg)
        if outseg > 0:
            add_edge(a, f"SFR_{outseg}")
        elif outseg < 0:
            add_edge(a, f"LAKE_{abs(outseg)}")
        if iupseg > 0:
            add_edge(a, f"SFR_{iupseg}")
        elif iupseg < 0:
            add_edge(a, f"LAKE_{abs(iupseg)}")

    if lak_info:
        for lake_id in range(1, int(lak_info.get("nlakes", 0) or 0) + 1):
            adj.setdefault(f"LAKE_{lake_id}", set())
        for system in lak_info.get("sublake_systems", []):
            lakes = system.get("lake_ids", system.get("sublakes", []))
            lakes = [int(x) for x in lakes]
            if len(lakes) < 2:
                continue
            anchor = f"LAKE_{lakes[0]}"
            for lake in lakes[1:]:
                add_edge(anchor, f"LAKE_{lake}")

    return adj


def _connected_components(adj: Dict[str, Set[str]]) -> List[Set[str]]:
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


def identify_hanging_subnetworks(rt: pd.DataFrame, lak_info: dict | None = None) -> Set[int]:
    n_total = len(rt)
    if n_total <= 2:
        return set()
    adj = _build_undirected_adjacency(rt, lak_info=lak_info)
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
            for node in comp:
                if str(node).startswith("SFR_"):
                    tail = str(node).replace("SFR_", "", 1)
                    if tail.isdigit():
                        flagged.add(int(tail))
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
    rt["outseg_norm"] = rt["outseg"].where(rt["outseg"].between(1, nss), 0)
    rt["iupseg_norm"] = rt["iupseg"].where(rt["iupseg"].between(1, nss), 0)

    rt["flows_to_lake"] = rt["outseg"] < 0
    rt["lake_out_id"] = rt["outseg"].where(rt["outseg"] < 0, 0).abs().astype(int)
    rt["flows_from_lake"] = rt["iupseg"] < 0
    rt["lake_in_id"] = rt["iupseg"].where(rt["iupseg"] < 0, 0).abs().astype(int)

    rt["is_out_of_model"] = (~rt["outseg"].between(1, nss)) & (~rt["flows_to_lake"])
    rt["is_diversion_segment"] = rt["iupseg"].between(1, nss)

    inflow_targets = set(rt.loc[rt["outseg_norm"] > 0, "outseg_norm"].astype(int)).union(
        set(rt.loc[rt["iupseg_norm"] > 0, "segment"].astype(int))
    )
    rt["is_head_segment"] = ~rt["segment"].astype(int).isin(inflow_targets) & (~rt["flows_from_lake"])

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
        lake_ids=sorted(set(rt.loc[rt["lake_out_id"] > 0, "lake_out_id"].tolist()) | set(rt.loc[rt["lake_in_id"] > 0, "lake_in_id"].tolist())),
    )


def update_hanging_subnetwork_flags(rt: pd.DataFrame, lak_info: dict | None = None) -> pd.DataFrame:
    """Recompute hanging-subnetwork flags with optional LAK connectivity."""
    rt = rt.copy()
    hanging_segments = identify_hanging_subnetworks(rt, lak_info=lak_info)
    rt["is_hanging_subnetwork"] = rt["segment"].astype(int).isin(hanging_segments)
    return rt


def _numeric_lak_records(lines: List[str]) -> List[Tuple[int, List[str], str]]:
    """Return non-comment LAK records as (1-based line number, tokens, raw text)."""
    records: List[Tuple[int, List[str], str]] = []
    for i, line in enumerate(lines):
        if _is_comment_line(line):
            continue
        tk = _tokens(line)
        if tk:
            records.append((i + 1, tk, _strip_inline_comments(line).strip()))
    return records


def _first_int_token(tk: List[str]) -> int | None:
    for token in tk:
        if INT_PAT.match(token):
            return int(token)
    return None


def _parse_sublake_records(
    records: List[Tuple[int, List[str], str]],
    start_record_index: int,
    nslms: int,
    nlakes: int,
) -> Tuple[List[dict], int]:
    """Parse NSLMS Data Set 8a records beginning after start_record_index."""
    systems: List[dict] = []
    idx = start_record_index + 1

    for system_id in range(1, nslms + 1):
        while idx < len(records) and not records[idx][1]:
            idx += 1
        if idx >= len(records):
            raise ValueError(
                f"LAK file ended before sublake system {system_id} of {nslms} could be read."
            )

        line_no, tk, raw = records[idx]
        if not tk or not INT_PAT.match(tk[0]):
            raise ValueError(
                f"Expected IC for LAK sublake system {system_id} on line {line_no}, but found: {raw}"
            )

        ic = int(tk[0])
        if ic <= 0:
            raise ValueError(
                f"Expected positive IC for LAK sublake system {system_id} on line {line_no}, but found {ic}."
            )

        lake_ids: List[int] = []
        idx2 = idx
        token_pos = 1
        while len(lake_ids) < ic:
            if idx2 >= len(records):
                raise ValueError(
                    f"LAK file ended while reading ISUB values for sublake system {system_id}."
                )
            line_no2, tk2, raw2 = records[idx2]
            while token_pos < len(tk2) and len(lake_ids) < ic:
                if INT_PAT.match(tk2[token_pos]):
                    lake_id = int(tk2[token_pos])
                    if lake_id < 1 or lake_id > nlakes:
                        raise ValueError(
                            f"Lake ID {lake_id} in sublake system {system_id} is outside the expected range 1..{nlakes}."
                        )
                    lake_ids.append(lake_id)
                token_pos += 1
            if len(lake_ids) < ic:
                idx2 += 1
                token_pos = 0

        systems.append(
            dict(
                sublake_system=system_id,
                ic=ic,
                lake_ids=lake_ids,
                line_no=line_no,
                raw=raw,
            )
        )
        idx = idx2 + 1

    return systems, idx


def parse_lak_sublake_systems(lak_input_path: str) -> dict:
    """
    Parse key LAK package items used for routing visualization.

    Extracts:
      - NLAKES from Data Set 1
      - NSLMS from Data Set 7
      - IC and ISUB(1)..ISUB(IC) from each Data Set 8a record

    The parser is intentionally focused on these routing/QC items. It ignores the
    lakebed arrays and time-varying stress-period data that are not needed for the
    routing diagram. It is tolerant of comments and comma-separated values, but it
    expects the Data Set 7 / 8a block to be present in standard LAK input order.
    """
    with open(lak_input_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    records = _numeric_lak_records(lines)
    if not records:
        raise ValueError("LAK input file does not contain any readable numeric records.")

    nlakes_record_index = None
    nlakes = None
    for idx, (_line_no, tk, _raw) in enumerate(records):
        first_int = _first_int_token(tk)
        if first_int is not None and first_int > 0:
            nlakes_record_index = idx
            nlakes = first_int
            break

    if nlakes_record_index is None or nlakes is None:
        raise ValueError("Could not identify NLAKES from LAK Data Set 1.")

    explicit_nslms_candidates: List[int] = []
    for idx, (line_no, tk, raw) in enumerate(records):
        if idx <= nlakes_record_index:
            continue
        if "NSLMS" in raw.upper() or "NSLMS" in lines[line_no - 1].upper():
            if tk and INT_PAT.match(tk[0]):
                explicit_nslms_candidates.append(idx)

    valid_candidates: List[Tuple[int, int, List[dict]]] = []
    candidate_indices = explicit_nslms_candidates or list(range(nlakes_record_index + 1, len(records)))

    for idx in candidate_indices:
        line_no, tk, raw = records[idx]
        if not tk or not INT_PAT.match(tk[0]):
            continue
        nslms = int(tk[0])
        if nslms < 0 or nslms > nlakes:
            continue
        try:
            systems, _ = _parse_sublake_records(records, idx, nslms, nlakes)
        except ValueError:
            continue
        valid_candidates.append((idx, nslms, systems))

    if not valid_candidates:
        raise ValueError(
            "Could not identify a valid NSLMS / sublake-system block in the LAK input file. "
            "Check that Data Set 7 and Data Set 8a are present in the expected format."
        )

    # Prefer explicit NSLMS labels when present. Otherwise use the latest valid
    # candidate because Data Set 7 occurs after the earlier lake-definition arrays.
    chosen_idx, nslms, systems = valid_candidates[-1]
    nslms_line_no, _nslms_tk, nslms_raw = records[chosen_idx]
    nlakes_line_no, _nlakes_tk, nlakes_raw = records[nlakes_record_index]

    lake_membership: Dict[int, List[int]] = {lake_id: [] for lake_id in range(1, nlakes + 1)}
    for system in systems:
        for lake_id in system["lake_ids"]:
            lake_membership.setdefault(int(lake_id), []).append(int(system["sublake_system"]))

    repeated_lakes = {
        lake_id: system_ids
        for lake_id, system_ids in lake_membership.items()
        if len(system_ids) > 1
    }

    return dict(
        nlakes=nlakes,
        nlakes_line_no=nlakes_line_no,
        nlakes_line=nlakes_raw,
        nslms=nslms,
        nslms_line_no=nslms_line_no,
        nslms_line=nslms_raw,
        sublake_systems=systems,
        lake_membership=lake_membership,
        repeated_lakes=repeated_lakes,
    )



def _normalize_zone_header(name: str) -> str:
    """Normalize common zone-file column names."""
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _read_zone_input_table(zone_input_path: str) -> pd.DataFrame:
    """
    Read a simple ZoneBudget-style zone input table.

    Supported forms include:
      - headered CSV/whitespace table with columns such as SEGMENT, ZONE
        (negative SEGMENT values are interpreted as lakes; e.g., -1 = Lake 1)
      - headered table with TYPE, ID, ZONE where TYPE is SEGMENT/SEG/LAKE/LAK
      - headerless two-column table interpreted as SEGMENT, ZONE
        (negative SEGMENT values are interpreted as lakes; e.g., -1 = Lake 1)
      - headerless three-column table interpreted as TYPE, ID, ZONE

    Lines beginning with #, ;, !, C/c, or * are ignored. Inline comments are
    stripped using the same convention as the SFR/LAK parsers.
    """
    rows: List[List[str]] = []
    with open(zone_input_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if _is_comment_line(line):
                continue
            tk = _tokens(line)
            if tk:
                rows.append(tk)

    if not rows:
        raise ValueError("Zone input file does not contain any readable records.")

    first = rows[0]
    has_header = any(not (INT_PAT.match(x) or FLOAT_PAT.match(x)) for x in first)
    if has_header:
        max_cols = max(len(r) for r in rows)
        header = [_normalize_zone_header(x) for x in first]
        data = [r + [""] * (max_cols - len(r)) for r in rows[1:]]
        header = header + [f"extra{i}" for i in range(len(header), max_cols)]
        df = pd.DataFrame(data, columns=header[:max_cols])
    else:
        max_cols = max(len(r) for r in rows)
        data = [r + [""] * (max_cols - len(r)) for r in rows]
        if max_cols == 2:
            df = pd.DataFrame(data, columns=["segment", "zone"])
        elif max_cols >= 3:
            df = pd.DataFrame(data, columns=["type", "id", "zone"] + [f"extra{i}" for i in range(3, max_cols)])
        else:
            raise ValueError("Zone input file must have at least two columns.")

    return df


def _as_int(value: object) -> int | None:
    s = str(value).strip()
    if INT_PAT.match(s):
        return int(s)
    if FLOAT_PAT.match(s):
        v = float(s)
        if v.is_integer():
            return int(v)
    return None


def parse_zone_input(zone_input_path: str) -> dict:
    """
    Parse an optional ZoneBudget zone input file for routing visualization.

    Returns node-to-zone assignments using graph node IDs:
      - SFR segments: "1", "2", ...
      - lakes: "LAKE_1", "LAKE_2", ...

    Zone 0 records are retained in the assignment table but are intentionally
    omitted from the Graphviz zone subgraphs.
    """
    df = _read_zone_input_table(zone_input_path)
    cols = set(df.columns)

    zone_col = next((c for c in df.columns if c in {"zone", "zoneid", "zonebudgetzone", "zbzone"}), None)
    if zone_col is None:
        raise ValueError("Could not identify a ZONE column in the zone input file.")

    type_col = next((c for c in df.columns if c in {"type", "nodetype", "featuretype", "kind"}), None)
    id_col = next((c for c in df.columns if c in {"id", "node", "nodeid", "featureid"}), None)
    segment_col = next((c for c in df.columns if c in {"segment", "seg", "nseg", "iseg"}), None)
    lake_col = next((c for c in df.columns if c in {"lake", "lak", "lakeid", "ilake"}), None)

    assignments: Dict[str, int] = {}
    records: List[dict] = []

    for row_index, row in df.iterrows():
        zone = _as_int(row.get(zone_col, ""))
        if zone is None:
            continue

        node_id = None
        feature_type = None
        feature_id = None

        if type_col and id_col:
            raw_type = str(row.get(type_col, "")).strip().lower()
            feature_id = _as_int(row.get(id_col, ""))
            if feature_id is None:
                continue
            if raw_type in {"lake", "lak", "l", "la"}:
                feature_type = "lake"
                node_id = f"LAKE_{feature_id}"
            elif raw_type in {"segment", "seg", "sfr", "stream", "streamsegment"}:
                feature_type = "segment"
                node_id = str(feature_id)
            else:
                # Unknown type; skip instead of guessing incorrectly.
                continue
        elif segment_col:
            feature_id = _as_int(row.get(segment_col, ""))
            if feature_id is None:
                continue
            # ZoneBudget zone files commonly use negative identifiers for lakes,
            # following the same convention as negative SFR OUTSEG/IUPSEG values.
            # For example, SEGMENT=-1 means Lake 1.
            if feature_id < 0:
                feature_type = "lake"
                feature_id = abs(feature_id)
                node_id = f"LAKE_{feature_id}"
            else:
                feature_type = "segment"
                node_id = str(feature_id)
        elif lake_col:
            feature_id = _as_int(row.get(lake_col, ""))
            if feature_id is None:
                continue
            feature_type = "lake"
            node_id = f"LAKE_{feature_id}"
        else:
            raise ValueError(
                "Could not identify segment/lake identifiers in the zone input file. "
                "Use columns such as SEGMENT,ZONE or TYPE,ID,ZONE."
            )

        assignments[node_id] = int(zone)
        records.append(
            dict(
                node_id=node_id,
                feature_type=feature_type,
                feature_id=feature_id,
                zone=int(zone),
                source_row=int(row_index) + 1,
            )
        )

    zone_to_nodes: Dict[int, List[str]] = {}
    for node_id, zone in assignments.items():
        zone_to_nodes.setdefault(int(zone), []).append(node_id)
    for zone in zone_to_nodes:
        zone_to_nodes[zone] = sorted(zone_to_nodes[zone], key=lambda x: (x.startswith("LAKE_"), int(x.replace("LAKE_", "")) if x.replace("LAKE_", "").isdigit() else x))

    return dict(
        assignments=assignments,
        zone_to_nodes=zone_to_nodes,
        records=records,
        dataframe=pd.DataFrame(records),
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


def write_dot(
    rt: pd.DataFrame,
    out_dot: str,
    network_direction: str = "LR",
    show_legend: bool = True,
    show_qc_issues: bool = True,
    lak_info: dict | None = None,
    zone_info: dict | None = None,
) -> None:
    os.makedirs(os.path.dirname(out_dot) or ".", exist_ok=True)

    diversion_segments = set(rt.loc[rt["is_diversion_segment"], "segment"].astype(int))
    hanging_segments = set(rt.loc[rt["is_hanging_subnetwork"], "segment"].astype(int))
    lake_ids_from_sfr = set(rt.loc[rt["lake_out_id"] > 0, "lake_out_id"].astype(int).tolist()) | set(rt.loc[rt["lake_in_id"] > 0, "lake_in_id"].astype(int).tolist())
    lake_ids_from_lak = set(range(1, int(lak_info["nlakes"]) + 1)) if lak_info else set()
    lake_ids_from_zones = set()
    if zone_info:
        for node_id in zone_info.get("assignments", {}):
            if str(node_id).startswith("LAKE_"):
                lake_id = _as_int(str(node_id).replace("LAKE_", ""))
                if lake_id is not None and lake_id > 0:
                    lake_ids_from_zones.add(lake_id)
    lake_ids = sorted(lake_ids_from_sfr | lake_ids_from_lak | lake_ids_from_zones)

    with open(out_dot, "w", encoding="utf-8") as f:
        f.write("digraph SFR_Segments {\n")
        network_direction = str(network_direction).upper().strip()
        if network_direction not in {"LR", "RL", "TB", "BT"}:
            raise ValueError("NETWORK_DIRECTION must be one of: LR, RL, TB, BT")
        f.write(f'  rankdir="{network_direction}";\n')
        f.write('  node [shape=box, fontname="Helvetica"];\n')
        f.write('  edge [fontname="Helvetica"];\n')

        # Legend
        if show_legend:
            f.write("  subgraph cluster_legend {\n")
            f.write('    label="Legend";\n')
            f.write('    fontsize=12;\n')
            f.write('    color="gray60";\n')
            f.write('    style="rounded";\n')
            f.write('    legend_normal [label="Normal segment", shape=box];\n')
            f.write('    legend_div [label="Diversion segment", shape=diamond];\n')
            f.write('    legend_lake [label="Lake", shape=doubleoctagon];\n')
            f.write('    legend_head [label="Head segment", shape=box, style="rounded"];\n')
            f.write('    legend_out [label="Out of model", shape=box, peripheries=2];\n')
            if show_qc_issues:
                f.write('    legend_qc [label="Potential QC issue", shape=box, style="filled", fillcolor="#f4cccc", color="#cc0000"];\n')
            f.write('    legend_a [label="", shape=point, width=0.01];\n')
            f.write('    legend_b [label="", shape=point, width=0.01];\n')
            f.write('    legend_c [label="", shape=point, width=0.01];\n')
            f.write('    legend_d [label="", shape=point, width=0.01];\n')
            f.write('    legend_a -> legend_b [label="Downstream connection"];\n')
            f.write('    legend_b -> legend_c [label="Diversion or diversion-adjacent connection", style=dashed];\n')
            f.write('    legend_c -> legend_d [label="Lake connection", style=bold];\n')
            if lak_info:
                f.write('    legend_sublake_a [label="", shape=point, width=0.01];\n')
                f.write('    legend_sublake_b [label="", shape=point, width=0.01];\n')
                f.write('    legend_sublake_a -> legend_sublake_b [label="Sublake-system connection", style=dashed, dir=none];\n')
            f.write("  }\n")

        for lake_id in lake_ids:
            label = f"Lake {lake_id}"
            if lak_info:
                memberships = lak_info.get("lake_membership", {}).get(int(lake_id), [])
                if memberships:
                    label += "\\nSublake systems: " + ", ".join(str(x) for x in memberships)
            f.write(f'  "LAKE_{lake_id}" [label="{label}", shape=doubleoctagon];\n')

        if lak_info:
            written_sublake_edges: Set[Tuple[int, int, int]] = set()
            for system in lak_info.get("sublake_systems", []):
                system_id = int(system["sublake_system"])
                ids = [int(x) for x in system.get("lake_ids", [])]
                for pos, a in enumerate(ids):
                    for b in ids[pos + 1:]:
                        edge = (system_id, min(a, b), max(a, b))
                        if edge in written_sublake_edges:
                            continue
                        written_sublake_edges.add(edge)
                        f.write(
                            f'  "LAKE_{a}" -> "LAKE_{b}" [label="sublake system {system_id}", style=dashed, dir=none];\n'
                        )

        for r in rt.itertuples(index=False):
            seg = int(r.segment)
            attrs = []
            attrs.append('shape=diamond' if bool(r.is_diversion_segment) else 'shape=box')

            styles = []
            if bool(r.is_head_segment):
                styles.append("rounded")
            if show_qc_issues and (bool(r.is_hanging_subnetwork) or bool(r.has_reverse_downstream_connection) or bool(r.has_reverse_diversion_connection)):
                styles.append("filled")
                attrs.append('fillcolor="#f4cccc"')
                attrs.append('color="#cc0000"')
            if styles:
                attrs.append(f'style="{",".join(styles)}"')
            if bool(r.is_out_of_model):
                attrs.append("peripheries=2")
            f.write(f'  "{seg}" [{", ".join(attrs)}];\n')

        if zone_info:
            valid_nodes = set(str(x) for x in rt["segment"].astype(int).tolist()) | {f"LAKE_{lake_id}" for lake_id in lake_ids}
            for zone in sorted(z for z in zone_info.get("zone_to_nodes", {}) if int(z) != 0):
                nodes = [str(node) for node in zone_info["zone_to_nodes"][zone] if str(node) in valid_nodes]
                if not nodes:
                    continue
                f.write(f'  subgraph "cluster_zone_{int(zone)}" {{\n')
                f.write(f'    label="Zone {int(zone)}";\n')
                f.write('    color="black";\n')
                f.write('    style="rounded";\n')
                for node in nodes:
                    f.write(f'    "{node}";\n')
                f.write('  }\n')

        for r in rt.itertuples(index=False):
            a = int(r.segment)
            b = int(r.outseg_norm)
            if b <= 0:
                continue
            attrs = []
            if a in diversion_segments or b in diversion_segments:
                attrs.append("style=dashed")
            if show_qc_issues and (a > b or (a in hanging_segments and b in hanging_segments)):
                attrs.append('color="#cc0000"')
                attrs.append("penwidth=2")
            if attrs:
                f.write(f'  "{a}" -> "{b}" [{", ".join(attrs)}];\n')
            else:
                f.write(f'  "{a}" -> "{b}";\n')

        for r in rt.itertuples(index=False):
            a = int(r.iupseg_norm)
            b = int(r.segment)
            if a <= 0:
                continue
            attrs = ['label="diversion"', "style=dashed"]
            if show_qc_issues and (a > b or (a in hanging_segments and b in hanging_segments)):
                attrs.append('color="#cc0000"')
                attrs.append("penwidth=2")
            f.write(f'  "{a}" -> "{b}" [{", ".join(attrs)}];\n')

        for r in rt.itertuples(index=False):
            seg = int(r.segment)
            lake_out_id = int(getattr(r, "lake_out_id", 0))
            lake_in_id = int(getattr(r, "lake_in_id", 0))
            if lake_out_id > 0:
                f.write(f'  "{seg}" -> "LAKE_{lake_out_id}";\n')
            if lake_in_id > 0:
                f.write(f'  "LAKE_{lake_in_id}" -> "{seg}";\n')

        f.write("}\n")


def render_png_from_dot(out_dot: str, out_png: str) -> None:
    try:
        import graphviz  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Python package 'graphviz' is not installed. "
            "Install it with 'pip install graphviz' or 'conda install python-graphviz'."
        ) from exc

    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)

    try:
        src = graphviz.Source.from_file(out_dot)
        src.render(outfile=out_png, format="png", cleanup=False)
    except Exception as exc:
        raise RuntimeError(
            "Failed to render PNG with Graphviz. Make sure the Graphviz executables "
            "(especially 'dot') are installed and available on PATH."
        ) from exc


def main() -> None:
    ap = argparse.ArgumentParser(description="SFR segment routing QC: extract segment routing table from SFR input.")
    ap.add_argument("--sfr-input", required=True, help="Path to SFR input file.")
    ap.add_argument("--out-csv", required=True, help="Path to output CSV routing table.")
    ap.add_argument("--out-dot", default=None, help="Optional Graphviz DOT output path.")
    ap.add_argument("--out-png", default=None, help="Optional rendered PNG output path.")
    ap.add_argument("--out-log", default=None, help="Optional text QC log output path.")
    ap.add_argument("--lak-input", default=None, help="Optional LAK input file used to add all lakes and sublake-system connections to the DOT/PNG output.")
    ap.add_argument("--zone-input", default=None, help="Optional ZoneBudget zone input file used to draw Zone 1, Zone 2, etc. subgraph boxes around assigned segment/lake nodes.")
    ap.add_argument("--network-direction", default="LR", choices=["LR", "RL", "TB", "BT", "lr", "rl", "tb", "bt"], help="Graphviz network direction for DOT/PNG output.")
    ap.add_argument("--show-legend", action=argparse.BooleanOptionalAction, default=True, help="Show or hide the legend in the DOT/PNG output.")
    ap.add_argument("--show-qc-issues", action=argparse.BooleanOptionalAction, default=True, help="Highlight or suppress QC issues in the DOT/PNG output.")
    args = ap.parse_args()

    res = parse_routing_table(args.sfr_input)
    rt = res["routing_table"]
    lak_info = parse_lak_sublake_systems(args.lak_input) if args.lak_input else None
    rt = update_hanging_subnetwork_flags(rt, lak_info=lak_info)
    zone_info = parse_zone_input(args.zone_input) if args.zone_input else None

    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    rt.to_csv(args.out_csv, index=False)

    print(f"NSTRM: {res['nstrm']}")
    print(f"NSS: {res['nss']}")
    print(f"Counts line: {res['counts_line_no']}: {res['counts_line']}")
    print(f"Segment block start line: {res['segblock_start_line_no']}")
    print(f"Out-of-model segments: {int(rt['is_out_of_model'].sum())}")
    print(f"Diversion segments: {int(rt['is_diversion_segment'].sum())}")
    print(f"Head segments: {int(rt['is_head_segment'].sum())}")
    if lak_info:
        print(f"LAK NLAKES: {lak_info['nlakes']}")
        print(f"LAK NSLMS: {lak_info['nslms']}")
    if zone_info:
        visible_zones = sorted(z for z in zone_info.get("zone_to_nodes", {}) if int(z) != 0)
        print(f"Zone file zones: {', '.join(str(z) for z in visible_zones) if visible_zones else 'none above 0'}")
    print(f"Wrote CSV: {args.out_csv}")

    if args.out_dot:
        write_dot(rt, args.out_dot, args.network_direction, args.show_legend, args.show_qc_issues, lak_info, zone_info)
        print(f"Wrote DOT: {args.out_dot}")

    if args.out_png:
        dot_for_render = args.out_dot
        if not dot_for_render:
            root, _ = os.path.splitext(args.out_png)
            dot_for_render = root + ".dot"
            write_dot(rt, dot_for_render, args.network_direction, args.show_legend, args.show_qc_issues, lak_info, zone_info)
            print(f"Wrote DOT: {dot_for_render}")
        render_png_from_dot(dot_for_render, args.out_png)
        print(f"Wrote PNG: {args.out_png}")

    if args.out_log:
        write_qc_log(rt, args.out_log)
        print(f"Wrote QC log: {args.out_log}")


# =========================
# USER SETTINGS (EDIT ME)
# =========================
SFR_INPUT_PATH = r"Y:\mbaillie\SFRZB\Test Models\test5\test5.sfr"  # e.g. r"Y:\path\to\model.sfr"
# Optional LAK package input file. Provide this to add all lakes and sublake-system connections to the visualization.
# Leave blank to use only lake connections that can be inferred from negative SFR OUTSEG/IUPSEG values.
LAK_INPUT_PATH = r"Y:\mbaillie\SFRZB\Test Models\test5\test5.lak"  # optional: r"Y:\path\to\model.lak"
# Optional ZoneBudget zone input file. Provide this to draw Zone 1, Zone 2, etc. boxes around assigned nodes.
# Leave blank to omit zone grouping from the visualization. Zone 0 assignments are not boxed.
ZONE_INPUT_PATH = r"Y:\mbaillie\SFRZB\Test Models\test5\test5_zone_1segperzone.csv"  # optional: r"Y:\path\to\zones.txt"
# Provide desired path for QC routing CSV file, leave blank to not print
OUT_CSV_PATH   = r"Y:\mbaillie\SFRZB\Test Models\test5\test5_RoutingQC.csv"  # e.g. r"Y:\path\to\routing.csv"
# Provide desired path for QC routing DOT file, leave blank to not print
OUT_DOT_PATH   = r"Y:\mbaillie\SFRZB\Test Models\test5\test5_RoutingQC.dot"  # optional: r"Y:\path\to\routing.dot" (leave blank to skip)
# NOTE that you must have the Graphviz system executable installed and available on your PATH to render the PNG.
# Download at https://www.graphviz.org/download/
# Otherwise, copy the contents of the .dot file into the input pane of https://dreampuf.github.io/GraphvizOnline/?engine=dot
OUT_PNG_PATH   = r""  # optional: r"Y:\path\to\routing.png" (leave blank to skip)
# Provide desired path for QC log text file, leave blank to not print
OUT_LOG_PATH = r"Y:\mbaillie\SFRZB\Test Models\test5\test5_RoutingQCLog.txt"
# Network diagram direction:
#   "LR" = left to right
#   "RL" = right to left
#   "TB" = top to bottom
#   "BT" = bottom to top
NETWORK_DIRECTION = "TB"
# Show the legend in the DOT/PNG visualization. Set to False for no legend.
SHOW_LEGEND = True
# Highlight potential QC issues in the DOT/PNG visualization. Set to False for a clean visualization.
SHOW_QC_ISSUES = True


# =========================
# RUN
# =========================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        if not SFR_INPUT_PATH or not OUT_CSV_PATH:
            raise SystemExit(
                "Set SFR_INPUT_PATH and OUT_CSV_PATH near the bottom of this script, "
                "or run from the command line with --sfr-input and --out-csv."
            )

        res = parse_routing_table(SFR_INPUT_PATH)
        rt = res["routing_table"]
        lak_path = LAK_INPUT_PATH.strip() if LAK_INPUT_PATH else ""
        lak_info = parse_lak_sublake_systems(lak_path) if lak_path else None
        rt = update_hanging_subnetwork_flags(rt, lak_info=lak_info)
        zone_path = ZONE_INPUT_PATH.strip() if ZONE_INPUT_PATH else ""
        zone_info = parse_zone_input(zone_path) if zone_path else None
        if lak_info:
            print(f"LAK NLAKES: {lak_info['nlakes']}")
            print(f"LAK NSLMS: {lak_info['nslms']}")
        if zone_info:
            visible_zones = sorted(z for z in zone_info.get("zone_to_nodes", {}) if int(z) != 0)
            print(f"Zone file zones: {', '.join(str(z) for z in visible_zones) if visible_zones else 'none above 0'}")

        os.makedirs(os.path.dirname(OUT_CSV_PATH) or ".", exist_ok=True)
        rt.to_csv(OUT_CSV_PATH, index=False)
        print(f"Wrote CSV: {OUT_CSV_PATH}")

        dot_written = False
        dot_path = OUT_DOT_PATH.strip() if OUT_DOT_PATH else ""

        if dot_path:
            write_dot(rt, dot_path, NETWORK_DIRECTION, SHOW_LEGEND, SHOW_QC_ISSUES, lak_info, zone_info)
            dot_written = True
            print(f"Wrote DOT: {dot_path}")

        if OUT_PNG_PATH and OUT_PNG_PATH.strip():
            png_path = OUT_PNG_PATH.strip()
            if not dot_written:
                root, _ = os.path.splitext(png_path)
                dot_path = root + ".dot"
                write_dot(rt, dot_path, NETWORK_DIRECTION, SHOW_LEGEND, SHOW_QC_ISSUES, lak_info, zone_info)
                print(f"Wrote DOT: {dot_path}")

            render_png_from_dot(dot_path, png_path)
            print(f"Wrote PNG: {png_path}")

        if OUT_LOG_PATH and OUT_LOG_PATH.strip():
            log_path = OUT_LOG_PATH.strip()
            write_qc_log(rt, log_path)
            print(f"Wrote QC log: {log_path}")
