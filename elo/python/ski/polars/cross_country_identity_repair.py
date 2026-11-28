#!/usr/bin/env python3
"""Replay a season after reviewed aliases reveal provisional duplicate IDs.

Writes an isolated candidate. Prior seasons remain byte-for-byte identical;
newcomer starting ratings are retained from their first accepted pre-race values.
No accepted history is changed by this command.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from cross_country_results_identity import load_aliases, load_history
from cross_country_results_elo_replay import PREVIOUS_COLUMNS, replay_results


def read(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames), list(reader)


def write(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def repair(*, men: Path, ladies: Path, aliases: Path, season: int, output: Path):
    if output.exists():
        raise ValueError("repair output already exists")
    athletes, _, names = load_history(men, ladies)
    reviewed = load_aliases(aliases, athletes, names)
    output.mkdir(parents=True)
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        candidates = []
        sources = {}
        repaired = set()
        for sex, source in [("M", men), ("L", ladies)]:
            fields, rows = read(source)
            if any(int(r["Season"]) > season for r in rows):
                raise ValueError("repair must include every subsequent season")
            current = [r for r in rows if int(r["Season"]) == season]
            if any(
                r["Event"] == "Offseason" or r["Distance"] in {"Rel", "Ts"}
                for r in current
            ):
                raise ValueError("repair requires an open individual season")
            prior = [r for r in rows if int(r["Season"]) < season]
            baseline = list(prior)
            known = {r["ID"] for r in prior}
            for row in sorted(current, key=lambda r: (r["Date"], int(r["Race"]))):
                alias = reviewed.get((sex, row["ID"]))
                canonical_id = alias.canonical_id if alias else row["ID"]
                name = athletes[(sex, canonical_id)].name
                nation = athletes[(sex, canonical_id)].nation
                if alias:
                    repaired.add((sex, row["ID"], canonical_id))
                if canonical_id not in known:
                    seed = dict(
                        row,
                        ID=canonical_id,
                        Skier=name,
                        Nation=nation,
                        Date=f"{season - 1}-07-01",
                        Race="0",
                        Event="Offseason",
                        Exp=str(int(float(row["Exp"])) - 1),
                    )
                    for stream, previous in PREVIOUS_COLUMNS.items():
                        seed[stream] = row[previous]
                    baseline.append(seed)
                    known.add(canonical_id)
                candidates.append(
                    {
                        "date": row["Date"][:10],
                        "sex": sex,
                        "season": row["Season"],
                        "race": row["Race"],
                        "city": row["City"],
                        "country": row["Country"],
                        "distance": row["Distance"],
                        "technique": row["Technique"],
                        "mass_start": row["MS"],
                        "championship": "1"
                        if row["Event"]
                        in {"World Championship", "Olympic Winter Games"}
                        else "0",
                        "place": row["Place"],
                        "canonical_athlete_id": canonical_id,
                        "canonical_name": name,
                        "canonical_nation": nation,
                    }
                )
            baseline_path = root / f"{sex}.csv"
            write(baseline_path, fields, baseline)
            sources[sex] = (source, fields, prior, baseline_path)
        if not repaired:
            raise ValueError("no provisional duplicate IDs have reviewed aliases")
        mapped = root / "mapped.csv"
        write(mapped, list(candidates[0]), candidates)
        report = replay_results(
            mapped_results=mapped,
            men_chrono=sources["M"][3],
            ladies_chrono=sources["L"][3],
            output_dir=output / "replay",
        )
        for sex, label in [("M", "men"), ("L", "ladies")]:
            source, fields, prior, _ = sources[sex]
            _, replayed = read(output / "replay" / f"{label}_elo_rows.csv")
            # Preserve all archival CSV bytes, including historical formatting.
            with source.open("rb") as handle:
                header = handle.readline()
                archival = [
                    line
                    for line in handle
                    if int(
                        next(csv.reader([line.decode("utf-8")]))[fields.index("Season")]
                    )
                    < season
                ]
            destination = output / f"{label}_chrono.csv"
            write(destination, fields, replayed)
            generated = destination.read_bytes().split(b"\n", 1)[1]
            destination.write_bytes(header + b"".join(archival) + generated)
        report["accepted_sources_sha256"] = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (men, ladies, aliases)
        }
        report["prior_seasons_preserved"] = True
        report["repaired_identities"] = [list(item) for item in sorted(repaired)]
        (output / "repair-report.json").write_text(json.dumps(report, indent=2) + "\n")
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--men", type=Path, required=True)
    parser.add_argument("--ladies", type=Path, required=True)
    parser.add_argument("--aliases", type=Path, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    print(json.dumps(repair(**vars(parser.parse_args())), indent=2))
