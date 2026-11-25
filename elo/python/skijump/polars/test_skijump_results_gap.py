from __future__ import annotations

import csv
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from skijump_results_gap import (
    DEFAULT_POLICY_EXCLUSIONS,
    audit_races,
    find_missing_races,
    load_policy_exclusions,
    load_schedule,
)

ROOT = Path(__file__).resolve().parent
ACCEPTED = ROOT / "relay" / "excel365"
SCHEDULE = ROOT / "excel365" / "races.csv"
SCHEDULE_FIELDS = [
    "Date",
    "Final_Date",
    "Startlist",
    "Sex",
    "City",
    "Country",
    "HillSize",
    "RaceType",
    "Period",
    "Elevation",
    "Championship",
    "Race",
]
INDIVIDUAL_FIELDS = ["Date", "Sex", "Season", "Race", "TeamEvent"]
TEAM_FIELDS = ["Date", "Sex", "Season", "Race"]
EXCLUSION_FIELDS = ["fis_race_id", "season", "sex", "date", "event_kind", "reason"]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def schedule_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Date": "11/21/2025",
        "Final_Date": "11/22/2025",
        "Startlist": (
            "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7459"
        ),
        "Sex": "M",
        "City": "Lillehammer",
        "Country": "Norway",
        "HillSize": "140",
        "RaceType": "Large",
        "Period": "1",
        "Elevation": "323",
        "Championship": "0",
        "Race": "1",
    }
    row.update(overrides)
    return row


def empty_histories(tmp_path: Path) -> dict[str, Path]:
    return {
        "men_chrono": write_csv(tmp_path / "men.csv", INDIVIDUAL_FIELDS, []),
        "ladies_chrono": write_csv(tmp_path / "ladies.csv", INDIVIDUAL_FIELDS, []),
        "men_team_chrono": write_csv(tmp_path / "men-team.csv", TEAM_FIELDS, []),
        "ladies_team_chrono": write_csv(tmp_path / "ladies-team.csv", TEAM_FIELDS, []),
        "mixed_team_chrono": write_csv(tmp_path / "mixed-team.csv", TEAM_FIELDS, []),
    }


def test_uses_final_date_and_separates_individual_and_mixed_team(
    tmp_path: Path,
) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [
            schedule_row(),
            schedule_row(
                Date="11/20/2025",
                Final_Date="11/21/2025",
                Startlist=(
                    "https://www.fis-ski.com/DB/general/results.html?"
                    "sectorcode=JP&raceid=7460"
                ),
                Sex="Mixed",
                RaceType="Team Large",
            ),
        ],
    )
    completed = load_schedule(schedule, date(2025, 11, 21))
    assert len(completed) == 1
    assert completed[0][1].event_kind == "team"
    assert completed[0][1].sex == "Mixed"


def test_wrong_sex_rows_cannot_satisfy_mixed_team_gate(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [schedule_row(Sex="Mixed", RaceType="Team Large")],
    )
    histories = empty_histories(tmp_path)
    write_csv(
        histories["mixed_team_chrono"],
        TEAM_FIELDS,
        [{"Date": "2025-11-22", "Sex": "M", "Season": 2026, "Race": 1}],
    )
    missing, completed = find_missing_races(
        schedule=schedule, as_of=date(2025, 11, 22), **histories
    )
    assert completed == 1
    assert [(item.sex, item.event_kind) for item in missing] == [("Mixed", "team")]


def test_all_history_types_can_close_their_own_schedule_rows(tmp_path: Path) -> None:
    rows = [
        schedule_row(),
        schedule_row(
            Sex="L",
            Race=1,
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7461"
            ),
        ),
        schedule_row(
            Sex="M",
            Race=2,
            RaceType="Team Large",
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7462"
            ),
        ),
        schedule_row(
            Sex="L",
            Race=2,
            RaceType="Team Normal",
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7463"
            ),
        ),
        schedule_row(
            Sex="Mixed",
            Race=3,
            RaceType="Team Normal",
            Startlist=(
                "https://www.fis-ski.com/DB/general/results.html?sectorcode=JP&raceid=7464"
            ),
        ),
    ]
    schedule = write_csv(tmp_path / "races.csv", SCHEDULE_FIELDS, rows)
    histories = empty_histories(tmp_path)
    write_csv(
        histories["men_chrono"],
        INDIVIDUAL_FIELDS,
        [{"Date": "2025-11-22", "Sex": "M", "Season": 2026, "Race": 1, "TeamEvent": 0}],
    )
    write_csv(
        histories["ladies_chrono"],
        INDIVIDUAL_FIELDS,
        [{"Date": "2025-11-22", "Sex": "L", "Season": 2026, "Race": 1, "TeamEvent": 0}],
    )
    for key, sex, race in [
        ("men_team_chrono", "M", 2),
        ("ladies_team_chrono", "L", 2),
        ("mixed_team_chrono", "Mixed", 3),
    ]:
        write_csv(
            histories[key],
            TEAM_FIELDS,
            [{"Date": "2025-11-22", "Sex": sex, "Season": 2026, "Race": race}],
        )
    missing, completed = find_missing_races(
        schedule=schedule, as_of=date(2025, 11, 22), **histories
    )
    assert completed == 5
    assert missing == []


def test_history_race_number_does_not_define_event_identity(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row(Race=1)]
    )
    histories = empty_histories(tmp_path)
    write_csv(
        histories["men_chrono"],
        INDIVIDUAL_FIELDS,
        [
            {
                "Date": "2025-11-22",
                "Sex": "M",
                "Season": 2026,
                "Race": 27,
                "TeamEvent": 0,
            }
        ],
    )
    missing, completed = find_missing_races(
        schedule=schedule, as_of=date(2025, 11, 22), **histories
    )
    assert completed == 1
    assert missing == []


def test_different_date_remains_a_gap_even_when_race_number_matches(
    tmp_path: Path,
) -> None:
    schedule = write_csv(
        tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row(Race=1)]
    )
    histories = empty_histories(tmp_path)
    write_csv(
        histories["men_chrono"],
        INDIVIDUAL_FIELDS,
        [
            {
                "Date": "2025-11-21",
                "Sex": "M",
                "Season": 2026,
                "Race": 1,
                "TeamEvent": 0,
            }
        ],
    )
    missing, completed = find_missing_races(
        schedule=schedule, as_of=date(2025, 11, 22), **histories
    )
    assert completed == 1
    assert [(item.date, item.race) for item in missing] == [("2025-11-22", 1)]


def test_rejects_duplicate_semantic_event_with_different_race_numbers(
    tmp_path: Path,
) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [
            schedule_row(Race=1),
            schedule_row(
                Race=99,
                Startlist=(
                    "https://www.fis-ski.com/DB/general/results.html?"
                    "sectorcode=JP&raceid=9999"
                ),
            ),
        ],
    )
    with pytest.raises(ValueError, match="duplicate schedule event"):
        load_schedule(schedule, date(2025, 11, 22))


def test_rejects_non_jp_result_url(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv",
        SCHEDULE_FIELDS,
        [
            schedule_row(
                Startlist=(
                    "https://www.fis-ski.com/DB/general/results.html?sectorcode=CC&raceid=7459"
                )
            )
        ],
    )
    with pytest.raises(ValueError, match="ski-jumping results URL"):
        load_schedule(schedule, date(2025, 11, 22))


def test_rejects_duplicate_schedule_identity(tmp_path: Path) -> None:
    schedule = write_csv(
        tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row(), schedule_row()]
    )
    with pytest.raises(ValueError, match="duplicate schedule event"):
        load_schedule(schedule, date(2025, 11, 22))


def test_cli_writes_report_and_fails_on_gaps(tmp_path: Path) -> None:
    schedule = write_csv(tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row()])
    histories = empty_histories(tmp_path)
    output = tmp_path / "report.json"
    script = Path(__file__).with_name("skijump_results_gap.py")
    command = [
        sys.executable,
        str(script),
        "--schedule",
        str(schedule),
        "--men-chrono",
        str(histories["men_chrono"]),
        "--ladies-chrono",
        str(histories["ladies_chrono"]),
        "--men-team-chrono",
        str(histories["men_team_chrono"]),
        "--ladies-team-chrono",
        str(histories["ladies_team_chrono"]),
        "--mixed-team-chrono",
        str(histories["mixed_team_chrono"]),
        "--as-of",
        "2025-11-22",
        "--output",
        str(output),
        "--fail-on-gaps",
    ]
    result = subprocess.run(command, check=False)
    assert result.returncode == 2
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["schema_version"] == "nordicnumbers.skijump-results-gap/v1"
    assert report["missing_individual_race_count"] == 1
    assert report["missing_team_race_count"] == 0


def test_accepted_2026_histories_leave_only_known_policy_candidates() -> None:
    missing, completed = find_missing_races(
        schedule=SCHEDULE,
        men_chrono=ACCEPTED / "men_chrono.csv",
        ladies_chrono=ACCEPTED / "ladies_chrono.csv",
        men_team_chrono=ACCEPTED / "men_team_chrono.csv",
        ladies_team_chrono=ACCEPTED / "ladies_team_chrono.csv",
        mixed_team_chrono=ACCEPTED / "mixed_team_chrono.csv",
        as_of=date(2026, 11, 24),
    )
    assert completed == 75
    assert {item.fis_race_id for item in missing} == {"7687", "7688"}
    assert all(item.event_kind == "individual" for item in missing)


def test_reviewed_policy_ledger_closes_2026_audit() -> None:
    missing, excluded, completed = audit_races(
        schedule=SCHEDULE,
        men_chrono=ACCEPTED / "men_chrono.csv",
        ladies_chrono=ACCEPTED / "ladies_chrono.csv",
        men_team_chrono=ACCEPTED / "men_team_chrono.csv",
        ladies_team_chrono=ACCEPTED / "ladies_team_chrono.csv",
        mixed_team_chrono=ACCEPTED / "mixed_team_chrono.csv",
        as_of=date(2026, 11, 24),
        policy_exclusions=DEFAULT_POLICY_EXCLUSIONS,
    )
    assert completed == 75
    assert missing == []
    assert {item.race.fis_race_id for item in excluded} == {"7687", "7688"}


def test_policy_exclusion_metadata_must_match_schedule(tmp_path: Path) -> None:
    schedule = write_csv(tmp_path / "races.csv", SCHEDULE_FIELDS, [schedule_row()])
    exclusions = write_csv(
        tmp_path / "exclusions.csv",
        EXCLUSION_FIELDS,
        [
            {
                "fis_race_id": "7459",
                "season": 2026,
                "sex": "L",
                "date": "2025-11-22",
                "event_kind": "individual",
                "reason": "test exclusion",
            }
        ],
    )
    with pytest.raises(ValueError, match="metadata mismatch"):
        audit_races(
            schedule=schedule,
            as_of=date(2025, 11, 22),
            policy_exclusions=exclusions,
            **empty_histories(tmp_path),
        )


def test_policy_exclusion_ids_must_be_unique(tmp_path: Path) -> None:
    row = {
        "fis_race_id": "7459",
        "season": 2026,
        "sex": "M",
        "date": "2025-11-22",
        "event_kind": "individual",
        "reason": "test exclusion",
    }
    exclusions = write_csv(tmp_path / "exclusions.csv", EXCLUSION_FIELDS, [row, row])
    with pytest.raises(ValueError, match="duplicate policy exclusion"):
        load_policy_exclusions(exclusions)
