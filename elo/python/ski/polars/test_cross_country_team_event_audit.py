import csv
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

from cross_country_team_event_audit import audit_team_events, build_report

HERE = Path(__file__).resolve().parent


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class CrossCountryTeamEventAuditTests(unittest.TestCase):
    def test_completed_2026_team_schedule_has_no_gaps(self) -> None:
        data = HERE / "excel365"
        team_data = HERE / "relay" / "excel365"
        audit = audit_team_events(
            schedule=data / "races2026.csv",
            men_relay_chrono=team_data / "men_relay_chrono.csv",
            ladies_relay_chrono=team_data / "ladies_relay_chrono.csv",
            team_sprint_chrono=team_data / "team_sprint_chrono.csv",
            as_of=date(2026, 3, 22),
        )
        report = build_report(audit, as_of=date(2026, 3, 22))

        self.assertEqual(audit.scheduled_team_events, 8)
        self.assertEqual(audit.future_team_events, 0)
        self.assertEqual(audit.completed_team_events, 8)
        self.assertEqual(audit.covered_relay_events, 2)
        self.assertEqual(audit.covered_team_sprint_events, 6)
        self.assertGreater(audit.accepted_team_rows, 0)
        self.assertEqual(audit.missing, ())
        self.assertEqual(report["missing_team_event_count"], 0)

    def test_current_2027_team_schedule_is_future_before_opener(self) -> None:
        data = HERE / "excel365"
        team_data = HERE / "relay" / "excel365"
        audit = audit_team_events(
            schedule=data / "races.csv",
            men_relay_chrono=team_data / "men_relay_chrono.csv",
            ladies_relay_chrono=team_data / "ladies_relay_chrono.csv",
            team_sprint_chrono=team_data / "team_sprint_chrono.csv",
            as_of=date(2026, 11, 18),
        )

        self.assertEqual(audit.scheduled_team_events, 6)
        self.assertEqual(audit.future_team_events, 6)
        self.assertEqual(audit.completed_team_events, 0)
        self.assertEqual(audit.missing, ())

    def test_missing_team_event_is_reported_and_cli_can_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schedule = root / "races.csv"
            men_relay = root / "men_relay.csv"
            ladies_relay = root / "ladies_relay.csv"
            team_sprint = root / "team_sprint.csv"
            output = root / "audit.json"
            write_csv(
                schedule,
                [
                    "Date",
                    "Startlist",
                    "Sex",
                    "City",
                    "Country",
                    "Distance",
                    "Technique",
                    "Race",
                    "Championship",
                ],
                [
                    {
                        "Date": "01/23/2026",
                        "Startlist": "https://example/19",
                        "Sex": "M",
                        "City": "Goms",
                        "Country": "Switzerland",
                        "Distance": "Ts",
                        "Technique": "F",
                        "Race": 19,
                        "Championship": 0,
                    }
                ],
            )
            chrono_fields = ["Date", "Season", "Race", "Sex", "Distance"]
            write_csv(men_relay, chrono_fields, [])
            write_csv(ladies_relay, chrono_fields, [])
            write_csv(team_sprint, chrono_fields, [])

            completed = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "cross_country_team_event_audit.py"),
                    "--schedule",
                    str(schedule),
                    "--men-relay-chrono",
                    str(men_relay),
                    "--ladies-relay-chrono",
                    str(ladies_relay),
                    "--team-sprint-chrono",
                    str(team_sprint),
                    "--as-of",
                    "2026-01-23",
                    "--output",
                    str(output),
                    "--fail-on-gaps",
                ],
                check=False,
            )

            self.assertEqual(completed.returncode, 2)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["missing_team_event_count"], 1)
            self.assertEqual(report["missing_team_events"][0]["distance"], "Ts")


if __name__ == "__main__":
    unittest.main()
