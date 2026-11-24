#!/usr/bin/env python3
"""Load and apply a versioned FIS Cross-Country World Cup quota contract.

The contract deliberately separates shared nation quota from nominative start
rights. A COC or Overall World Cup right belongs to the named athlete; it must
never become another generic place when that athlete does not start.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from cross_country_results_identity import normalized_name

SCHEMA_VERSION = "nordicnumbers.cross-country-world-cup-quotas/v1"
DEFAULT_CONTRACT_PATH = Path(__file__).with_name(
    "cross_country_world_cup_quotas_2027.json"
)
QUOTA_FIELDS = (
    "quota_contract_version",
    "base_nation_quota",
    "shared_event_quota",
    "personal_quota",
    "personal_start_right",
    "is_host_nation",
    "team_event_weekend_exception",
    "max_nation_starters",
)


class QuotaContractError(RuntimeError):
    """The FIS quota contract is missing, stale, or internally inconsistent."""


@dataclass(frozen=True)
class PersonalStartRight:
    sex: str
    athlete_name: str
    nation: str
    right_type: str
    valid_from: date
    valid_through: date
    competition: str

    def active_on(self, logical_date: date) -> bool:
        return self.valid_from <= logical_date <= self.valid_through


@dataclass(frozen=True)
class QuotaContract:
    contract_version: str
    season: int
    valid_from: date
    valid_through: date
    default_quota: int
    maximum_starters: int
    host_shared_quota: int
    team_event_minimum_quota: int
    nation_codes: dict[str, str]
    quotas: dict[tuple[str, str], int]
    personal_rights: tuple[PersonalStartRight, ...]

    def _nation_name(self, nation: str) -> str:
        value = nation.strip()
        if value in self.nation_codes:
            return self.nation_codes[value]
        by_name = {name.casefold(): name for name in self.nation_codes.values()}
        return by_name.get(value.casefold(), value)

    def base_quota(self, nation: str, sex: str) -> int:
        if sex not in {"M", "L"}:
            raise QuotaContractError(f"unsupported quota sex: {sex!r}")
        return self.quotas.get((self._nation_name(nation), sex), self.default_quota)

    def assignment(
        self,
        *,
        athlete_name: str,
        nation: str,
        sex: str,
        event_country: str,
        logical_date: date,
        team_event_weekend: bool = False,
    ) -> dict[str, str]:
        if not self.valid_from <= logical_date <= self.valid_through:
            raise QuotaContractError(
                f"quota contract {self.contract_version} does not cover "
                f"{logical_date.isoformat()}"
            )
        canonical_nation = self._nation_name(nation)
        host = canonical_nation == self._nation_name(event_country)
        base = self.base_quota(canonical_nation, sex)
        shared = self.host_shared_quota if host else base
        if team_event_weekend and not host:
            shared = max(shared, self.team_event_minimum_quota)
        rights = sorted(
            {
                right.right_type
                for right in self.personal_rights
                if right.sex == sex
                and self._nation_name(right.nation) == canonical_nation
                and normalized_name(right.athlete_name)
                == normalized_name(athlete_name)
                and right.active_on(logical_date)
            }
        )
        return {
            "quota_contract_version": self.contract_version,
            "base_nation_quota": str(base),
            # The host national group is shared. Nominative rights are not.
            "shared_event_quota": str(shared),
            "personal_quota": "1" if rights else "0",
            "personal_start_right": "|".join(rights),
            "is_host_nation": "1" if host else "0",
            "team_event_weekend_exception": "1" if team_event_weekend else "0",
            "max_nation_starters": str(self.maximum_starters),
        }


def _require_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QuotaContractError(f"{label} must be an object")
    return value


def _parse_date(value: Any, label: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise QuotaContractError(f"{label} must be an ISO date") from exc


def load_quota_contract(
    path: Path = DEFAULT_CONTRACT_PATH,
    *,
    logical_date: date | None = None,
) -> QuotaContract:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QuotaContractError(f"cannot read quota contract {path}: {exc}") from exc
    root = _require_dict(document, "quota contract")
    if root.get("schema_version") != SCHEMA_VERSION:
        raise QuotaContractError("unsupported quota contract schema")

    season = int(root.get("season", 0))
    valid_from = _parse_date(root.get("valid_from"), "valid_from")
    valid_through = _parse_date(root.get("valid_through"), "valid_through")
    if valid_from > valid_through:
        raise QuotaContractError("quota contract validity is reversed")
    if logical_date is not None and not valid_from <= logical_date <= valid_through:
        raise QuotaContractError(
            f"quota contract does not cover {logical_date.isoformat()}"
        )

    rules = _require_dict(root.get("rules"), "rules")
    default_quota = int(rules.get("default_basic_quota", 0))
    maximum_starters = int(rules.get("maximum_starters_per_nation_gender", 0))
    host_shared_quota = int(rules.get("host_shared_quota", 0))
    team_event_minimum_quota = int(
        rules.get("team_event_weekend_minimum_quota", 0)
    )
    non_transferable = rules.get("personal_rights_are_non_transferable")
    if (
        default_quota != 2
        or maximum_starters != 10
        or host_shared_quota != 10
        or team_event_minimum_quota != 4
    ):
        raise QuotaContractError("unexpected 2026/27 quota rule constants")
    if non_transferable is not True:
        raise QuotaContractError("personal start rights must be non-transferable")

    nations = root.get("nations")
    if not isinstance(nations, list) or not nations:
        raise QuotaContractError("nations must be a non-empty list")
    nation_codes: dict[str, str] = {}
    quotas: dict[tuple[str, str], int] = {}
    for index, raw in enumerate(nations):
        item = _require_dict(raw, f"nations[{index}]")
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if len(code) != 3 or not name or code in nation_codes:
            raise QuotaContractError(f"invalid or duplicate nation at index {index}")
        nation_codes[code] = name
        for sex, key in (("L", "women"), ("M", "men")):
            quota = int(item.get(key, 0))
            if quota not in {2, 3, 4, 5, 6}:
                raise QuotaContractError(f"invalid {key} quota for {code}")
            quotas[(name, sex)] = quota

    rights_value = root.get("personal_start_rights")
    if not isinstance(rights_value, list):
        raise QuotaContractError("personal_start_rights must be a list")
    rights: list[PersonalStartRight] = []
    seen_rights: set[tuple[str, str, str, str]] = set()
    for index, raw in enumerate(rights_value):
        item = _require_dict(raw, f"personal_start_rights[{index}]")
        sex = str(item.get("sex", ""))
        name = str(item.get("athlete_name", "")).strip()
        nation = str(item.get("nation", "")).strip()
        right_type = str(item.get("right_type", "")).strip()
        competition = str(item.get("competition", "")).strip()
        start = _parse_date(item.get("valid_from"), f"right[{index}].valid_from")
        end = _parse_date(item.get("valid_through"), f"right[{index}].valid_through")
        if (
            sex not in {"M", "L"}
            or not name
            or nation not in nation_codes
            or right_type not in {"overall_world_cup", "continental_cup"}
            or not competition
            or start > end
            or start < valid_from
            or end > valid_through
        ):
            raise QuotaContractError(f"invalid personal start right at index {index}")
        identity = (sex, normalized_name(name), nation, right_type)
        if identity in seen_rights:
            raise QuotaContractError(f"duplicate personal start right at index {index}")
        seen_rights.add(identity)
        rights.append(
            PersonalStartRight(
                sex=sex,
                athlete_name=name,
                nation=nation,
                right_type=right_type,
                valid_from=start,
                valid_through=end,
                competition=competition,
            )
        )

    contract_version = str(root.get("contract_version", "")).strip()
    if not contract_version or season != 2027:
        raise QuotaContractError("missing contract version or unexpected season")
    return QuotaContract(
        contract_version=contract_version,
        season=season,
        valid_from=valid_from,
        valid_through=valid_through,
        default_quota=default_quota,
        maximum_starters=maximum_starters,
        host_shared_quota=host_shared_quota,
        team_event_minimum_quota=team_event_minimum_quota,
        nation_codes=nation_codes,
        quotas=quotas,
        personal_rights=tuple(rights),
    )
