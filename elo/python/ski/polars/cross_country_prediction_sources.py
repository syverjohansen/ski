"""Source tiers and fixed-anchor comparisons for XC prediction ratings.

Official A-tier results remain the only source of accepted Elo. Lower-tier FIS
and national results may estimate an athlete who has never previously produced
an A-tier result, but only through direct comparisons with fixed A-tier anchors.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlencode

A_TIER_FIS_CATEGORIES = {
    "OWG": "Olympics",
    "WSC": "World Championships",
    "WC": "World Cup",
    "SWC": "Stage World Cup",
}

PREDICTION_ONLY_FIS_CATEGORIES = {
    "WSCQUA": "World Championships Qualification",
    "WJC": "FIS Junior World Ski Championships",
    "U23": "FIS U23 World Ski Championships",
    "AWG": "Asian Winter Games",
    "YOG": "Youth Olympic Winter Games",
    "EYOF": "European Youth Olympic Festival",
    "COC": "Continental Cup",
    "SCAN": "Scandinavian Cup",
    "FESA": "FESA Cup",
    "EEC": "Eastern Europe Cup",
    "UST": "US Super Tour",
    "NAC": "Nor-Am Cup",
    "FEC": "Far East Cup",
    "SEE": "South East Europe Cup",
    "SC": "Slavic Cup",
    "BC": "Balkan Cup",
    "ANC": "Australian New Zealand Cup",
    "SAC": "South American Cup",
    "FIS": "FIS",
    "JUN": "Junior",
    "UVS": "Universiade",
    "UNI": "University",
    "CISM": "Military and Police",
}

EXCLUDED_FIS_CATEGORIES = {
    "CIT": "Citizen",
    "MAS": "Masters",
    "ML": "Popular Cross-Country",
    "WLC": "World Loppet Cup",
}


@dataclass(frozen=True)
class PredictionParticipant:
    athlete_id: str
    place: int
    a_tier_verified: bool
    rating: float | None = None


@dataclass(frozen=True)
class AnchorComparison:
    anchor_athlete_id: str
    anchor_rating: float
    score: float


def classify_fis_category(category: str) -> str:
    """Classify an explicit FIS category without silently accepting unknowns."""
    category = category.strip().upper()
    if category in A_TIER_FIS_CATEGORIES:
        return "a_tier"
    if category in PREDICTION_ONLY_FIS_CATEGORIES:
        return "prediction_only"
    if category in EXCLUDED_FIS_CATEGORIES:
        return "excluded"
    raise ValueError(f"unsupported FIS cross-country category: {category!r}")


def prediction_calendar_url(season: int, category: str) -> str:
    """Return one bounded FIS calendar URL for an approved prediction category."""
    try:
        season = int(season)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"season must be an integer: {season!r}") from exc
    if season < 2000 or season > 2100:
        raise ValueError(f"season is outside the supported range: {season}")
    category = category.strip().upper()
    classification = classify_fis_category(category)
    if classification != "prediction_only":
        raise ValueError(
            f"category {category!r} is {classification}, not prediction-only"
        )
    query = urlencode(
        {
            "eventselection": "",
            "place": "",
            "sectorcode": "CC",
            "seasoncode": season,
            "categorycode": category,
            "disciplinecode": "",
            "gendercode": "",
            "racedate": "",
            "racecodex": "",
            "nationcode": "",
            "seasonmonth": f"X-{season}",
            "saveselection": -1,
            "seasonselection": "",
        }
    )
    return f"https://www.fis-ski.com/DB/cross-country/calendar-results.html?{query}"


def build_fixed_anchor_comparisons(
    participants: list[PredictionParticipant],
) -> dict[str, tuple[AnchorComparison, ...]]:
    """Compare never-A-tier athletes only with immutable A-tier anchors.

    Unknown-versus-unknown ordering is intentionally absent. Two unknowns in
    the same interval between anchors therefore receive identical race evidence.
    """
    if len(participants) < 2:
        raise ValueError("a prediction evidence race needs at least two participants")
    athlete_ids = [participant.athlete_id for participant in participants]
    if any(not athlete_id.strip() for athlete_id in athlete_ids):
        raise ValueError("prediction participants need stable athlete IDs")
    if len(athlete_ids) != len(set(athlete_ids)):
        raise ValueError("prediction participants contain duplicate athlete IDs")
    if any(participant.place < 1 for participant in participants):
        raise ValueError("prediction participant places must be positive")

    anchors = []
    for participant in participants:
        if not participant.a_tier_verified:
            continue
        if participant.rating is None:
            raise ValueError(
                f"A-tier anchor {participant.athlete_id} has no official rating"
            )
        anchors.append(participant)

    comparisons: dict[str, tuple[AnchorComparison, ...]] = {}
    for participant in participants:
        if participant.a_tier_verified:
            continue
        comparisons[participant.athlete_id] = tuple(
            AnchorComparison(
                anchor_athlete_id=anchor.athlete_id,
                anchor_rating=float(anchor.rating),
                score=(
                    1.0
                    if participant.place < anchor.place
                    else 0.0
                    if participant.place > anchor.place
                    else 0.5
                ),
            )
            for anchor in anchors
        )
    return comparisons
