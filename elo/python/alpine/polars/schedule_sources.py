"""Pure configuration helpers for Alpine FIS schedule acquisition."""


ALPINE_DISCIPLINES = (
    "Slalom",
    "Giant Slalom",
    "Super G",
    "Downhill",
    "Combined",
    "Alpine Combined",
)
EXCLUDED_RACE_LABELS = ("Training", "Qualification", "Team")


def championship_value(source_category):
    """Return the legacy championship flag for a FIS source label."""
    return "1" if source_category in {"Olympics", "World Championships"} else "0"


def prediction_discipline(label):
    """Return a target race label, excluding non-prediction event rows."""
    normalized = label.strip()
    lowered = normalized.casefold()
    if any(excluded.casefold() in lowered for excluded in EXCLUDED_RACE_LABELS):
        return ""
    if any(discipline.casefold() in lowered for discipline in ALPINE_DISCIPLINES):
        return normalized
    return ""


def championship_category(season):
    """Return the senior championship category held within a FIS season."""
    season = int(season)
    if season % 4 == 2:
        return "OWG", "Olympics"
    if season % 2 == 1:
        return "WSC", "World Championships"
    return None


def calendar_sources(season, include_championships=True):
    """Return the authoritative FIS calendar sources for a season."""
    try:
        season = int(season)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"season must be an integer: {season!r}") from exc
    if season < 2000 or season > 2100:
        raise ValueError(f"season is outside the supported range: {season}")

    base = (
        "https://www.fis-ski.com/DB/alpine-skiing/calendar-results.html?"
        "eventselection=&place=&sectorcode=AL&seasoncode={season}&"
        "categorycode={category}&disciplinecode=&gendercode=&racedate=&"
        "racecodex=&nationcode=&seasonmonth=X-{season}&saveselection=-1&"
        "seasonselection="
    )
    sources = [(base.format(season=season, category="WC"), "World Cup")]
    championship = championship_category(season)
    if include_championships and championship:
        category, label = championship
        sources.append((base.format(season=season, category=category), label))
    return sources
