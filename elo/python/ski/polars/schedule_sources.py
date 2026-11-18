"""Pure configuration helpers for cross-country FIS schedule acquisition."""


def championship_category(season):
    """Return the senior championship category held within a FIS season."""
    season = int(season)
    if season % 4 == 2:
        return "OWG", "Olympics"
    if season % 2 == 1:
        return "WSC", "World Championships"
    return None


def calendar_sources(season, include_championships=True):
    """Return the authoritative cross-country FIS calendar sources."""
    try:
        season = int(season)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"season must be an integer: {season!r}") from exc
    if season < 2000 or season > 2100:
        raise ValueError(f"season is outside the supported range: {season}")

    base = (
        "https://www.fis-ski.com/DB/cross-country/calendar-results.html?"
        "eventselection=&place=&sectorcode=CC&seasoncode={season}&"
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


def championship_value(source_category):
    return "1" if source_category in {"Olympics", "World Championships"} else "0"
