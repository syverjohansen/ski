"""Pure configuration helpers for Alpine FIS schedule acquisition."""


def calendar_sources(season, include_olympics=True):
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
    if include_olympics:
        sources.append((base.format(season=season, category="OWG"), "Olympics"))
    return sources
