"""Pure configuration for authoritative Nordic-combined FIS calendars."""


def championship_category(season: int) -> tuple[str, str] | None:
    """Return the senior championship category held within a FIS season."""
    season = int(season)
    if season % 4 == 2:
        return "OWG", "Olympics"
    if season % 2 == 1:
        return "WSC", "World Championships"
    return None


def calendar_sources(
    season: int, *, include_championships: bool = True
) -> list[tuple[str, str]]:
    """Return parameterized official FIS Nordic-combined calendar sources."""
    try:
        season = int(season)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"season must be an integer: {season!r}") from exc
    if not 2000 <= season <= 2100:
        raise ValueError(f"season is outside the supported range: {season}")

    template = (
        "https://www.fis-ski.com/DB/nordic-combined/calendar-results.html?"
        "eventselection=&place=&sectorcode=NK&seasoncode={season}&"
        "categorycode={category}&disciplinecode=&gendercode=&racedate=&"
        "racecodex=&nationcode=&seasonmonth=X-{season}&saveselection=-1&"
        "seasonselection="
    )
    sources = [(template.format(season=season, category="WC"), "World Cup")]
    championship = championship_category(season)
    if include_championships and championship:
        category, label = championship
        sources.append((template.format(season=season, category=category), label))
    return sources


def championship_value(source_category: str) -> str:
    return "1" if source_category in {"Olympics", "World Championships"} else "0"
