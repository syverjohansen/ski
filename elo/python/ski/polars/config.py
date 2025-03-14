# Nation quotas based on FIS World Cup rules
NATION_QUOTAS = {
    'Andorra': {'men': 3, 'ladies': 3},
    'Armenia': {'men': 2, 'ladies': 3},
    'Australia': {'men': 3, 'ladies': 3},
    'Austria': {'men': 5, 'ladies': 5},
    'Bulgaria': {'men': 3, 'ladies': 2},
    'Canada': {'men': 5, 'ladies': 5},
    'China': {'men': 3, 'ladies': 2},
    'Czechia': {'men': 5, 'ladies': 5},
    'Estonia': {'men': 4, 'ladies': 4},
    'Spain': {'men': 3, 'ladies': 2},
    'Finland': {'men': 6, 'ladies': 6},
    'France': {'men': 6, 'ladies': 6},
    'Great Britain': {'men': 5, 'ladies': 2},
    'Germany': {'men': 6, 'ladies': 6},
    'Italy': {'men': 6, 'ladies': 6},
    'Ireland': {'men': 4, 'ladies': 2},
    'Japan': {'men': 4, 'ladies': 3},
    'Kazakhstan': {'men': 3, 'ladies': 4},
    'Latvia': {'men': 3, 'ladies': 5},
    'Norway': {'men': 6, 'ladies': 6},
    'Poland': {'men': 3, 'ladies': 4},
    'Slovenia': {'men': 4, 'ladies': 4},
    'Switzerland': {'men': 6, 'ladies': 6},
    'Sweden': {'men': 6, 'ladies': 6},
    'Ukraine': {'men': 2, 'ladies': 3},
    'USA': {'men': 6, 'ladies': 6}
}

# Default quota for unlisted nations
DEFAULT_QUOTA = {'men': 2, 'ladies': 2}

# World Cup leaders (get extra start)
WC_LEADERS = {
    'men': ['Norway'],
    'ladies': ['USA']
}

def get_nation_quota(nation: str, gender: str, is_host: bool = False) -> int:
    """
    Get the quota for a nation considering:
    1. Base quota from NATION_QUOTAS or default
    2. Host country bonus (+5 if applicable)
    3. World Cup leader bonus (+1 if applicable)
    
    Example:
    >>> get_nation_quota('Norway', 'men', is_host=True)
    12  # Base(6) + Leader(1) + Host(5)
    
    Args:
        nation: The nation code
        gender: 'men' or 'ladies'
        is_host: Whether this nation is hosting
    
    Returns:
        int: The total quota for the nation
    """
    # Get base quota
    base_quota = NATION_QUOTAS.get(nation, DEFAULT_QUOTA).get(gender, DEFAULT_QUOTA[gender])
    
    # Add host country bonus
    if is_host:
        base_quota += 5
        
    # Add World Cup leader bonus
    if nation in WC_LEADERS.get(gender, []):
        base_quota += 1
        
    return base_quota

# Add additional skiers organized by country
ADDITIONAL_SKIERS_MEN = {

}

ADDITIONAL_SKIERS_LADIES = {

}

def get_additional_skiers(gender: str) -> dict:
    """Get additional skiers for specified gender"""
    if gender == 'men':
        return ADDITIONAL_SKIERS_MEN
    elif gender == 'ladies':
        return ADDITIONAL_SKIERS_LADIES
    else:
        raise ValueError(f"Unknown gender: {gender}")

# For backward compatibility
ADDITIONAL_SKIERS = ADDITIONAL_SKIERS_MEN