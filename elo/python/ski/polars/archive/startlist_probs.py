import pandas as pd
import numpy as np
from config import get_additional_skiers

def process_gender(gender: str, race_types: list[str]) -> None:
    """Process a single gender's data"""
    # Load the appropriate startlist
    input_path = f'~/ski/elo/python/ski/polars/excel365/startlist_scraped_{gender}.feather'
    output_path = f'~/ski/elo/python/ski/polars/excel365/startlist_with_probs_{gender}.feather'
    
    df = pd.read_feather(input_path)
    result_df = calculate_race_probabilities(df, race_types, gender)
    
    # Save results
    result_df.to_feather(output_path)
    
    print(f"\n{gender.capitalize()} Startlist with Race Probabilities:")
    print(result_df.to_string(index=False))

def process_additional_skiers(df: pd.DataFrame, n_races: int, gender: str) -> pd.DataFrame:
    """
    Add a column for each race indicating if a skier is known to participate
    """
    # Add columns for known participation
    for i in range(n_races):
        df[f'Known_Race{i}'] = False
    
    # Get the appropriate ADDITIONAL_SKIERS dictionary
    ADDITIONAL_SKIERS = get_additional_skiers(gender)
    
    # Process the known race participation from ADDITIONAL_SKIERS
    for nation, skiers in ADDITIONAL_SKIERS.items():
        for skier in skiers:
            if isinstance(skier, dict):  # Skier has specific race information
                name = skier['name']
                races = skier['races']
                if races is not None:
                    # Try to find the skier using both FIS_Name and Skier columns
                    # First try exact match
                    mask = (df['FIS_Name'] == name) | (df['Skier'] == name)
                    
                    # If no exact match, try fuzzy matching
                    if not mask.any():
                        from thefuzz import fuzz
                        
                        def fuzzy_match_score(row):
                            fis_score = fuzz.ratio(name.lower(), row['FIS_Name'].lower())
                            skier_score = fuzz.ratio(name.lower(), row['Skier'].lower())
                            return max(fis_score, skier_score)
                        
                        scores = df.apply(fuzzy_match_score, axis=1)
                        best_match_idx = scores.idxmax()
                        
                        if scores[best_match_idx] >= 85:  # Using same threshold as in startlist-scrape.py
                            mask = df.index == best_match_idx
                    
                    # Set known participation for specified races
                    if mask.any():
                        for race_idx in races:
                            df.loc[mask, f'Known_Race{race_idx}'] = True
                    else:
                        print(f"Warning: Could not find match for {name} in {nation}")
    
    return df

def calculate_race_probabilities(df: pd.DataFrame, race_types: list[str], gender: str) -> pd.DataFrame:
    """
    Calculate race probabilities for each athlete over a race weekend
    """
    # Create copy of DataFrame to modify
    result_df = df.copy()
    
    # Process known race participation
    result_df = process_additional_skiers(result_df, len(race_types), gender)
    
    # Add columns for each race probability, initialized to 0
    for i in range(len(race_types)):
        result_df[f'Race{i+1}_Prob'] = 0.0
    
    # Check if we have any FIS list entries
    has_fis_entries = result_df['In_FIS_List'].any()
    
    if has_fis_entries:
        # Calculate nation quotas based on first race entries
        first_race_entries = result_df[result_df['In_FIS_List']]
        nation_quotas = first_race_entries.groupby('Nation').size()
        
        # Print quotas
        print("\nNation Quotas based on first race:")
        for nation, quota in nation_quotas.items():
            print(f"{nation}: {quota}")
    else:
        print("\nNo FIS entries found - using default quotas of 6 per nation")
        # Get unique nations from the DataFrame and set default quota of 6
        nations = result_df['Nation'].unique()
        nation_quotas = pd.Series({nation: 7 for nation in nations})
            
    # Set Race1 probabilities based on FIS startlist if available
    if has_fis_entries:
        result_df.loc[result_df['In_FIS_List'], 'Race1_Prob'] = 1.0
    
    # Process each race
    for i, race_type in enumerate(race_types):
        race_prob_col = f'Race{i+1}_Prob'
        elo_col = f'{race_type}_Elo'
        known_col = f'Known_Race{i}'
        
        print(f"\nProcessing Race {i+1} ({race_type}):")
        
        # First, set probability to 1 for skiers known to participate
        result_df.loc[result_df[known_col], race_prob_col] = 1.0
        
        # Special case for two-race weekend
        if len(race_types) == 2 and i == 1:
            # Set probability to 1 for additional skiers without known race info
            mask = (~result_df['In_FIS_List']) & (~result_df[known_col])
            result_df.loc[mask, race_prob_col] = 1.0
        
        # Process each nation
        for nation, quota in nation_quotas.items():
            nation_skiers = result_df[result_df['Nation'] == nation].copy()
            
            if len(nation_skiers) == 0:
                continue
            
            # Calculate spots taken by known participants
            known_spots = nation_skiers[known_col].sum()
            
            # For 3+ race weekends, distribute remaining additional skiers
            if len(race_types) > 2 and i > 0:
                # Handle additional skiers without known race information
                additional_unknown = nation_skiers[
                    (~nation_skiers['In_FIS_List']) & 
                    (~nation_skiers[known_col])
                ]
                
                if len(additional_unknown) > 0:
                    remaining_for_additional = max(0, quota - known_spots)
                    if remaining_for_additional > 0:
                        # Sort by ELO and assign probabilities
                        additional_sorted = additional_unknown.sort_values(elo_col, ascending=False)
                        probs = np.exp(-np.arange(len(additional_unknown)) / 2)
                        probs = np.minimum(1.0, probs * (remaining_for_additional / probs.sum()))
                        result_df.loc[additional_sorted.index, race_prob_col] = probs
            
            # Calculate remaining spots for FIS list skiers
            filled_spots = result_df[
                (result_df['Nation'] == nation)
            ][race_prob_col].sum()
            
            remaining_spots = max(0, quota - filled_spots)
            
            print(f"\n{nation}:")
            print(f"  Quota: {quota}")
            print(f"  Known participants: {known_spots}")
            print(f"  Total filled spots: {filled_spots:.2f}")
            print(f"  Remaining spots: {remaining_spots:.2f}")
            
            if remaining_spots <= 0:
                continue
            
            # Get potential entrants based on whether we have FIS entries
            if has_fis_entries:
                potential_entrants = nation_skiers[
                    (nation_skiers['In_FIS_List']) & 
                    (~nation_skiers[known_col]) &
                    (nation_skiers[elo_col].notna())
                ].sort_values(elo_col, ascending=False)
            else:
                # When no FIS entries, consider all non-known skiers as potential entrants
                potential_entrants = nation_skiers[
                    (~nation_skiers[known_col]) &
                    (nation_skiers[elo_col].notna())
                ].sort_values(elo_col, ascending=False)
                print(f"\nDEBUG for {nation}:")
                print(f"Potential entrants:\n{potential_entrants['Skier'].tolist()}")

            
            if len(potential_entrants) > 0:
                if len(potential_entrants) <= remaining_spots:
                    # If we have fewer or equal skiers than remaining spots, all get probability 1.0
                    print(f"  Fewer skiers ({len(potential_entrants)}) than remaining spots ({remaining_spots})")
                    print("  Assigning probability 1.0 to all potential entrants")
                    result_df.loc[potential_entrants.index, race_prob_col] = 1.0
                else:
                    # Only calculate probabilities if we have more skiers than spots
                    probs = np.exp(-np.arange(len(potential_entrants)) / 2)
                    probs = np.minimum(1.0, probs * (remaining_spots / probs.sum()))
                    
                    print(f"  Assigning probabilities to {len(potential_entrants)} potential entrants")
                    print(f"  Top 3 probabilities: {probs[:3] if len(probs) >= 3 else probs}")
                    
                    result_df.loc[potential_entrants.index, race_prob_col] = probs
    
    # Clean up by removing the temporary Known_ columns
    result_df = result_df.drop(columns=[f'Known_Race{i}' for i in range(len(race_types))])
    
    return result_df


def main():
    race_types = ['Sprint_F', 'Distance_C']
    
    # Process men's data
    print("\nProcessing men's data...")
    process_gender('men', race_types)
    
    # Process ladies' data
    print("\nProcessing ladies' data...")
    process_gender('ladies', race_types)

if __name__ == "__main__":
    main()