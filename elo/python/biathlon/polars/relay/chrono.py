import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
    # Use explicit list like main chrono.py to ensure consistent behavior
    elo_cols = [
        "Elo", 
        "Individual_Elo", "Sprint_Elo", "Pursuit_Elo", "MassStart_Elo"
    ]
    pelo_cols = [
        "Pelo", 
        "Individual_Pelo", "Sprint_Pelo", "Pursuit_Pelo", "MassStart_Pelo"
    ]
    
    # Create a list of expressions for forward fill
    exprs = [
        pl.col(col).fill_null(strategy="forward").alias(col)
        for col in elo_cols + pelo_cols if col in df.columns
    ]
    
    # Add all other columns to preserve them
    other_cols = [col for col in df.columns if col not in elo_cols + pelo_cols]
    exprs.extend([pl.col(col) for col in other_cols])
    
    return df.select(exprs)

def apply_offseason_rules(df):
    """
    Apply special rules for offseason rows:
    1. For max Season, set Elo = Pelo
    2. For other seasons, apply discount formula: Elo = Pelo * 0.85 + 1300 * 0.15
    """
    # Get the maximum season
    max_season = df['Season'].max()
    
    # Find all Elo and Pelo column pairs
    elo_cols = [col for col in df.columns if col.endswith('_Elo') or col == 'Elo']
    pelo_cols = [col for col in df.columns if col.endswith('_Pelo') or col == 'Pelo']
    
    # Map each Elo column to its corresponding Pelo column
    elo_pelo_pairs = []
    for elo_col in elo_cols:
        if elo_col == 'Elo':
            pelo_col = 'Pelo'
        else:
            pelo_col = elo_col.replace('_Elo', '_Pelo')
        
        if pelo_col in df.columns:
            elo_pelo_pairs.append((elo_col, pelo_col))
    
    # For each Elo-Pelo pair, apply the rules
    for elo_col, pelo_col in elo_pelo_pairs:
        # Apply the rules based on whether it's max season or not
        df = df.with_columns([
            pl.when((pl.col("RaceType") == "Offseason") & (pl.col("Season") == max_season))
            .then(pl.col(pelo_col))  # For max season, use Pelo value
            .when(pl.col("RaceType") == "Offseason")
            .then(pl.col(pelo_col) * 0.85 + 1300 * 0.15)  # For other seasons, apply discount
            .otherwise(pl.col(elo_col))
            .alias(elo_col)
        ])
    
    return df

def ladies():
    # Read all ladies files with consistent path
    base_path = '~/ski/elo/python/biathlon/polars/relay/excel365'
    
    # Load the main Elo files
    L = pl.read_csv(f'{base_path}/L.csv', schema_overrides={"Distance": pl.String})
    
    # Load and rename specific race type Elo files
    L_Individual = pl.read_csv(f'{base_path}/L_Individual.csv', schema_overrides={"Distance": pl.String})
    L_Individual = L_Individual.rename({"Pelo": "Individual_Pelo", "Elo": "Individual_Elo"})
    
    L_Sprint = pl.read_csv(f'{base_path}/L_Sprint.csv', schema_overrides={"Distance": pl.String})
    L_Sprint = L_Sprint.rename({"Pelo": "Sprint_Pelo", "Elo": "Sprint_Elo"})
    
    L_Pursuit = pl.read_csv(f'{base_path}/L_Pursuit.csv', schema_overrides={"Distance": pl.String})
    L_Pursuit = L_Pursuit.rename({"Pelo": "Pursuit_Pelo", "Elo": "Pursuit_Elo"})
    
    L_Mass_Start = pl.read_csv(f'{base_path}/L_Mass_Start.csv', schema_overrides={"Distance": pl.String})
    L_Mass_Start = L_Mass_Start.rename({"Pelo": "MassStart_Pelo", "Elo": "MassStart_Elo"})
    
    print("Done reading ladies files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'RaceType', 'MassStart', 
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp', 'Leg'
    ]

    dfs = [L, L_Individual, L_Sprint, L_Pursuit, L_Mass_Start]
    
    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.join(df, on=common_columns, how="left")

    # Fill nulls forward within each ID group
    merged_df = (
        merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
        .group_by('ID')
        .map_groups(fill_nulls_forward)
    )

    # Fill Pelo values with corresponding Elo values when Pelo is null
    elo_cols = ["Elo", "Individual_Elo", "Sprint_Elo", "Pursuit_Elo", "MassStart_Elo"]
    pelo_cols = ["Pelo", "Individual_Pelo", "Sprint_Pelo", "Pursuit_Pelo", "MassStart_Pelo"]

    for i in range(len(elo_cols)):
        if elo_cols[i] in merged_df.columns and pelo_cols[i] in merged_df.columns:
            merged_df = merged_df.with_columns(
                pl.when(pl.col(pelo_cols[i]).is_null())
                .then(pl.col(elo_cols[i]))
                .otherwise(pl.col(pelo_cols[i]))
                .alias(pelo_cols[i])
            )

    # Apply offseason rules
    merged_df = apply_offseason_rules(merged_df)
    
    # Sort the final DataFrame
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    return merged_df

def men():
    # Read all men's files with consistent path
    base_path = '~/ski/elo/python/biathlon/polars/relay/excel365'
    
    # Load the main Elo files
    M = pl.read_csv(f'{base_path}/M.csv', schema_overrides={"Distance": pl.String})
    
    # Load and rename specific race type Elo files
    M_Individual = pl.read_csv(f'{base_path}/M_Individual.csv', schema_overrides={"Distance": pl.String})
    M_Individual = M_Individual.rename({"Pelo": "Individual_Pelo", "Elo": "Individual_Elo"})
    
    M_Sprint = pl.read_csv(f'{base_path}/M_Sprint.csv', schema_overrides={"Distance": pl.String})
    M_Sprint = M_Sprint.rename({"Pelo": "Sprint_Pelo", "Elo": "Sprint_Elo"})
    
    M_Pursuit = pl.read_csv(f'{base_path}/M_Pursuit.csv', schema_overrides={"Distance": pl.String})
    M_Pursuit = M_Pursuit.rename({"Pelo": "Pursuit_Pelo", "Elo": "Pursuit_Elo"})
    
    M_Mass_Start = pl.read_csv(f'{base_path}/M_Mass_Start.csv', schema_overrides={"Distance": pl.String})
    M_Mass_Start = M_Mass_Start.rename({"Pelo": "MassStart_Pelo", "Elo": "MassStart_Elo"})
    
    print("Done reading men's files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'RaceType', 'MassStart', 
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp', 'Leg'
    ]

    dfs = [M, M_Individual, M_Sprint, M_Pursuit, M_Mass_Start]
    
    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.join(df, on=common_columns, how="left")

    # Fill nulls forward within each ID group
    merged_df = (
        merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
        .group_by('ID')
        .map_groups(fill_nulls_forward)
    )

    # Fill Pelo values with corresponding Elo values when Pelo is null
    elo_cols = ["Elo", "Individual_Elo", "Sprint_Elo", "Pursuit_Elo", "MassStart_Elo"]
    pelo_cols = ["Pelo", "Individual_Pelo", "Sprint_Pelo", "Pursuit_Pelo", "MassStart_Pelo"]

    for i in range(len(elo_cols)):
        if elo_cols[i] in merged_df.columns and pelo_cols[i] in merged_df.columns:
            merged_df = merged_df.with_columns(
                pl.when(pl.col(pelo_cols[i]).is_null())
                .then(pl.col(elo_cols[i]))
                .otherwise(pl.col(pelo_cols[i]))
                .alias(pelo_cols[i])
            )

    # Apply offseason rules
    merged_df = apply_offseason_rules(merged_df)
    
    # Sort the final DataFrame
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    return merged_df

# Main execution
ladiesdf = ladies()
mendf = men()

# Print unique nations for verification
pl.Config.set_tbl_rows(100)
ladies_nation = ladiesdf.select("Nation").unique().sort(["Nation"])
men_nation = mendf.select("Nation").unique().sort(["Nation"])
print(ladies_nation)
print(men_nation)

# Save the final files
ladiesdf.write_csv("~/ski/elo/python/biathlon/polars/relay/excel365/ladies_chrono.csv")
mendf.write_csv("~/ski/elo/python/biathlon/polars/relay/excel365/men_chrono.csv")

# Also save as CSV for easier viewing/sharing
ladiesdf.write_csv("~/ski/elo/python/biathlon/polars/relay/excel365/ladies_chrono.csv")
mendf.write_csv("~/ski/elo/python/biathlon/polars/relay/excel365/men_chrono.csv")

print(f"Total execution time: {time.time() - start_time:.2f} seconds")