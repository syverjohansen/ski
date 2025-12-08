import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
    # Modify to use agg instead of apply
    elo_cols = [
        "Elo", 
        "Downhill_Elo", "Super G_Elo", "Giant Slalom_Elo", "Slalom_Elo", "Combined_Elo", 
        "Tech_Elo", "Speed_Elo"
    ]
    
    # Create a list of expressions for forward fill
    exprs = [
        pl.col(col).fill_null(strategy="forward").alias(col)
        for col in elo_cols if col in df.columns
    ]
    
    # Add all other columns to preserve them
    other_cols = [col for col in df.columns if col not in elo_cols]
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
    
    # List of all Elo and Pelo column pairs
    elo_pelo_pairs = [
        ("Elo", "Pelo"),
        ("Downhill_Elo", "Downhill_Pelo"),
        ("Super G_Elo", "Super G_Pelo"),
        ("Giant Slalom_Elo", "Giant Slalom_Pelo"),
        ("Slalom_Elo", "Slalom_Pelo"),
        ("Combined_Elo", "Combined_Pelo"),
        ("Tech_Elo", "Tech_Pelo"),
        ("Speed_Elo", "Speed_Pelo")
    ]
    
    # For each Elo-Pelo pair, apply the rules
    for elo_col, pelo_col in elo_pelo_pairs:
        # Only proceed if both columns exist in the DataFrame
        if elo_col in df.columns and pelo_col in df.columns:
            # Apply the rules based on whether it's max season or not
            df = df.with_columns([
                pl.when((pl.col("Event") == "Offseason") & (pl.col("Season") == max_season))
                .then(pl.col(pelo_col))  # For max season, use Pelo value
                .when(pl.col("Event") == "Offseason")
                .then(pl.col(pelo_col) * 0.85 + 1300 * 0.15)  # For other seasons, apply discount
                .otherwise(pl.col(elo_col))
                .alias(elo_col)
            ])
    
    return df

def ladies():
    """Process and combine all ladies' ELO data"""
    # Read all ladies files with consistent path
    base_path = '~/ski/elo/python/alpine/polars/excel365'
    
    try:
        L = pl.read_csv(f'{base_path}/L.csv')
        print("Successfully read L.csv")
    except Exception as e:
        print(f"Error reading L.csv: {e}")
        L = None
        
    try:
        L_Downhill = pl.read_csv(f'{base_path}/L_Downhill.csv')
        L_Downhill = L_Downhill.rename({"Pelo": "Downhill_Pelo", "Elo": "Downhill_Elo"})
        print("Successfully read L_Downhill.csv")
    except Exception as e:
        print(f"Error reading L_Downhill.csv: {e}")
        L_Downhill = None
        
    try:
        L_SuperG = pl.read_csv(f'{base_path}/L_SuperG.csv')
        L_SuperG = L_SuperG.rename({'Pelo': 'Super G_Pelo', 'Elo': 'Super G_Elo'})
        print("Successfully read L_SuperG.csv")
    except Exception as e:
        print(f"Error reading L_SuperG.csv: {e}")
        L_SuperG = None
        
    try:
        L_GS = pl.read_csv(f'{base_path}/L_GS.csv')
        L_GS = L_GS.rename({'Pelo': 'Giant Slalom_Pelo', 'Elo': 'Giant Slalom_Elo'})
        print("Successfully read L_GS.csv")
    except Exception as e:
        print(f"Error reading L_GS.csv: {e}")
        L_GS = None
        
    try:
        L_SL = pl.read_csv(f'{base_path}/L_SL.csv')
        L_SL = L_SL.rename({'Pelo': 'Slalom_Pelo', 'Elo': 'Slalom_Elo'})
        print("Successfully read L_SL.csv")
    except Exception as e:
        print(f"Error reading L_SL.csv: {e}")
        L_SL = None
        
    try:
        L_Combined = pl.read_csv(f'{base_path}/L_Combined.csv')
        L_Combined = L_Combined.rename({'Pelo': 'Combined_Pelo', 'Elo': 'Combined_Elo'})
        print("Successfully read L_Combined.csv")
    except Exception as e:
        print(f"Error reading L_Combined.csv: {e}")
        L_Combined = None
        
    try:
        L_Tech = pl.read_csv(f'{base_path}/L_Tech.csv')
        L_Tech = L_Tech.rename({'Pelo': 'Tech_Pelo', 'Elo': 'Tech_Elo'})
        print("Successfully read L_Tech.csv")
    except Exception as e:
        print(f"Error reading L_Tech.csv: {e}")
        L_Tech = None
        
    try:
        L_Speed = pl.read_csv(f'{base_path}/L_Speed.csv')
        L_Speed = L_Speed.rename({'Pelo': 'Speed_Pelo', 'Elo': 'Speed_Elo'})
        print("Successfully read L_Speed.csv")
    except Exception as e:
        print(f"Error reading L_Speed.csv: {e}")
        L_Speed = None
    
    print("Done reading ladies files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS', 
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    # Filter out None values (files that failed to load)
    dfs = [df for df in [L, L_Downhill, L_SuperG, L_GS, L_SL, L_Combined, L_Tech, L_Speed] if df is not None]
    
    if not dfs:
        print("No data frames loaded for ladies, cannot continue")
        return None
    
    # Handle null techniques
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        try:
            # Use only common columns for joining to avoid duplicates
            join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
            merged_df = merged_df.join(df, on=join_columns, how="left")
            print(f"Successfully joined dataframe with shape {df.shape}")
        except Exception as e:
            print(f"Error joining dataframe: {e}")
            continue

    # Fill nulls forward within each ID group
    try:
        merged_df = (
            merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
            .group_by('ID')
            .map_groups(fill_nulls_forward)
        )
        print("Successfully filled nulls forward")
    except Exception as e:
        print(f"Error filling nulls forward: {e}")

    # Fill Pelo values with corresponding Elo values when Pelo is null
    elo_cols = [
        "Elo", "Downhill_Elo", "Super G_Elo", "Giant Slalom_Elo", "Slalom_Elo", "Combined_Elo", 
        "Tech_Elo", "Speed_Elo"
    ]
    pelo_cols = [
        "Pelo", "Downhill_Pelo", "Super G_Pelo", "Giant Slalom_Pelo", "Slalom_Pelo", "Combined_Pelo", 
        "Tech_Pelo", "Speed_Pelo"
    ]

    for a in range(len(elo_cols)):
        if elo_cols[a] in merged_df.columns and pelo_cols[a] in merged_df.columns:
            try:
                merged_df = merged_df.with_columns(
                    pl.when(pl.col(pelo_cols[a]).is_null())
                    .then(pl.col(elo_cols[a]))
                    .otherwise(pl.col(pelo_cols[a]))
                    .alias(pelo_cols[a])
                )
                print(f"Successfully filled nulls in {pelo_cols[a]}")
            except Exception as e:
                print(f"Error filling nulls in {pelo_cols[a]}: {e}")

    # Apply offseason rules
    try:
        merged_df = apply_offseason_rules(merged_df)
        print("Successfully applied offseason rules")
    except Exception as e:
        print(f"Error applying offseason rules: {e}")
    
    # Sort the final DataFrame
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    return merged_df

def men():
    """Process and combine all men's ELO data"""
    # Read all men's files with consistent path
    base_path = '~/ski/elo/python/alpine/polars/excel365'
    
    try:
        M = pl.read_csv(f'{base_path}/M.csv')
        print("Successfully read M.csv")
    except Exception as e:
        print(f"Error reading M.csv: {e}")
        M = None
        
    try:
        M_Downhill = pl.read_csv(f'{base_path}/M_Downhill.csv')
        M_Downhill = M_Downhill.rename({"Pelo": "Downhill_Pelo", "Elo": "Downhill_Elo"})
        print("Successfully read M_Downhill.csv")
    except Exception as e:
        print(f"Error reading M_Downhill.csv: {e}")
        M_Downhill = None
        
    try:
        M_SuperG = pl.read_csv(f'{base_path}/M_SuperG.csv')
        M_SuperG = M_SuperG.rename({'Pelo': 'Super G_Pelo', 'Elo': 'Super G_Elo'})
        print("Successfully read M_SuperG.csv")
    except Exception as e:
        print(f"Error reading M_SuperG.csv: {e}")
        M_SuperG = None
        
    try:
        M_GS = pl.read_csv(f'{base_path}/M_GS.csv')
        M_GS = M_GS.rename({'Pelo': 'Giant Slalom_Pelo', 'Elo': 'Giant Slalom_Elo'})
        print("Successfully read M_GS.csv")
    except Exception as e:
        print(f"Error reading M_GS.csv: {e}")
        M_GS = None
        
    try:
        M_SL = pl.read_csv(f'{base_path}/M_SL.csv')
        M_SL = M_SL.rename({'Pelo': 'Slalom_Pelo', 'Elo': 'Slalom_Elo'})
        print("Successfully read M_SL.csv")
    except Exception as e:
        print(f"Error reading M_SL.csv: {e}")
        M_SL = None
        
    try:
        M_Combined = pl.read_csv(f'{base_path}/M_Combined.csv')
        M_Combined = M_Combined.rename({'Pelo': 'Combined_Pelo', 'Elo': 'Combined_Elo'})
        print("Successfully read M_Combined.csv")
    except Exception as e:
        print(f"Error reading M_Combined.csv: {e}")
        M_Combined = None
        
    try:
        M_Tech = pl.read_csv(f'{base_path}/M_Tech.csv')
        M_Tech = M_Tech.rename({'Pelo': 'Tech_Pelo', 'Elo': 'Tech_Elo'})
        print("Successfully read M_Tech.csv")
    except Exception as e:
        print(f"Error reading M_Tech.csv: {e}")
        M_Tech = None
        
    try:
        M_Speed = pl.read_csv(f'{base_path}/M_Speed.csv')
        M_Speed = M_Speed.rename({'Pelo': 'Speed_Pelo', 'Elo': 'Speed_Elo'})
        print("Successfully read M_Speed.csv")
    except Exception as e:
        print(f"Error reading M_Speed.csv: {e}")
        M_Speed = None
    
    print("Done reading men's files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS', 
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    # Filter out None values (files that failed to load)
    dfs = [df for df in [M, M_Downhill, M_SuperG, M_GS, M_SL, M_Combined, M_Tech, M_Speed] if df is not None]
    
    if not dfs:
        print("No data frames loaded for men, cannot continue")
        return None
    
    # Handle null techniques
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        try:
            # Use only common columns for joining to avoid duplicates
            join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
            merged_df = merged_df.join(df, on=join_columns, how="left")
            print(f"Successfully joined dataframe with shape {df.shape}")
        except Exception as e:
            print(f"Error joining dataframe: {e}")
            continue

    # Fill nulls forward within each ID group
    try:
        merged_df = (
            merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
            .group_by('ID')
            .map_groups(fill_nulls_forward)
        )
        print("Successfully filled nulls forward")
    except Exception as e:
        print(f"Error filling nulls forward: {e}")

    # Fill Pelo values with corresponding Elo values when Pelo is null
    elo_cols = [
        "Elo", "Downhill_Elo", "Super G_Elo", "Giant Slalom_Elo", "Slalom_Elo", "Combined_Elo", 
        "Tech_Elo", "Speed_Elo"
    ]
    pelo_cols = [
        "Pelo", "Downhill_Pelo", "Super G_Pelo", "Giant Slalom_Pelo", "Slalom_Pelo", "Combined_Pelo", 
        "Tech_Pelo", "Speed_Pelo"
    ]

    for a in range(len(elo_cols)):
        if elo_cols[a] in merged_df.columns and pelo_cols[a] in merged_df.columns:
            try:
                merged_df = merged_df.with_columns(
                    pl.when(pl.col(pelo_cols[a]).is_null())
                    .then(pl.col(elo_cols[a]))
                    .otherwise(pl.col(pelo_cols[a]))
                    .alias(pelo_cols[a])
                )
                print(f"Successfully filled nulls in {pelo_cols[a]}")
            except Exception as e:
                print(f"Error filling nulls in {pelo_cols[a]}: {e}")

    # Apply offseason rules
    try:
        merged_df = apply_offseason_rules(merged_df)
        print("Successfully applied offseason rules")
    except Exception as e:
        print(f"Error applying offseason rules: {e}")
    
    # Sort the final DataFrame
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    return merged_df

# Main execution
if __name__ == "__main__":
    print("Processing ladies data...")
    ladiesdf = ladies()
    
    print("Processing men's data...")
    mendf = men()

    # Print unique nations for verification if dataframes loaded successfully
    if ladiesdf is not None:
        pl.Config.set_tbl_rows(100)
        ladies_nation = ladiesdf.select("Nation").unique().sort(["Nation"])
        print("Ladies nations:")
        print(ladies_nation)
        
        # Save the ladies chrono CSV file
        ladiesdf.write_csv("~/ski/elo/python/alpine/polars/excel365/ladies_chrono.csv")
        print("Saved ladies chrono CSV file")

    if mendf is not None:
        men_nation = mendf.select("Nation").unique().sort(["Nation"])
        print("Men's nations:")
        print(men_nation)
        
        # Save the men's chrono CSV file
        mendf.write_csv("~/ski/elo/python/alpine/polars/excel365/men_chrono.csv")
        print("Saved men's chrono CSV file")

    print(f"Total execution time: {time.time() - start_time:.2f} seconds")