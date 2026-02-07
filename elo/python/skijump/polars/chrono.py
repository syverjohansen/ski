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
        "Small_Elo", "Medium_Elo", "Normal_Elo", "Large_Elo", "Flying_Elo"
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
        ("Small_Elo", "Small_Pelo"),
        ("Medium_Elo", "Medium_Pelo"),
        ("Normal_Elo", "Normal_Pelo"),
        ("Large_Elo", "Large_Pelo"),
        ("Flying_Elo", "Flying_Pelo")
    ]
    
    # For each Elo-Pelo pair, apply the rules
    for elo_col, pelo_col in elo_pelo_pairs:
        # Only proceed if both columns exist in the DataFrame
        if elo_col in df.columns and pelo_col in df.columns:
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
    """Process and combine all ladies' ELO data"""
    # Read all ladies files with consistent path
    base_path = '~/ski/elo/python/skijump/polars/excel365'
    
    # Define consistent schema based on elo.py expectations
    schema_overrides = {
        "Date": pl.String,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "HillSize": pl.String,
        "RaceType": pl.String,
        "TeamEvent": pl.Int64,
        "Event": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.String,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.String,  # Will be cast to Datetime later
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Length1": pl.Float64,
        "Length2": pl.Float64,
        "Points": pl.Float64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64
    }
    
    try:
        L = pl.read_csv(f'{base_path}/L.csv', schema_overrides=schema_overrides)
        print("Successfully read L.csv")
    except Exception as e:
        print(f"Error reading L.csv: {e}")
        L = None
        
    try:
        L_Small = pl.read_csv(f'{base_path}/L_Small.csv', schema_overrides=schema_overrides)
        L_Small = L_Small.rename({"Pelo": "Small_Pelo", "Elo": "Small_Elo"})
        print("Successfully read L_Small.csv")
    except Exception as e:
        print(f"Error reading L_Small.csv: {e}")
        L_Small = None
        
    try:
        L_Medium = pl.read_csv(f'{base_path}/L_Medium.csv', schema_overrides=schema_overrides)
        L_Medium = L_Medium.rename({'Pelo': 'Medium_Pelo', 'Elo': 'Medium_Elo'})
        print("Successfully read L_Medium.csv")
    except Exception as e:
        print(f"Error reading L_Medium.csv: {e}")
        L_Medium = None
        
    try:
        L_Normal = pl.read_csv(f'{base_path}/L_Normal.csv', schema_overrides=schema_overrides)
        L_Normal = L_Normal.rename({'Pelo': 'Normal_Pelo', 'Elo': 'Normal_Elo'})
        print("Successfully read L_Normal.csv")
    except Exception as e:
        print(f"Error reading L_Normal.csv: {e}")
        L_Normal = None
        
    try:
        L_Large = pl.read_csv(f'{base_path}/L_Large.csv', schema_overrides=schema_overrides)
        L_Large = L_Large.rename({'Pelo': 'Large_Pelo', 'Elo': 'Large_Elo'})
        print("Successfully read L_Large.csv")
    except Exception as e:
        print(f"Error reading L_Large.csv: {e}")
        L_Large = None
        
    try:
        L_Flying = pl.read_csv(f'{base_path}/L_Flying.csv', schema_overrides=schema_overrides)
        L_Flying = L_Flying.rename({'Pelo': 'Flying_Pelo', 'Elo': 'Flying_Elo'})
        print("Successfully read L_Flying.csv")
    except Exception as e:
        print(f"Error reading L_Flying.csv: {e}")
        L_Flying = None
    
    print("Done reading ladies files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'HillSize', 'RaceType', 'TeamEvent', 
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    # Filter out None values (files that failed to load)
    dfs = [df for df in [L, L_Small, L_Medium, L_Normal, L_Large, L_Flying] if df is not None]
    
    if not dfs:
        print("No data frames loaded for ladies, cannot continue")
        return None
    
    # Handle null techniques if column exists
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes using the alpine approach
    merged_df = dfs[0]
    for df in dfs[1:]:
        try:
            # Use only common columns for joining to avoid duplicates
            join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
            
            # Select only the join columns plus the new ELO columns to avoid duplicates
            elo_cols_in_df = [col for col in df.columns if col.endswith('_Elo') or col.endswith('_Pelo')]
            select_columns = join_columns + elo_cols_in_df
            df_to_join = df.select(select_columns)
            
            merged_df = merged_df.join(df_to_join, on=join_columns, how="left")
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
        "Elo", "Small_Elo", "Medium_Elo", "Normal_Elo", "Large_Elo", "Flying_Elo"
    ]
    pelo_cols = [
        "Pelo", "Small_Pelo", "Medium_Pelo", "Normal_Pelo", "Large_Pelo", "Flying_Pelo"
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
    
    # Sort the final DataFrame and drop unwanted columns
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    
    # Drop Length1, Length2, Points columns if they exist
    columns_to_drop = ['Length1', 'Length2', 'Points']
    existing_columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
    if existing_columns_to_drop:
        merged_df = merged_df.drop(existing_columns_to_drop)
        print(f"Dropped columns: {existing_columns_to_drop}")
    
    return merged_df

def men():
    """Process and combine all men's ELO data"""
    # Read all men's files with consistent path
    base_path = '~/ski/elo/python/skijump/polars/excel365'
    
    # Define consistent schema based on elo.py expectations
    schema_overrides = {
        "Date": pl.String,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "HillSize": pl.String,
        "RaceType": pl.String,
        "TeamEvent": pl.Int64,
        "Event": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.String,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.String,  # Will be cast to Datetime later
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Length1": pl.Float64,
        "Length2": pl.Float64,
        "Points": pl.Float64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64
    }
    
    try:
        M = pl.read_csv(f'{base_path}/M.csv', schema_overrides=schema_overrides)
        print("Successfully read M.csv")
    except Exception as e:
        print(f"Error reading M.csv: {e}")
        M = None
        
    try:
        M_Small = pl.read_csv(f'{base_path}/M_Small.csv', schema_overrides=schema_overrides)
        M_Small = M_Small.rename({"Pelo": "Small_Pelo", "Elo": "Small_Elo"})
        print("Successfully read M_Small.csv")
    except Exception as e:
        print(f"Error reading M_Small.csv: {e}")
        M_Small = None
        
    try:
        M_Medium = pl.read_csv(f'{base_path}/M_Medium.csv', schema_overrides=schema_overrides)
        M_Medium = M_Medium.rename({'Pelo': 'Medium_Pelo', 'Elo': 'Medium_Elo'})
        print("Successfully read M_Medium.csv")
    except Exception as e:
        print(f"Error reading M_Medium.csv: {e}")
        M_Medium = None
        
    try:
        M_Normal = pl.read_csv(f'{base_path}/M_Normal.csv', schema_overrides=schema_overrides)
        M_Normal = M_Normal.rename({'Pelo': 'Normal_Pelo', 'Elo': 'Normal_Elo'})
        print("Successfully read M_Normal.csv")
    except Exception as e:
        print(f"Error reading M_Normal.csv: {e}")
        M_Normal = None
        
    try:
        M_Large = pl.read_csv(f'{base_path}/M_Large.csv', schema_overrides=schema_overrides)
        M_Large = M_Large.rename({'Pelo': 'Large_Pelo', 'Elo': 'Large_Elo'})
        print("Successfully read M_Large.csv")
    except Exception as e:
        print(f"Error reading M_Large.csv: {e}")
        M_Large = None
        
    try:
        M_Flying = pl.read_csv(f'{base_path}/M_Flying.csv', schema_overrides=schema_overrides)
        M_Flying = M_Flying.rename({'Pelo': 'Flying_Pelo', 'Elo': 'Flying_Elo'})
        print("Successfully read M_Flying.csv")
    except Exception as e:
        print(f"Error reading M_Flying.csv: {e}")
        M_Flying = None
    
    print("Done reading men's files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'HillSize', 'RaceType', 'TeamEvent', 
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season', 
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    # Filter out None values (files that failed to load)
    dfs = [df for df in [M, M_Small, M_Medium, M_Normal, M_Large, M_Flying] if df is not None]
    
    if not dfs:
        print("No data frames loaded for men, cannot continue")
        return None
    
    # Handle null techniques if column exists
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes using the alpine approach
    merged_df = dfs[0]
    for df in dfs[1:]:
        try:
            # Use only common columns for joining to avoid duplicates
            join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
            
            # Select only the join columns plus the new ELO columns to avoid duplicates
            elo_cols_in_df = [col for col in df.columns if col.endswith('_Elo') or col.endswith('_Pelo')]
            select_columns = join_columns + elo_cols_in_df
            df_to_join = df.select(select_columns)
            
            merged_df = merged_df.join(df_to_join, on=join_columns, how="left")
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
        "Elo", "Small_Elo", "Medium_Elo", "Normal_Elo", "Large_Elo", "Flying_Elo"
    ]
    pelo_cols = [
        "Pelo", "Small_Pelo", "Medium_Pelo", "Normal_Pelo", "Large_Pelo", "Flying_Pelo"
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
    
    # Sort the final DataFrame and drop unwanted columns
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    
    # Drop Length1, Length2, Points columns if they exist
    columns_to_drop = ['Length1', 'Length2', 'Points']
    existing_columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
    if existing_columns_to_drop:
        merged_df = merged_df.drop(existing_columns_to_drop)
        print(f"Dropped columns: {existing_columns_to_drop}")
    
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
        
        # Save the ladies files
        ladiesdf.write_csv("~/ski/elo/python/skijump/polars/excel365/ladies_chrono.csv")
        print("Saved ladies chrono files")

    if mendf is not None:
        men_nation = mendf.select("Nation").unique().sort(["Nation"])
        print("Men's nations:")
        print(men_nation)
        
        # Save the men's files
        mendf.write_csv("~/ski/elo/python/skijump/polars/excel365/men_chrono.csv")
        print("Saved men's chrono files")

    print(f"Total execution time: {time.time() - start_time:.2f} seconds")