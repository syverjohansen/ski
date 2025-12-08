import polars as pl
import time
import os
from pathlib import Path

pl.Config.set_tbl_cols(100)
start_time = time.time()

# Define paths
base_path = Path('~/ski/elo/python/skijump/polars/relay/excel365').expanduser()
output_path = Path('~/ski/elo/python/skijump/polars/relay/excel365').expanduser()

# Create directories if they don't exist
os.makedirs(base_path, exist_ok=True)
os.makedirs(output_path, exist_ok=True)

def load_chrono_files():
    """Load the chronological files for men and ladies"""
    print("Loading chronological files...")
    
    try:
        # Define schema for skijump relay
        schema_overrides = {
            "HillSize": pl.String,
            "TeamEvent": pl.String,
            "Event": pl.String,
            "Leg": pl.String,
            "Length1": pl.Float64,
            "Length2": pl.Float64,
            "Points": pl.Float64,
            "Elo": pl.Float64,
            "Pelo": pl.Float64,
            "Small_Elo": pl.Float64,
            "Small_Pelo": pl.Float64,
            "Medium_Elo": pl.Float64,
            "Medium_Pelo": pl.Float64,
            "Normal_Elo": pl.Float64,
            "Normal_Pelo": pl.Float64,
            "Large_Elo": pl.Float64,
            "Large_Pelo": pl.Float64,
            "Flying_Elo": pl.Float64,
            "Flying_Pelo": pl.Float64
        }
        
        men_chrono = pl.read_csv(base_path / "men_chrono.csv", schema_overrides=schema_overrides)
        ladies_chrono = pl.read_csv(base_path / "ladies_chrono.csv", schema_overrides=schema_overrides)
        print(f"Loaded men's chrono with {men_chrono.shape[0]} rows")
        print(f"Loaded ladies' chrono with {ladies_chrono.shape[0]} rows")
        return men_chrono, ladies_chrono
    except Exception as e:
        print(f"Error loading chrono files: {e}")
        return None, None

def create_combined_df(men_df, ladies_df):
    """Combine men's and ladies' dataframes"""
    print("Combining men's and ladies' data...")
    
    # Make sure all columns have the same data types before concatenating
    men_schema = men_df.schema
    ladies_schema = ladies_df.schema
    
    # Get all columns from both dataframes
    all_columns = set(men_schema.keys()).union(set(ladies_schema.keys()))
    
    # Ensure both dataframes have all columns with the same types
    for col in all_columns:
        if col in men_schema and col in ladies_schema:
            # If column exists in both, ensure the same type
            men_type = men_schema[col]
            ladies_type = ladies_schema[col]
            
            if men_type != ladies_type:
                print(f"Column {col} has different types: {men_type} vs {ladies_type}, converting...")
                # Choose the more general type (usually string)
                if pl.Utf8 in (men_type, ladies_type):
                    men_df = men_df.with_columns(pl.col(col).cast(pl.Utf8))
                    ladies_df = ladies_df.with_columns(pl.col(col).cast(pl.Utf8))
                else:
                    # Default to float for numeric differences
                    men_df = men_df.with_columns(pl.col(col).cast(pl.Float64))
                    ladies_df = ladies_df.with_columns(pl.col(col).cast(pl.Float64))
        elif col in men_schema:
            # Add missing column to ladies with null values
            ladies_df = ladies_df.with_columns(pl.lit(None).cast(men_schema[col]).alias(col))
        elif col in ladies_schema:
            # Add missing column to men with null values
            men_df = men_df.with_columns(pl.lit(None).cast(ladies_schema[col]).alias(col))
    
    # Concatenate the dataframes
    combined_df = pl.concat([men_df, ladies_df])
    print(f"Combined dataframe has {combined_df.shape[0]} rows")
    
    return combined_df

def get_elo_pelo_columns(df):
    """Get all Elo and Pelo columns from the dataframe, excluding Small and Medium hill types"""
    # Only include Normal, Large, Flying hill types and base Elo/Pelo
    elo_cols = [col for col in df.columns if 
                (col.endswith('_Elo') or col == 'Elo') and 
                not col.startswith('Small_') and not col.startswith('Medium_')]
    pelo_cols = [col for col in df.columns if 
                 (col.endswith('_Pelo') or col == 'Pelo') and 
                 not col.startswith('Small_') and not col.startswith('Medium_')]
    return elo_cols, pelo_cols

def calculate_quartile_imputation(df, elo_cols, pelo_cols):
    """Calculate first quartile values for Elo and Pelo columns for imputation"""
    quartile_values = {}
    
    for col in elo_cols + pelo_cols:
        if col in df.columns:
            # Calculate first quartile, handling nulls
            q1_value = df.select(pl.col(col).quantile(0.25)).to_numpy()[0, 0]
            if q1_value is not None and not pl.Series([q1_value]).is_nan()[0]:
                quartile_values[col] = q1_value
            else:
                # Fallback to a reasonable default if no valid data
                quartile_values[col] = 1200.0 if 'Elo' in col else 1200.0
    
    return quartile_values

def process_mixed_team(combined_df):
    """Process Mixed Team data combining men's and ladies' team events"""
    print("Processing Mixed Team data (combining men's and ladies' teams)...")
    
    # Filter for Team races from combined data
    team_df = combined_df.filter(pl.col("RaceType").str.contains("Team"))
    print(f"Found {team_df.shape[0]} rows for Mixed Team (from combined teams)")
    
    if team_df.shape[0] == 0:
        print("No Team data found in combined data")
        return None
    
    # Get the list of Elo and Pelo columns (excluding Small and Medium)
    elo_cols, pelo_cols = get_elo_pelo_columns(team_df)
    print(f"Using Elo columns: {elo_cols}")
    print(f"Using Pelo columns: {pelo_cols}")
    
    # Calculate quartile values for imputation
    quartile_values = calculate_quartile_imputation(team_df, elo_cols, pelo_cols)
    print(f"Quartile imputation values: {quartile_values}")
    
    # Fill missing values with quartile imputation before aggregation
    team_df_imputed = team_df
    for col in elo_cols + pelo_cols:
        if col in team_df.columns and col in quartile_values:
            team_df_imputed = team_df_imputed.with_columns(
                pl.col(col).fill_null(quartile_values[col])
            )
    
    # Group by team (Nation, Season, Race, Place)
    team_df_processed = (team_df_imputed
        .group_by(["Nation", "Season", "Race", "Place", "Date", "City", "Country", "Event", "HillSize"])
        .agg([
            # Count team members
            pl.len().alias("TeamSize"),
            # Take the first value for general race info
            pl.first("RaceType").alias("RaceType"),
            pl.first("Sex").alias("Sex"),  # This will be mixed for mixed teams
            
            # Sum Elo/Pelo values for all hill sizes (Total columns)
            *[pl.sum(col).alias(f"Total_{col}") for col in elo_cols if col in team_df.columns],
            *[pl.sum(col).alias(f"Total_{col}") for col in pelo_cols if col in team_df.columns],
            
            # Calculate average Elo/Pelo values (Avg columns)
            *[pl.mean(col).alias(f"Avg_{col}") for col in elo_cols if col in team_df.columns],
            *[pl.mean(col).alias(f"Avg_{col}") for col in pelo_cols if col in team_df.columns],
            
            # List team members
            pl.col("Skier").alias("TeamMembers"),
            pl.col("ID").alias("MemberIDs"),
            pl.col("Sex").alias("MemberSex"),  # Track sex of each member
            
            # Calculate team age and experience metrics
            pl.mean("Age").alias("AvgAge"),
            pl.max("Age").alias("MaxAge"),
            pl.min("Age").alias("MinAge"),
            pl.mean("Exp").alias("AvgExp"),
            pl.max("Exp").alias("MaxExp"),
            pl.min("Exp").alias("MinExp"),
            
            # Note: Length1, Length2, Points columns removed from processing
        ])
        .sort(["Date", "Race", "Place"])
    )
    
    print(f"Created mixed team dataframe with {team_df_processed.shape[0]} rows")
    return team_df_processed

def process_team(df, sex):
    """Process regular Team data for a given sex"""
    print(f"Processing {sex} Team data...")
    
    # Filter for Team races
    team_df = df.filter(pl.col("RaceType").str.contains("Team"))
    print(f"Found {team_df.shape[0]} rows for {sex} Team")
    
    if team_df.shape[0] == 0:
        print(f"No {sex} Team data found")
        return None
    
    # Get the list of Elo and Pelo columns (excluding Small and Medium)
    elo_cols, pelo_cols = get_elo_pelo_columns(team_df)
    print(f"Using Elo columns: {elo_cols}")
    print(f"Using Pelo columns: {pelo_cols}")
    
    # Calculate quartile values for imputation
    quartile_values = calculate_quartile_imputation(team_df, elo_cols, pelo_cols)
    print(f"Quartile imputation values: {quartile_values}")
    
    # Fill missing values with quartile imputation before aggregation
    team_df_imputed = team_df
    for col in elo_cols + pelo_cols:
        if col in team_df.columns and col in quartile_values:
            team_df_imputed = team_df_imputed.with_columns(
                pl.col(col).fill_null(quartile_values[col])
            )
    
    # Group by team (Nation, Season, Race, Place)
    team_df_processed = (team_df_imputed
        .group_by(["Nation", "Season", "Race", "Place", "Date", "City", "Country", "Event", "HillSize"])
        .agg([
            # Count team members
            pl.len().alias("TeamSize"),
            # Take the first value for general race info
            pl.first("RaceType").alias("RaceType"),
            pl.first("Sex").alias("Sex"),
            
            # Sum Elo/Pelo values for all hill sizes (Total columns)
            *[pl.sum(col).alias(f"Total_{col}") for col in elo_cols if col in team_df.columns],
            *[pl.sum(col).alias(f"Total_{col}") for col in pelo_cols if col in team_df.columns],
            
            # Calculate average Elo/Pelo values (Avg columns)
            *[pl.mean(col).alias(f"Avg_{col}") for col in elo_cols if col in team_df.columns],
            *[pl.mean(col).alias(f"Avg_{col}") for col in pelo_cols if col in team_df.columns],
            
            # List team members
            pl.col("Skier").alias("TeamMembers"),
            pl.col("ID").alias("MemberIDs"),
            
            # Calculate team age and experience metrics
            pl.mean("Age").alias("AvgAge"),
            pl.max("Age").alias("MaxAge"),
            pl.min("Age").alias("MinAge"),
            pl.mean("Exp").alias("AvgExp"),
            pl.max("Exp").alias("MaxExp"),
            pl.min("Exp").alias("MinExp"),
            
            # Note: Length1, Length2, Points columns removed from processing
        ])
        .sort(["Date", "Race", "Place"])
    )
    
    print(f"Created {sex} team dataframe with {team_df_processed.shape[0]} rows")
    return team_df_processed

def save_dataframe(df, filename):
    """Save dataframe to CSV format"""
    if df is None:
        print(f"No data to save for {filename}")
        return
    
    # Drop Length1, Length2, Points columns if they exist
    columns_to_drop = ['Length1', 'Length2', 'Points']
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(existing_columns_to_drop)
        print(f"Dropped columns from {filename}: {existing_columns_to_drop}")
    
    try:
        # For CSV: Convert nested data to string representations
        csv_df = df.clone()
        for col in csv_df.columns:
            # Check if column contains nested data (lists)
            if csv_df[col].dtype == pl.List:
                # Convert lists to comma-separated strings
                csv_df = csv_df.with_columns(
                    pl.col(col).map_elements(lambda x: ','.join(map(str, x)) if x is not None else "", return_dtype=pl.Utf8).alias(col)
                )
        
        # Save to CSV
        csv_df.write_csv(output_path / f"{filename}.csv")
        print(f"Saved {filename}.csv with {csv_df.shape[0]} rows")
    
    except Exception as e:
        print(f"Error saving {filename}: {e}")

def main():
    # Load chronological files
    men_chrono, ladies_chrono = load_chrono_files()
    
    if men_chrono is None or ladies_chrono is None:
        print("Error: Could not load chronological files")
        return
    
    # Combine men and ladies data for mixed team events
    combined_df = create_combined_df(men_chrono, ladies_chrono)
    
    # Process Mixed Team events (combining men's and ladies' data)
    mixed_team_df = process_mixed_team(combined_df)
    save_dataframe(mixed_team_df, "mixed_team_chrono")
    
    # Process Men's Team events
    men_team_df = process_team(men_chrono, "Men's")
    save_dataframe(men_team_df, "men_team_chrono")
    
    # Process Ladies' Team events
    ladies_team_df = process_team(ladies_chrono, "Ladies'")
    save_dataframe(ladies_team_df, "ladies_team_chrono")
    
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()