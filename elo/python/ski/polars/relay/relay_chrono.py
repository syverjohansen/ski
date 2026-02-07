import polars as pl
import time
import os
from pathlib import Path

pl.Config.set_tbl_cols(100)
start_time = time.time()

# Define paths
base_path = Path('~/ski/elo/python/ski/polars/relay/excel365').expanduser()
output_path = Path('~/ski/elo/python/ski/polars/relay/excel365').expanduser()

# Create directories if they don't exist
os.makedirs(base_path, exist_ok=True)
os.makedirs(output_path, exist_ok=True)

def load_chrono_files():
    """Load the chronological files for men and ladies"""
    print("Loading chronological files...")
    
    try:
        # Define schema for cross-country relay
        schema_overrides = {
            "Distance": pl.String,  # Mixed values like "7.5", "10", etc.
            "Event": pl.String,
            "MS": pl.String,  # May have mixed values
            "Technique": pl.String,
            "ID": pl.String,  # Keep as string for now to avoid issues
            "Age": pl.String,
            "Elo": pl.Float64,
            "Pelo": pl.Float64,
            "Distance_Elo": pl.Float64,
            "Distance_Pelo": pl.Float64,
            "Distance_C_Elo": pl.Float64,
            "Distance_C_Pelo": pl.Float64,
            "Distance_F_Elo": pl.Float64,
            "Distance_F_Pelo": pl.Float64,
            "Sprint_Elo": pl.Float64,
            "Sprint_Pelo": pl.Float64,
            "Sprint_C_Elo": pl.Float64,
            "Sprint_C_Pelo": pl.Float64,
            "Sprint_F_Elo": pl.Float64,
            "Sprint_F_Pelo": pl.Float64,
            "Classic_Elo": pl.Float64,
            "Classic_Pelo": pl.Float64,
            "Freestyle_Elo": pl.Float64,
            "Freestyle_Pelo": pl.Float64
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
    """Get all Elo and Pelo columns from the dataframe (cross-country style)"""
    elo_cols = [col for col in df.columns if 
                (col.endswith('_Elo') or col == 'Elo')]
    pelo_cols = [col for col in df.columns if 
                 (col.endswith('_Pelo') or col == 'Pelo')]
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

def process_mixed_relay(combined_df):
    """Process Mixed Relay data"""
    print("Processing Mixed Relay data...")
    
    # Filter for Mixed Relay races - cross-country typically doesn't have this but including for completeness
    mixed_relay_df = combined_df.filter(pl.col("Event").str.contains("Mixed") & pl.col("Event").str.contains("Relay"))
    print(f"Found {mixed_relay_df.shape[0]} rows for Mixed Relay")
    
    if mixed_relay_df.shape[0] == 0:
        print("No Mixed Relay data found")
        return None
    
    # Get the list of Elo and Pelo columns
    elo_cols, pelo_cols = get_elo_pelo_columns(mixed_relay_df)
    print(f"Using Elo columns: {elo_cols}")
    print(f"Using Pelo columns: {pelo_cols}")
    
    # Calculate quartile values for imputation
    quartile_values = calculate_quartile_imputation(mixed_relay_df, elo_cols, pelo_cols)
    print(f"Quartile imputation values: {quartile_values}")
    
    # Fill missing values with quartile imputation before aggregation
    mixed_relay_imputed = mixed_relay_df
    for col in elo_cols + pelo_cols:
        if col in mixed_relay_df.columns and col in quartile_values:
            mixed_relay_imputed = mixed_relay_imputed.with_columns(
                pl.col(col).fill_null(quartile_values[col])
            )
    
    # Group by team (Nation, Season, Race, Place)
    team_df = (mixed_relay_imputed
        .group_by(["Nation", "Season", "Race", "Place", "Date", "City", "Country", "Event"])
        .agg([
            # Count team members
            pl.len().alias("TeamSize"),
            # Take the first value for general race info
            pl.first("Distance").alias("Distance"),
            pl.first("MS").alias("MS"),
            pl.first("Technique").alias("Technique"),
            
            # Sum Elo/Pelo values
            *[pl.sum(col).alias(f"Team_{col}") for col in elo_cols if col in mixed_relay_df.columns],
            *[pl.sum(col).alias(f"Team_{col}") for col in pelo_cols if col in mixed_relay_df.columns],
            
            # Calculate average Elo/Pelo values (needed for R predictions)
            *[pl.mean(col).alias(f"Avg_{col}") for col in elo_cols if col in mixed_relay_df.columns],
            *[pl.mean(col).alias(f"Avg_{col}") for col in pelo_cols if col in mixed_relay_df.columns],
            
            # List team members
            pl.col("Skier").alias("TeamMembers"),
            pl.col("Sex").alias("MemberSexes"),
            pl.col("ID").alias("MemberIDs"),
            
            # Calculate team age and experience metrics
            pl.mean("Age").alias("AvgAge"),
            pl.max("Age").alias("MaxAge"),
            pl.min("Age").alias("MinAge"),
            pl.mean("Exp").alias("AvgExp"),
            pl.max("Exp").alias("MaxExp"),
            pl.min("Exp").alias("MinExp"),
        ])
        .sort(["Date", "Race", "Place"])
    )
    
    print(f"Created team dataframe with {team_df.shape[0]} rows")
    return team_df

def process_team_sprint(combined_df):
    """Process Team Sprint data"""
    print("Processing Team Sprint data...")
    
    # Filter for Team Sprint races using Distance column
    team_sprint_df = combined_df.filter(pl.col("Distance").str.contains("Ts"))
    print(f"Found {team_sprint_df.shape[0]} rows for Team Sprint")
    
    if team_sprint_df.shape[0] == 0:
        print("No Team Sprint data found")
        return None
    
    # Get the list of Elo and Pelo columns
    elo_cols, pelo_cols = get_elo_pelo_columns(team_sprint_df)
    print(f"Using Elo columns: {elo_cols}")
    print(f"Using Pelo columns: {pelo_cols}")
    
    # Calculate quartile values for imputation
    quartile_values = calculate_quartile_imputation(team_sprint_df, elo_cols, pelo_cols)
    print(f"Quartile imputation values: {quartile_values}")
    
    # Fill missing values with quartile imputation before aggregation
    team_sprint_imputed = team_sprint_df
    for col in elo_cols + pelo_cols:
        if col in team_sprint_df.columns and col in quartile_values:
            team_sprint_imputed = team_sprint_imputed.with_columns(
                pl.col(col).fill_null(quartile_values[col])
            )
    
    # Group by team (Nation, Season, Race, Place)
    team_df = (team_sprint_imputed
        .group_by(["Nation", "Season", "Race", "Place", "Date", "City", "Country", "Event"])
        .agg([
            # Count team members
            pl.len().alias("TeamSize"),
            # Take the first value for general race info
            pl.first("Distance").alias("Distance"),
            pl.first("MS").alias("MS"),
            pl.first("Technique").alias("Technique"),
            pl.first("Sex").alias("Sex"),
            
            # Sum Elo/Pelo values
            *[pl.sum(col).alias(f"Team_{col}") for col in elo_cols if col in team_sprint_df.columns],
            *[pl.sum(col).alias(f"Team_{col}") for col in pelo_cols if col in team_sprint_df.columns],
            
            # Calculate average Elo/Pelo values (needed for R predictions)
            *[pl.mean(col).alias(f"Avg_{col}") for col in elo_cols if col in team_sprint_df.columns],
            *[pl.mean(col).alias(f"Avg_{col}") for col in pelo_cols if col in team_sprint_df.columns],
            
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
        ])
        .sort(["Date", "Race", "Place"])
    )
    
    print(f"Created team dataframe with {team_df.shape[0]} rows")
    return team_df

def process_relay(df, sex):
    """Process regular Relay data for a given sex"""
    print(f"Processing {sex} Relay data...")
    
    # Filter for Relay races using Distance column
    relay_df = df.filter(pl.col("Distance").str.contains("Rel"))
    print(f"Found {relay_df.shape[0]} rows for {sex} Relay")
    
    if relay_df.shape[0] == 0:
        print(f"No {sex} Relay data found")
        return None
    
    # Get the list of Elo and Pelo columns
    elo_cols, pelo_cols = get_elo_pelo_columns(relay_df)
    print(f"Using Elo columns: {elo_cols}")
    print(f"Using Pelo columns: {pelo_cols}")
    
    # Calculate quartile values for imputation
    quartile_values = calculate_quartile_imputation(relay_df, elo_cols, pelo_cols)
    print(f"Quartile imputation values: {quartile_values}")
    
    # Fill missing values with quartile imputation before aggregation
    relay_df_imputed = relay_df
    for col in elo_cols + pelo_cols:
        if col in relay_df.columns and col in quartile_values:
            relay_df_imputed = relay_df_imputed.with_columns(
                pl.col(col).fill_null(quartile_values[col])
            )
    
    # Group by team (Nation, Season, Race, Place)
    team_df = (relay_df_imputed
        .group_by(["Nation", "Season", "Race", "Place", "Date", "City", "Country", "Event"])
        .agg([
            # Count team members
            pl.len().alias("TeamSize"),
            # Take the first value for general race info
            pl.first("Distance").alias("Distance"),
            pl.first("MS").alias("MS"),
            pl.first("Technique").alias("Technique"),
            pl.first("Sex").alias("Sex"),
            
            # Sum Elo/Pelo values
            *[pl.sum(col).alias(f"Team_{col}") for col in elo_cols if col in relay_df.columns],
            *[pl.sum(col).alias(f"Team_{col}") for col in pelo_cols if col in relay_df.columns],
            
            # Calculate average Elo/Pelo values (needed for R predictions)
            *[pl.mean(col).alias(f"Avg_{col}") for col in elo_cols if col in relay_df.columns],
            *[pl.mean(col).alias(f"Avg_{col}") for col in pelo_cols if col in relay_df.columns],
            
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
        ])
        .sort(["Date", "Race", "Place"])
    )
    
    print(f"Created {sex} team dataframe with {team_df.shape[0]} rows")
    return team_df

def save_dataframe(df, filename):
    """Save dataframe to both feather and CSV formats"""
    if df is None:
        print(f"No data to save for {filename}")
        return
    
    try:
        # For CSV: Convert nested data to string representations
        csv_df = df.clone()
        for col in csv_df.columns:
            # Check if column contains nested data (lists)
            if csv_df[col].dtype == pl.List:
                # Convert lists to comma-separated strings
                csv_df = csv_df.with_columns(
                    pl.col(col).map_elements(lambda x: ','.join(map(str, x)) if x is not None else "").alias(col)
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
    
    # Combine men and ladies data for mixed events
    combined_df = create_combined_df(men_chrono, ladies_chrono)
    
    # Process Mixed Relay (if any exist in cross-country)
    mixed_relay_df = process_mixed_relay(combined_df)
    save_dataframe(mixed_relay_df, "mixed_relay_chrono")
    
    # Process Team Sprint
    team_sprint_df = process_team_sprint(combined_df)
    save_dataframe(team_sprint_df, "team_sprint_chrono")
    
    # Process Men's Relay
    men_relay_df = process_relay(men_chrono, "Men's")
    save_dataframe(men_relay_df, "men_relay_chrono")
    
    # Process Ladies' Relay
    ladies_relay_df = process_relay(ladies_chrono, "Ladies'")
    save_dataframe(ladies_relay_df, "ladies_relay_chrono")
    
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()