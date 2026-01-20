import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
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
    """Process and combine all ladies' dynamic ELO data"""
    # Read all ladies dyn files with consistent path
    base_path = '~/ski/elo/python/alpine/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.Date,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "Distance": pl.String,
        "Event": pl.String,
        "MS": pl.Int64,
        "Technique": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.Int64,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    L = pl.read_csv(f'{base_path}/dyn_L.csv', schema_overrides=schema_overrides)
    L = L.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    L_Downhill = pl.read_csv(f'{base_path}/dyn_L_Downhill.csv', schema_overrides=schema_overrides)
    L_Downhill = L_Downhill.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Downhill_Pelo', 'pred_Elo': 'Downhill_Elo'})

    L_SuperG = pl.read_csv(f'{base_path}/dyn_L_Super_G.csv', schema_overrides=schema_overrides)
    L_SuperG = L_SuperG.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Super G_Pelo', 'pred_Elo': 'Super G_Elo'})

    L_GS = pl.read_csv(f'{base_path}/dyn_L_Giant_Slalom.csv', schema_overrides=schema_overrides)
    L_GS = L_GS.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Giant Slalom_Pelo', 'pred_Elo': 'Giant Slalom_Elo'})

    L_SL = pl.read_csv(f'{base_path}/dyn_L_Slalom.csv', schema_overrides=schema_overrides)
    L_SL = L_SL.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Slalom_Pelo', 'pred_Elo': 'Slalom_Elo'})

    L_Combined = pl.read_csv(f'{base_path}/dyn_L_Combined.csv', schema_overrides=schema_overrides)
    L_Combined = L_Combined.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Combined_Pelo', 'pred_Elo': 'Combined_Elo'})

    L_Tech = pl.read_csv(f'{base_path}/dyn_L_Tech.csv', schema_overrides=schema_overrides)
    L_Tech = L_Tech.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Tech_Pelo', 'pred_Elo': 'Tech_Elo'})

    L_Speed = pl.read_csv(f'{base_path}/dyn_L_Speed.csv', schema_overrides=schema_overrides)
    L_Speed = L_Speed.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Speed_Pelo', 'pred_Elo': 'Speed_Elo'})

    print("Done reading ladies dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS',
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    dfs = [L, L_Downhill, L_SuperG, L_GS, L_SL, L_Combined, L_Tech, L_Speed]

    # Handle null techniques
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        # Use only common columns for joining to avoid duplicates
        join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
        merged_df = merged_df.join(df, on=join_columns, how="left")

    # Fill nulls forward within each ID group
    merged_df = (
        merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
        .group_by('ID')
        .map_groups(fill_nulls_forward)
    )

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
            merged_df = merged_df.with_columns(
                pl.when(pl.col(pelo_cols[a]).is_null())
                .then(pl.col(elo_cols[a]))
                .otherwise(pl.col(pelo_cols[a]))
                .alias(pelo_cols[a])
            )

    # Apply offseason rules
    merged_df = apply_offseason_rules(merged_df)

    # Sort the final DataFrame
    merged_df = merged_df.sort(['Date', 'Race', 'Place'])
    return merged_df

def men():
    """Process and combine all men's dynamic ELO data"""
    # Read all men's dyn files with consistent path
    base_path = '~/ski/elo/python/alpine/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.Date,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "Distance": pl.String,
        "Event": pl.String,
        "MS": pl.Int64,
        "Technique": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.Int64,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    M = pl.read_csv(f'{base_path}/dyn_M.csv', schema_overrides=schema_overrides)
    M = M.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    M_Downhill = pl.read_csv(f'{base_path}/dyn_M_Downhill.csv', schema_overrides=schema_overrides)
    M_Downhill = M_Downhill.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Downhill_Pelo', 'pred_Elo': 'Downhill_Elo'})

    M_SuperG = pl.read_csv(f'{base_path}/dyn_M_Super_G.csv', schema_overrides=schema_overrides)
    M_SuperG = M_SuperG.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Super G_Pelo', 'pred_Elo': 'Super G_Elo'})

    M_GS = pl.read_csv(f'{base_path}/dyn_M_Giant_Slalom.csv', schema_overrides=schema_overrides)
    M_GS = M_GS.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Giant Slalom_Pelo', 'pred_Elo': 'Giant Slalom_Elo'})

    M_SL = pl.read_csv(f'{base_path}/dyn_M_Slalom.csv', schema_overrides=schema_overrides)
    M_SL = M_SL.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Slalom_Pelo', 'pred_Elo': 'Slalom_Elo'})

    M_Combined = pl.read_csv(f'{base_path}/dyn_M_Combined.csv', schema_overrides=schema_overrides)
    M_Combined = M_Combined.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Combined_Pelo', 'pred_Elo': 'Combined_Elo'})

    M_Tech = pl.read_csv(f'{base_path}/dyn_M_Tech.csv', schema_overrides=schema_overrides)
    M_Tech = M_Tech.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Tech_Pelo', 'pred_Elo': 'Tech_Elo'})

    M_Speed = pl.read_csv(f'{base_path}/dyn_M_Speed.csv', schema_overrides=schema_overrides)
    M_Speed = M_Speed.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Speed_Pelo', 'pred_Elo': 'Speed_Elo'})

    print("Done reading men's dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS',
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    dfs = [M, M_Downhill, M_SuperG, M_GS, M_SL, M_Combined, M_Tech, M_Speed]

    # Handle null techniques
    for i in range(len(dfs)):
        if 'Technique' in dfs[i].columns:
            dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        # Use only common columns for joining to avoid duplicates
        join_columns = [col for col in common_columns if col in merged_df.columns and col in df.columns]
        merged_df = merged_df.join(df, on=join_columns, how="left")

    # Fill nulls forward within each ID group
    merged_df = (
        merged_df.sort(['ID', 'Date', 'Race', 'Place'])  # Sort first to ensure correct forward fill
        .group_by('ID')
        .map_groups(fill_nulls_forward)
    )

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
            merged_df = merged_df.with_columns(
                pl.when(pl.col(pelo_cols[a]).is_null())
                .then(pl.col(elo_cols[a]))
                .otherwise(pl.col(pelo_cols[a]))
                .alias(pelo_cols[a])
            )

    # Apply offseason rules
    merged_df = apply_offseason_rules(merged_df)

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

        # Save the ladies chrono dyn CSV file
        ladiesdf.write_csv("~/ski/elo/python/alpine/polars/excel365/ladies_chrono_dyn.csv")
        print("Saved ladies chrono dyn CSV file")

    if mendf is not None:
        men_nation = mendf.select("Nation").unique().sort(["Nation"])
        print("Men's nations:")
        print(men_nation)

        # Save the men's chrono dyn CSV file
        mendf.write_csv("~/ski/elo/python/alpine/polars/excel365/men_chrono_dyn.csv")
        print("Saved men's chrono dyn CSV file")

    print(f"Total execution time: {time.time() - start_time:.2f} seconds")
