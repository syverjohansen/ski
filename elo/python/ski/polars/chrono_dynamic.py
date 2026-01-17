import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
    elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo",
                "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo",
                "Classic_Elo", "Freestyle_Elo"]

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
        ("Distance_Elo", "Distance_Pelo"),
        ("Distance_C_Elo", "Distance_C_Pelo"),
        ("Distance_F_Elo", "Distance_F_Pelo"),
        ("Sprint_Elo", "Sprint_Pelo"),
        ("Sprint_C_Elo", "Sprint_C_Pelo"),
        ("Sprint_F_Elo", "Sprint_F_Pelo"),
        ("Classic_Elo", "Classic_Pelo"),
        ("Freestyle_Elo", "Freestyle_Pelo")
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
    # Read all ladies dyn files with consistent path
    base_path = '~/ski/elo/python/ski/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.String,
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
        "Birthday": pl.String,
        "Age": pl.String,
        "Exp": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    L = pl.read_csv(f'{base_path}/dyn_L.csv', schema_overrides=schema_overrides)
    L = L.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    L_Distance = pl.read_csv(f'{base_path}/dyn_L_Distance.csv', schema_overrides=schema_overrides)
    L_Distance = L_Distance.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_Pelo', 'pred_Elo': 'Distance_Elo'})

    L_Distance_C = pl.read_csv(f'{base_path}/dyn_L_Distance_C.csv', schema_overrides=schema_overrides)
    L_Distance_C = L_Distance_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_C_Pelo', 'pred_Elo': 'Distance_C_Elo'})

    L_Distance_F = pl.read_csv(f'{base_path}/dyn_L_Distance_F.csv', schema_overrides=schema_overrides)
    L_Distance_F = L_Distance_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_F_Pelo', 'pred_Elo': 'Distance_F_Elo'})

    L_Sprint = pl.read_csv(f'{base_path}/dyn_L_Sprint.csv', schema_overrides=schema_overrides)
    L_Sprint = L_Sprint.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_Pelo', 'pred_Elo': 'Sprint_Elo'})

    L_Sprint_C = pl.read_csv(f'{base_path}/dyn_L_Sprint_C.csv', schema_overrides=schema_overrides)
    L_Sprint_C = L_Sprint_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_C_Pelo', 'pred_Elo': 'Sprint_C_Elo'})

    L_Sprint_F = pl.read_csv(f'{base_path}/dyn_L_Sprint_F.csv', schema_overrides=schema_overrides)
    L_Sprint_F = L_Sprint_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_F_Pelo', 'pred_Elo': 'Sprint_F_Elo'})

    L_C = pl.read_csv(f'{base_path}/dyn_L_C.csv', schema_overrides=schema_overrides)
    L_C = L_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Classic_Pelo', 'pred_Elo': 'Classic_Elo'})

    L_F = pl.read_csv(f'{base_path}/dyn_L_F.csv', schema_overrides=schema_overrides)
    L_F = L_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Freestyle_Pelo', 'pred_Elo': 'Freestyle_Elo'})

    print("Done reading ladies dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS',
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    dfs = [L, L_Distance, L_Distance_C, L_Distance_F, L_Sprint,
           L_Sprint_C, L_Sprint_F, L_C, L_F]

    # Handle null techniques
    for i in range(len(dfs)):
        dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

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
    elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo",
                "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo",
                "Classic_Elo", "Freestyle_Elo"]
    pelo_cols = ["Pelo", "Distance_Pelo", "Distance_C_Pelo", "Distance_F_Pelo",
                 "Sprint_Pelo", "Sprint_C_Pelo", "Sprint_F_Pelo",
                 "Classic_Pelo", "Freestyle_Pelo"]

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
    # Read all men's dyn files with consistent path
    base_path = '~/ski/elo/python/ski/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.String,
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
        "Birthday": pl.String,
        "Age": pl.String,
        "Exp": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    M = pl.read_csv(f'{base_path}/dyn_M.csv', schema_overrides=schema_overrides)
    M = M.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    M_Distance = pl.read_csv(f'{base_path}/dyn_M_Distance.csv', schema_overrides=schema_overrides)
    M_Distance = M_Distance.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_Pelo', 'pred_Elo': 'Distance_Elo'})

    M_Distance_C = pl.read_csv(f'{base_path}/dyn_M_Distance_C.csv', schema_overrides=schema_overrides)
    M_Distance_C = M_Distance_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_C_Pelo', 'pred_Elo': 'Distance_C_Elo'})

    M_Distance_F = pl.read_csv(f'{base_path}/dyn_M_Distance_F.csv', schema_overrides=schema_overrides)
    M_Distance_F = M_Distance_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Distance_F_Pelo', 'pred_Elo': 'Distance_F_Elo'})

    M_Sprint = pl.read_csv(f'{base_path}/dyn_M_Sprint.csv', schema_overrides=schema_overrides)
    M_Sprint = M_Sprint.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_Pelo', 'pred_Elo': 'Sprint_Elo'})

    M_Sprint_C = pl.read_csv(f'{base_path}/dyn_M_Sprint_C.csv', schema_overrides=schema_overrides)
    M_Sprint_C = M_Sprint_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_C_Pelo', 'pred_Elo': 'Sprint_C_Elo'})

    M_Sprint_F = pl.read_csv(f'{base_path}/dyn_M_Sprint_F.csv', schema_overrides=schema_overrides)
    M_Sprint_F = M_Sprint_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Sprint_F_Pelo', 'pred_Elo': 'Sprint_F_Elo'})

    M_C = pl.read_csv(f'{base_path}/dyn_M_C.csv', schema_overrides=schema_overrides)
    M_C = M_C.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Classic_Pelo', 'pred_Elo': 'Classic_Elo'})

    M_F = pl.read_csv(f'{base_path}/dyn_M_F.csv', schema_overrides=schema_overrides)
    M_F = M_F.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Freestyle_Pelo', 'pred_Elo': 'Freestyle_Elo'})

    print("Done reading men's dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'Event', 'MS',
        'Technique', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp'
    ]

    dfs = [M, M_Distance, M_Distance_C, M_Distance_F, M_Sprint,
           M_Sprint_C, M_Sprint_F, M_C, M_F]

    # Handle null techniques
    for i in range(len(dfs)):
        dfs[i] = dfs[i].with_columns(pl.col("Technique").fill_null(""))

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
    elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo",
                "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo",
                "Classic_Elo", "Freestyle_Elo"]
    pelo_cols = ["Pelo", "Distance_Pelo", "Distance_C_Pelo", "Distance_F_Pelo",
                 "Sprint_Pelo", "Sprint_C_Pelo", "Sprint_F_Pelo",
                 "Classic_Pelo", "Freestyle_Pelo"]

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
ladiesdf = ladies()
mendf = men()

# Print unique nations for verification
pl.Config.set_tbl_rows(100)
ladies_nation = ladiesdf.select("Nation").unique().sort(["Nation"])
men_nation = mendf.select("Nation").unique().sort(["Nation"])
print(ladies_nation)
print(men_nation)

# Save the final files as CSV
ladiesdf.write_csv("~/ski/elo/python/ski/polars/excel365/ladies_chrono_dyn.csv")
mendf.write_csv("~/ski/elo/python/ski/polars/excel365/men_chrono_dyn.csv")

print(time.time() - start_time)
