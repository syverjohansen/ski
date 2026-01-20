import polars as pl
import time
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
    elo_cols = ["Elo", "Team_Elo", "TeamSprint_Elo"]

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

    # List of all Elo and Pelo column pairs for nordic-combined relay
    elo_pelo_pairs = [
        ("Elo", "Pelo"),
        ("Team_Elo", "Team_Pelo"),
        ("TeamSprint_Elo", "TeamSprint_Pelo")
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
    # Read all ladies pred files with consistent path
    base_path = '~/ski/elo/python/nordic-combined/polars/relay/excel365'

    # Define consistent schema based on elo_predict.py output
    schema_overrides = {
        "Date": pl.Date,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "Distance": pl.String,
        "RaceType": pl.String,
        "MassStart": pl.Int64,
        "Event": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.String,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read pred files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    L = pl.read_csv(f'{base_path}/pred_L_rel.csv', schema_overrides=schema_overrides)
    L = L.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    L_Team = pl.read_csv(f'{base_path}/pred_L_rel_Team.csv', schema_overrides=schema_overrides)
    L_Team = L_Team.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Team_Pelo', 'pred_Elo': 'Team_Elo'})

    L_Team_Sprint = pl.read_csv(f'{base_path}/pred_L_rel_Team_Sprint.csv', schema_overrides=schema_overrides)
    L_Team_Sprint = L_Team_Sprint.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'TeamSprint_Pelo', 'pred_Elo': 'TeamSprint_Elo'})

    print("Done reading ladies pred files")

    # Common columns that match our current schema (includes Leg for relay)
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'RaceType', 'MassStart',
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp', 'Leg'
    ]

    dfs = [L, L_Team, L_Team_Sprint]

    # Handle null RaceType
    for i in range(len(dfs)):
        dfs[i] = dfs[i].with_columns(pl.col("RaceType").fill_null(""))

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
    elo_cols = ["Elo", "Team_Elo", "TeamSprint_Elo"]
    pelo_cols = ["Pelo", "Team_Pelo", "TeamSprint_Pelo"]

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

    # Sort the final DataFrame (includes Leg for relay)
    merged_df = merged_df.sort(['Date', 'Race', 'Place', 'Leg'])
    return merged_df

def men():
    # Read all men's pred files with consistent path
    base_path = '~/ski/elo/python/nordic-combined/polars/relay/excel365'

    # Define consistent schema based on elo_predict.py output
    schema_overrides = {
        "Date": pl.Date,
        "City": pl.String,
        "Country": pl.String,
        "Sex": pl.String,
        "Distance": pl.String,
        "RaceType": pl.String,
        "MassStart": pl.Int64,
        "Event": pl.String,
        "Place": pl.Int64,
        "Skier": pl.String,
        "Nation": pl.String,
        "ID": pl.String,
        "Season": pl.Int64,
        "Race": pl.Int64,
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read pred files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    M = pl.read_csv(f'{base_path}/pred_M_rel.csv', schema_overrides=schema_overrides)
    M = M.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    M_Team = pl.read_csv(f'{base_path}/pred_M_rel_Team.csv', schema_overrides=schema_overrides)
    M_Team = M_Team.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Team_Pelo', 'pred_Elo': 'Team_Elo'})

    M_Team_Sprint = pl.read_csv(f'{base_path}/pred_M_rel_Team_Sprint.csv', schema_overrides=schema_overrides)
    M_Team_Sprint = M_Team_Sprint.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'TeamSprint_Pelo', 'pred_Elo': 'TeamSprint_Elo'})

    print("Done reading men's pred files")

    # Common columns that match our current schema (includes Leg for relay)
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'Distance', 'RaceType', 'MassStart',
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp', 'Leg'
    ]

    dfs = [M, M_Team, M_Team_Sprint]

    # Handle null RaceType
    for i in range(len(dfs)):
        dfs[i] = dfs[i].with_columns(pl.col("RaceType").fill_null(""))

    # Merge all dataframes
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.join(df, on=common_columns, how="left")

    # Fill nulls forward within each ID group
    merged_df = (
        merged_df.sort(['ID', 'Date', 'Race', 'Place', 'Leg'])  # Sort first to ensure correct forward fill
        .group_by('ID')
        .map_groups(fill_nulls_forward)
    )

    # Fill Pelo values with corresponding Elo values when Pelo is null
    elo_cols = ["Elo", "Team_Elo", "TeamSprint_Elo"]
    pelo_cols = ["Pelo", "Team_Pelo", "TeamSprint_Pelo"]

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

    # Sort the final DataFrame (includes Leg for relay)
    merged_df = merged_df.sort(['Date', 'Race', 'Place', 'Leg'])
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
ladiesdf.write_csv("~/ski/elo/python/nordic-combined/polars/relay/excel365/ladies_chrono_pred.csv")
mendf.write_csv("~/ski/elo/python/nordic-combined/polars/relay/excel365/men_chrono_pred.csv")

print(time.time() - start_time)
