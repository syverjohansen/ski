import polars as pl
import time
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    """
    Fill null values forward for Elo columns within each ID group
    """
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
    """Process and combine all ladies' dynamic ELO data"""
    # Read all ladies dyn files with consistent path
    base_path = '~/ski/elo/python/skijump/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.Date,
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
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Length1": pl.Float64,
        "Length2": pl.Float64,
        "Points": pl.Float64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    L = pl.read_csv(f'{base_path}/dyn_L.csv', schema_overrides=schema_overrides)
    L = L.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    L_Small = pl.read_csv(f'{base_path}/dyn_L_Small.csv', schema_overrides=schema_overrides)
    L_Small = L_Small.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Small_Pelo', 'pred_Elo': 'Small_Elo'})

    L_Medium = pl.read_csv(f'{base_path}/dyn_L_Medium.csv', schema_overrides=schema_overrides)
    L_Medium = L_Medium.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Medium_Pelo', 'pred_Elo': 'Medium_Elo'})

    L_Normal = pl.read_csv(f'{base_path}/dyn_L_Normal.csv', schema_overrides=schema_overrides)
    L_Normal = L_Normal.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Normal_Pelo', 'pred_Elo': 'Normal_Elo'})

    L_Large = pl.read_csv(f'{base_path}/dyn_L_Large.csv', schema_overrides=schema_overrides)
    L_Large = L_Large.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Large_Pelo', 'pred_Elo': 'Large_Elo'})

    L_Flying = pl.read_csv(f'{base_path}/dyn_L_Flying.csv', schema_overrides=schema_overrides)
    L_Flying = L_Flying.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Flying_Pelo', 'pred_Elo': 'Flying_Elo'})

    print("Done reading ladies dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'HillSize', 'RaceType', 'TeamEvent',
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp', 'Leg', 'Length1', 'Length2', 'Points'
    ]

    dfs = [L, L_Small, L_Medium, L_Normal, L_Large, L_Flying]

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
    elo_cols = ["Elo", "Small_Elo", "Medium_Elo", "Normal_Elo", "Large_Elo", "Flying_Elo"]
    pelo_cols = ["Pelo", "Small_Pelo", "Medium_Pelo", "Normal_Pelo", "Large_Pelo", "Flying_Pelo"]

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
    base_path = '~/ski/elo/python/skijump/polars/excel365'

    # Define consistent schema based on elo_dynamic.py output
    schema_overrides = {
        "Date": pl.Date,
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
        "Birthday": pl.Datetime,
        "Age": pl.Float64,
        "Exp": pl.Int64,
        "Leg": pl.Int64,
        "Length1": pl.Float64,
        "Length2": pl.Float64,
        "Points": pl.Float64,
        "Elo": pl.Float64,
        "Pelo": pl.Float64,
        "pred_Elo": pl.Float64,
        "pred_Pelo": pl.Float64
    }

    # Read dyn files and use pred_Pelo/pred_Elo as the Pelo/Elo values
    M = pl.read_csv(f'{base_path}/dyn_M.csv', schema_overrides=schema_overrides)
    M = M.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Pelo', 'pred_Elo': 'Elo'})

    M_Small = pl.read_csv(f'{base_path}/dyn_M_Small.csv', schema_overrides=schema_overrides)
    M_Small = M_Small.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Small_Pelo', 'pred_Elo': 'Small_Elo'})

    M_Medium = pl.read_csv(f'{base_path}/dyn_M_Medium.csv', schema_overrides=schema_overrides)
    M_Medium = M_Medium.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Medium_Pelo', 'pred_Elo': 'Medium_Elo'})

    M_Normal = pl.read_csv(f'{base_path}/dyn_M_Normal.csv', schema_overrides=schema_overrides)
    M_Normal = M_Normal.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Normal_Pelo', 'pred_Elo': 'Normal_Elo'})

    M_Large = pl.read_csv(f'{base_path}/dyn_M_Large.csv', schema_overrides=schema_overrides)
    M_Large = M_Large.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Large_Pelo', 'pred_Elo': 'Large_Elo'})

    M_Flying = pl.read_csv(f'{base_path}/dyn_M_Flying.csv', schema_overrides=schema_overrides)
    M_Flying = M_Flying.drop(['Pelo', 'Elo']).rename({'pred_Pelo': 'Flying_Pelo', 'pred_Elo': 'Flying_Elo'})

    print("Done reading men's dyn files")

    # Common columns that match our current schema
    common_columns = [
        'Date', 'City', 'Country', 'Sex', 'HillSize', 'RaceType', 'TeamEvent',
        'Event', 'Place', 'Skier', 'Nation', 'ID', 'Season',
        'Race', 'Birthday', 'Age', 'Exp', 'Leg', 'Length1', 'Length2', 'Points'
    ]

    dfs = [M, M_Small, M_Medium, M_Normal, M_Large, M_Flying]

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
    elo_cols = ["Elo", "Small_Elo", "Medium_Elo", "Normal_Elo", "Large_Elo", "Flying_Elo"]
    pelo_cols = ["Pelo", "Small_Pelo", "Medium_Pelo", "Normal_Pelo", "Large_Pelo", "Flying_Pelo"]

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
ladiesdf.write_csv("~/ski/elo/python/skijump/polars/excel365/ladies_chrono_dyn.csv")
mendf.write_csv("~/ski/elo/python/skijump/polars/excel365/men_chrono_dyn.csv")

print(time.time() - start_time)
