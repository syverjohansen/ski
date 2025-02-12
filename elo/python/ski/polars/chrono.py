import polars as pl
import time
from functools import reduce
pl.Config.set_tbl_cols(100)
start_time = time.time()

def fill_nulls_forward(df):
    # Modify to use agg instead of apply
    elo_cols = ["Elo", "Distance_Elo", "Distance_C_Elo", "Distance_F_Elo", 
                "Sprint_Elo", "Sprint_C_Elo", "Sprint_F_Elo", 
                "Classic_Elo", "Freestyle_Elo"]
    
    # Create a list of expressions for forward fill
    exprs = [
        pl.col(col).fill_null(strategy="forward").alias(col)
        for col in elo_cols
    ]
    
    # Add all other columns to preserve them
    other_cols = [col for col in df.columns if col not in elo_cols]
    exprs.extend([pl.col(col) for col in other_cols])
    
    return df.select(exprs)

def ladies():
    # Read all ladies files with consistent path
    base_path = '~/ski/elo/python/ski/polars/excel365'
    
    L = pl.read_ipc(f'{base_path}/L.feather')
    L_Distance = pl.read_ipc(f'{base_path}/L_Distance.feather')
    L_Distance = L_Distance.rename({"Pelo": "Distance_Pelo", "Elo": "Distance_Elo"})
    L_Distance_C = pl.read_ipc(f'{base_path}/L_Distance_C.feather')
    L_Distance_C = L_Distance_C.rename({'Pelo': 'Distance_C_Pelo', 'Elo': 'Distance_C_Elo'})
    L_Distance_F = pl.read_ipc(f'{base_path}/L_Distance_F.feather')
    L_Distance_F = L_Distance_F.rename({'Pelo': 'Distance_F_Pelo', 'Elo': 'Distance_F_Elo'})
    L_Sprint = pl.read_ipc(f'{base_path}/L_Sprint.feather')
    L_Sprint = L_Sprint.rename({'Pelo': 'Sprint_Pelo', 'Elo': 'Sprint_Elo'})
    L_Sprint_C = pl.read_ipc(f'{base_path}/L_Sprint_C.feather')
    L_Sprint_C = L_Sprint_C.rename({'Pelo': 'Sprint_C_Pelo', 'Elo': 'Sprint_C_Elo'})
    L_Sprint_F = pl.read_ipc(f'{base_path}/L_Sprint_F.feather')
    L_Sprint_F = L_Sprint_F.rename({'Pelo': 'Sprint_F_Pelo', 'Elo': 'Sprint_F_Elo'})
    L_C = pl.read_ipc(f'{base_path}/L_C.feather')
    L_C = L_C.rename({'Pelo': 'Classic_Pelo', 'Elo': 'Classic_Elo'})
    L_F = pl.read_ipc(f'{base_path}/L_F.feather')
    L_F = L_F.rename({'Pelo': 'Freestyle_Pelo', 'Elo': 'Freestyle_Elo'})
    print("Done reading ladies files")

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
        merged_df.sort(['ID', 'Season', 'Race', 'Place'])  # Sort first to ensure correct forward fill
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
        merged_df = merged_df.with_columns(
            pl.when(pl.col(pelo_cols[a]).is_null())
            .then(pl.col(elo_cols[a]))
            .otherwise(pl.col(pelo_cols[a]))
            .alias(pelo_cols[a])
        )

    # Sort the final DataFrame
    merged_df = merged_df.sort(['Season', 'Race', 'Place'])
    return merged_df

def men():
    # Read all men's files with consistent path
    base_path = '~/ski/elo/python/ski/polars/excel365'
    
    M = pl.read_ipc(f'{base_path}/M.feather')
    M_Distance = pl.read_ipc(f'{base_path}/M_Distance.feather')
    M_Distance = M_Distance.rename({"Pelo": "Distance_Pelo", "Elo": "Distance_Elo"})
    M_Distance_C = pl.read_ipc(f'{base_path}/M_Distance_C.feather')
    M_Distance_C = M_Distance_C.rename({'Pelo': 'Distance_C_Pelo', 'Elo': 'Distance_C_Elo'})
    M_Distance_F = pl.read_ipc(f'{base_path}/M_Distance_F.feather')
    M_Distance_F = M_Distance_F.rename({'Pelo': 'Distance_F_Pelo', 'Elo': 'Distance_F_Elo'})
    M_Sprint = pl.read_ipc(f'{base_path}/M_Sprint.feather')
    M_Sprint = M_Sprint.rename({'Pelo': 'Sprint_Pelo', 'Elo': 'Sprint_Elo'})
    M_Sprint_C = pl.read_ipc(f'{base_path}/M_Sprint_C.feather')
    M_Sprint_C = M_Sprint_C.rename({'Pelo': 'Sprint_C_Pelo', 'Elo': 'Sprint_C_Elo'})
    M_Sprint_F = pl.read_ipc(f'{base_path}/M_Sprint_F.feather')
    M_Sprint_F = M_Sprint_F.rename({'Pelo': 'Sprint_F_Pelo', 'Elo': 'Sprint_F_Elo'})
    M_C = pl.read_ipc(f'{base_path}/M_C.feather')
    M_C = M_C.rename({'Pelo': 'Classic_Pelo', 'Elo': 'Classic_Elo'})
    M_F = pl.read_ipc(f'{base_path}/M_F.feather')
    M_F = M_F.rename({'Pelo': 'Freestyle_Pelo', 'Elo': 'Freestyle_Elo'})
    print("Done reading men's files")

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
        merged_df.sort(['ID', 'Season', 'Race', 'Place'])  # Sort first to ensure correct forward fill
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
        merged_df = merged_df.with_columns(
            pl.when(pl.col(pelo_cols[a]).is_null())
            .then(pl.col(elo_cols[a]))
            .otherwise(pl.col(pelo_cols[a]))
            .alias(pelo_cols[a])
        )

    # Sort the final DataFrame
    merged_df = merged_df.sort(['Season', 'Race', 'Place'])
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
ladiesdf.write_ipc("~/ski/elo/python/ski/polars/excel365/ladies_chrono.feather")
mendf.write_ipc("~/ski/elo/python/ski/polars/excel365/men_chrono.feather")

# Also save as CSV for easier viewing/sharing
ladiesdf.write_csv("~/ski/elo/python/ski/polars/excel365/ladies_chrono.csv")
mendf.write_csv("~/ski/elo/python/ski/polars/excel365/men_chrono.csv")

print(time.time() - start_time)