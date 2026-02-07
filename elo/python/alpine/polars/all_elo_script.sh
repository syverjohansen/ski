#!/bin/bash

# First run update_scrape.py to get the latest data
#python3 alpine_update_scrape.py

# Function to generate JSON and run elo script
run_elo() {
    local sex=$1
    local distance=$2
    local output_name=$3

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "distance": "$distance",
    "date1": null,
    "date2": null,
    "city": null,
    "country": null,
    "event": null,
    "place1": null,
    "place2": null,
    "name": null,
    "season1": null,
    "season2": null,
    "nation": null
}
EOF
)
    
    echo "Processing $output_name..."
    python3 all_elo.py "$json"

    # If this is not the main file (like M or L), ensure we rename it with the correct prefix
    if [ "$distance" != "null" ]; then
        base_path="~/ski/elo/python/alpine/polars/excel365"
        # Expand the path
        expanded_path=$(eval echo $base_path)

        # Check if the file without prefix exists and rename it

        if [ -f "$expanded_path/all_$distance.csv" ]; then
            mv "$expanded_path/all_$distance.csv" "$expanded_path/all_${output_name}.csv"
            echo "Renamed all_$distance.csv to all_${output_name}.csv"
        fi
    fi
}

# Process men's calculations
echo "Starting men's ELO calculations..."
# Overall men's results
run_elo "M" null "M"
# By discipline
run_elo "M" "Downhill" "M_Downhill"
run_elo "M" "Super G" "M_SuperG"
run_elo "M" "Giant Slalom" "M_GS" 
run_elo "M" "Slalom" "M_SL"
run_elo "M" "Combined" "M_Combined"
# By category
run_elo "M" "Tech" "M_Tech"
run_elo "M" "Speed" "M_Speed"

# Process ladies' calculations
echo "Starting ladies' ELO calculations..."
# Overall ladies' results
run_elo "L" null "L"
# By discipline
run_elo "L" "Downhill" "L_Downhill"
run_elo "L" "Super G" "L_SuperG"
run_elo "L" "Giant Slalom" "L_GS"
run_elo "L" "Slalom" "L_SL"
run_elo "L" "Combined" "L_Combined"
# By category
run_elo "L" "Tech" "L_Tech"
run_elo "L" "Speed" "L_Speed"

echo "All Alpine ELO calculations completed!"