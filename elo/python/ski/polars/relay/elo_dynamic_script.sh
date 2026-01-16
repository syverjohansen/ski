#!/bin/bash

# First run the update scrapers and merge to get the latest combined data:
#   python3 all_update_scrape.py      # Update FIS data
#   python3 russia_update_scrape.py   # Update Russia data
#   python3 merge_scrape.py           # Merge into combined_*_scrape.csv

# Function to generate JSON and run elo_dynamic script
run_elo() {
    local sex=$1
    local distance=$2
    local technique=$3
    local output_name=$4

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "relay": 1,
    "date1": null,
    "date2": null,
    "city": null,
    "country": null,
    "event": null,
    "distance": "$distance",
    "ms": null,
    "technique": "$technique",
    "place1": null,
    "place2": null,
    "name": null,
    "season1": null,
    "season2": null,
    "nation": null,
    "race1": null,
    "race2": null
}
EOF
)

    echo "Processing dyn_$output_name..."
    python3 elo_dynamic.py "$json"
}

# Process all combinations
# Note: elo_dynamic.py adds "dyn_" prefix to output files automatically
for sex in "M" "L"; do
    # All results for sex
    run_elo "$sex" null null "${sex}"

    # By technique
    run_elo "$sex" null "F" "${sex}_F"
    run_elo "$sex" null "C" "${sex}_C"

    # By distance type
    run_elo "$sex" "Distance" null "${sex}_Distance"
    run_elo "$sex" "Sprint" null "${sex}_Sprint"

    # Distance combinations
    run_elo "$sex" "Distance" "F" "${sex}_Distance_F"
    run_elo "$sex" "Distance" "C" "${sex}_Distance_C"

    # Sprint combinations
    run_elo "$sex" "Sprint" "F" "${sex}_Sprint_F"
    run_elo "$sex" "Sprint" "C" "${sex}_Sprint_C"
done

echo "All ELO dynamic calculations completed!"