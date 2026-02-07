#!/bin/bash

# First run all_update_scrape.py to get the latest data
#python3 all_update_scrape.py

# Function to generate JSON and run all_elo script
run_elo() {
    local sex=$1
    local race_type=$2
    local team=$3
    local output_name=$4

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "event_filter": 1,
    "race_type": "$race_type",
    "team_filter": $team,
    "date1": null,
    "date2": null,
    "city": null,
    "country": null,
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
}

# Process men's calculations
echo "Starting men's ELO calculations (all competitions)..."
# Overall men's results (no team events by default)
run_elo "M" null 0 "all_M"
# By individual race types
run_elo "M" "Small" 0 "all_M_Small"
run_elo "M" "Medium" 0 "all_M_Medium"
run_elo "M" "Normal" 0 "all_M_Normal"
run_elo "M" "Large" 0 "all_M_Large"
run_elo "M" "Flying" 0 "all_M_Flying"

# Process ladies' calculations
echo "Starting ladies' ELO calculations (all competitions)..."
# Overall ladies' results (no team events by default)
run_elo "L" null 0 "all_L"
# By individual race types
run_elo "L" "Small" 0 "all_L_Small"
run_elo "L" "Medium" 0 "all_L_Medium"
run_elo "L" "Normal" 0 "all_L_Normal"
run_elo "L" "Large" 0 "all_L_Large"
run_elo "L" "Flying" 0 "all_L_Flying"

echo "All Ski Jumping ELO calculations (all competitions) completed!"
