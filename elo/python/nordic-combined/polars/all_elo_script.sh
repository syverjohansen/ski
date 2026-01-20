#!/bin/bash

# First run all_scrape.py to get the complete calendar data
#python3 all_scrape.py

# Function to generate JSON and run elo script
run_elo() {
    local sex=$1
    local race_type=$2
    local team=$3
    local output_name=$4

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "event_filter": null,
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
run_elo "M" "Individual" 0 "all_M_Individual"
run_elo "M" "Individual Compact" 0 "all_M_Individual_Compact"
run_elo "M" "Sprint" 0 "all_M_Sprint"
run_elo "M" "Mass Start" 0 "all_M_Mass_Start"

# Process ladies' calculations
echo "Starting ladies' ELO calculations (all competitions)..."
# Overall ladies' results (no team events by default)
run_elo "L" null 0 "all_L"
# By individual race types
run_elo "L" "Individual" 0 "all_L_Individual"
run_elo "L" "Individual Compact" 0 "all_L_Individual_Compact"
run_elo "L" "Sprint" 0 "all_L_Sprint"
run_elo "L" "Mass Start" 0 "all_L_Mass_Start"

echo "All Nordic Combined ELO calculations (all competitions) completed!"
