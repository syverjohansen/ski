#!/bin/bash

# First run the update scrapers to get the latest data:
#   python3 all_update_scrape.py      # Update data from all competitions

# Function to generate JSON and run elo_predict script
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

    echo "Processing pred_$output_name..."
    python3 elo_predict.py "$json"
}

# Process men's calculations
echo "Starting men's ELO predict calculations (all competitions)..."
# Overall men's results (no team events by default)
run_elo "M" null 0 "all_M"
# By individual race types
run_elo "M" "Individual" 0 "all_M_Individual"
run_elo "M" "Individual Compact" 0 "all_M_Individual_Compact"
run_elo "M" "Sprint" 0 "all_M_Sprint"
run_elo "M" "Mass Start" 0 "all_M_Mass_Start"

# Process ladies' calculations
echo "Starting ladies' ELO predict calculations (all competitions)..."
# Overall ladies' results (no team events by default)
run_elo "L" null 0 "all_L"
# By individual race types
run_elo "L" "Individual" 0 "all_L_Individual"
run_elo "L" "Individual Compact" 0 "all_L_Individual_Compact"
run_elo "L" "Sprint" 0 "all_L_Sprint"
run_elo "L" "Mass Start" 0 "all_L_Mass_Start"

echo "All Nordic Combined ELO predict calculations completed!"
