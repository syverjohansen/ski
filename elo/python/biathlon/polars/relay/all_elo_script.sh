#!/bin/bash

# First run all_scrape.py to get the complete calendar data
#python3 all_scrape.py

# Function to generate JSON and run elo script
run_elo() {
    local sex=$1
    local race_type=$2
    local output_name=$3

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "event_filter": null,
    "race_type": "$race_type",
    "relay_filter": 1,
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
echo "Starting men's relay ELO calculations (all competitions)..."
# Overall men's results (relay)
run_elo "M" null "all_M_rel"
# By individual race types
run_elo "M" "Individual" "all_M_rel_Individual"
run_elo "M" "Sprint" "all_M_rel_Sprint"
run_elo "M" "Pursuit" "all_M_rel_Pursuit"
run_elo "M" "Mass Start" "all_M_rel_Mass_Start"

# Process ladies' calculations
echo "Starting ladies' relay ELO calculations (all competitions)..."
# Overall ladies' results (relay)
run_elo "L" null "all_L_rel"
# By individual race types
run_elo "L" "Individual" "all_L_rel_Individual"
run_elo "L" "Sprint" "all_L_rel_Sprint"
run_elo "L" "Pursuit" "all_L_rel_Pursuit"
run_elo "L" "Mass Start" "all_L_rel_Mass_Start"

echo "All Biathlon relay ELO calculations (all competitions) completed!"
