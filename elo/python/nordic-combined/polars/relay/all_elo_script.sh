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
    "team_filter": 1,
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
run_elo "M" "Team" "all_M_rel_Team"
run_elo "M" "Team Sprint" "all_M_rel_Team_Sprint"

# Process ladies' calculations
echo "Starting ladies' relay ELO calculations (all competitions)..."
# Overall ladies' results (relay)
run_elo "L" null "all_L_rel"
# By individual race types
run_elo "L" "Team" "all_L_rel_Team"
run_elo "L" "Team Sprint" "all_L_rel_Team_Sprint"

echo "All Nordic Combined relay ELO calculations (all competitions) completed!"
