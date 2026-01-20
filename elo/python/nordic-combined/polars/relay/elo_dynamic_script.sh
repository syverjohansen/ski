#!/bin/bash

# First run the update scraper to get the latest all-calendar data:
#   python3 all_scrape.py   # or update variant if available

# Function to generate JSON and run elo_dynamic script
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

    echo "Processing dyn_$output_name..."
    python3 elo_dynamic.py "$json"
}

# Process men's calculations
echo "Starting men's relay ELO dynamic calculations..."
# Overall men's results (relay)
run_elo "M" null "M_rel"
# By individual race types
run_elo "M" "Team" "M_rel_Team"
run_elo "M" "Team Sprint" "M_rel_Team_Sprint"

# Process ladies' calculations
echo "Starting ladies' relay ELO dynamic calculations..."
# Overall ladies' results (relay)
run_elo "L" null "L_rel"
# By individual race types
run_elo "L" "Team" "L_rel_Team"
run_elo "L" "Team Sprint" "L_rel_Team_Sprint"

echo "All Nordic Combined relay ELO dynamic calculations completed!"
