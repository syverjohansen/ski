#!/bin/bash

# First run the update scraper to get the latest all-calendar data:
#   python3 all_scrape.py   # or update variant if available

# Function to generate JSON and run elo_predict script
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

    echo "Processing pred_$output_name..."
    python3 elo_predict.py "$json"
}

# Process men's calculations
echo "Starting men's relay ELO predict calculations..."
# Overall men's results (relay)
run_elo "M" null "M_rel"
# By individual race types
run_elo "M" "Individual" "M_rel_Individual"
run_elo "M" "Sprint" "M_rel_Sprint"
run_elo "M" "Pursuit" "M_rel_Pursuit"
run_elo "M" "Mass Start" "M_rel_Mass_Start"

# Process ladies' calculations
echo "Starting ladies' relay ELO predict calculations..."
# Overall ladies' results (relay)
run_elo "L" null "L_rel"
# By individual race types
run_elo "L" "Individual" "L_rel_Individual"
run_elo "L" "Sprint" "L_rel_Sprint"
run_elo "L" "Pursuit" "L_rel_Pursuit"
run_elo "L" "Mass Start" "L_rel_Mass_Start"

echo "All Biathlon relay ELO predict calculations completed!"
