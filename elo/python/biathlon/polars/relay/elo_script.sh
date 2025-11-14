#!/bin/bash

# First run update_scrape.py to get the latest data
#python3 biathlon_update_scrape.py

# Function to generate JSON and run elo script
run_elo() {
    local sex=$1
    local race_type=$2
    local relay=$3
    local output_name=$4

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "event_filter": null,
    "race_type": "$race_type",
    "relay_filter": $relay,
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
    python3 elo.py "$json"
}

# Process men's calculations
echo "Starting men's ELO calculations..."
# Overall men's results (no relays by default)
run_elo "M" null 1 "M"
# By individual race types
run_elo "M" "Individual" 0 "M_Individual"
run_elo "M" "Sprint" 0 "M_Sprint"
run_elo "M" "Pursuit" 0 "M_Pursuit"
run_elo "M" "Mass Start" 0 "M_Mass_Start"

# Process ladies' calculations
echo "Starting ladies' ELO calculations..."
# Overall ladies' results (no relays by default)
run_elo "L" null 1 "L"
# By individual race types
run_elo "L" "Individual" 0 "L_Individual"
run_elo "L" "Sprint" 0 "L_Sprint"
run_elo "L" "Pursuit" 0 "L_Pursuit"
run_elo "L" "Mass Start" 0 "L_Mass_Start"

echo "All Biathlon ELO calculations completed!"