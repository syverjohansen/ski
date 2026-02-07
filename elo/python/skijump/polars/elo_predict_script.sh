#!/bin/bash

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
    "event_filter": 0,
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
    python3 elo_predict.py "$json"
}

# Process men's calculations
echo "Starting men's predicted ELO calculations..."
# Overall men's results (no team events by default)
run_elo "M" null 0 "pred_M"
# By individual race types
run_elo "M" "Small" 0 "pred_M_Small"
run_elo "M" "Medium" 0 "pred_M_Medium"
run_elo "M" "Normal" 0 "pred_M_Normal"
run_elo "M" "Large" 0 "pred_M_Large"
run_elo "M" "Flying" 0 "pred_M_Flying"

# Process ladies' calculations
echo "Starting ladies' predicted ELO calculations..."
# Overall ladies' results (no team events by default)
run_elo "L" null 0 "pred_L"
# By individual race types
run_elo "L" "Small" 0 "pred_L_Small"
run_elo "L" "Medium" 0 "pred_L_Medium"
run_elo "L" "Normal" 0 "pred_L_Normal"
run_elo "L" "Large" 0 "pred_L_Large"
run_elo "L" "Flying" 0 "pred_L_Flying"

echo "All Ski Jumping predicted ELO calculations completed!"
