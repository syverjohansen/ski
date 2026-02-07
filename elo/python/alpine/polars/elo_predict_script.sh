#!/bin/bash

# Function to generate JSON and run elo_predict script
run_elo() {
    local sex=$1
    local distance=$2
    local output_name=$3

    # Generate JSON configuration
    json=$(cat <<EOF
{
    "sex": "$sex",
    "event_filter": 0,
    "distance": $distance,
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
run_elo "M" null "pred_M"
run_elo "M" \"Downhill\" "pred_M_Downhill"
run_elo "M" \"Super\ G\" "pred_M_SuperG"
run_elo "M" \"Giant\ Slalom\" "pred_M_GS"
run_elo "M" \"Slalom\" "pred_M_SL"
run_elo "M" \"Combined\" "pred_M_Combined"
run_elo "M" \"Tech\" "pred_M_Tech"
run_elo "M" \"Speed\" "pred_M_Speed"

# Process ladies' calculations
echo "Starting ladies' predicted ELO calculations..."
run_elo "L" null "pred_L"
run_elo "L" \"Downhill\" "pred_L_Downhill"
run_elo "L" \"Super\ G\" "pred_L_SuperG"
run_elo "L" \"Giant\ Slalom\" "pred_L_GS"
run_elo "L" \"Slalom\" "pred_L_SL"
run_elo "L" \"Combined\" "pred_L_Combined"
run_elo "L" \"Tech\" "pred_L_Tech"
run_elo "L" \"Speed\" "pred_L_Speed"

echo "All Alpine predicted ELO calculations completed!"
