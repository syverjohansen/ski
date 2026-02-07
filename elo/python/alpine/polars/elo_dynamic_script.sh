#!/bin/bash

# Function to generate JSON and run elo_dynamic script
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
    python3 elo_dynamic.py "$json"
}

# Process men's calculations
echo "Starting men's dynamic ELO calculations..."
run_elo "M" null "dyn_M"
run_elo "M" \"Downhill\" "dyn_M_Downhill"
run_elo "M" \"Super\ G\" "dyn_M_SuperG"
run_elo "M" \"Giant\ Slalom\" "dyn_M_GS"
run_elo "M" \"Slalom\" "dyn_M_SL"
run_elo "M" \"Combined\" "dyn_M_Combined"
run_elo "M" \"Tech\" "dyn_M_Tech"
run_elo "M" \"Speed\" "dyn_M_Speed"

# Process ladies' calculations
echo "Starting ladies' dynamic ELO calculations..."
run_elo "L" null "dyn_L"
run_elo "L" \"Downhill\" "dyn_L_Downhill"
run_elo "L" \"Super\ G\" "dyn_L_SuperG"
run_elo "L" \"Giant\ Slalom\" "dyn_L_GS"
run_elo "L" \"Slalom\" "dyn_L_SL"
run_elo "L" \"Combined\" "dyn_L_Combined"
run_elo "L" \"Tech\" "dyn_L_Tech"
run_elo "L" \"Speed\" "dyn_L_Speed"

echo "All Alpine dynamic ELO calculations completed!"
