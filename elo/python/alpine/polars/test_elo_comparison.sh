#!/bin/bash

# Test script to compare original ELO vs time-decay ELO
# This script runs both versions and compares timing and results

echo "=== ELO Implementation Comparison Test ==="
echo "Date: $(date)"
echo

# Set the directory
cd ~/ski/elo/python/alpine/polars

# Test configurations to run
declare -a test_configs=(
    '{"sex": "M"}'
    '{"sex": "W"}'
    '{"sex": "M", "distance": "Slalom"}'
    '{"sex": "W", "distance": "Downhill"}'
    '{"sex": "M", "season1": 2020, "season2": 2023}'
)

# Function to run a single test
run_test() {
    local config="$1"
    local version="$2"
    local script="$3"
    
    echo "--- Running $version ---"
    echo "Config: $config"
    
    start_time=$(date +%s.%N)
    python3 "$script" "$config"
    end_time=$(date +%s.%N)
    
    execution_time=$(echo "$end_time - $start_time" | bc -l)
    echo "Execution time: ${execution_time} seconds"
    echo
}

# Function to compare results
compare_results() {
    local config_name="$1"
    local original_file="$2"
    local decay_file="$3"
    
    echo "--- Comparing Results for $config_name ---"
    
    if [[ -f "$original_file" && -f "$decay_file" ]]; then
        # Get file sizes
        original_size=$(wc -l < "$original_file")
        decay_size=$(wc -l < "$decay_file")
        
        echo "Original file: $original_file ($original_size lines)"
        echo "Time decay file: $decay_file ($decay_size lines)"
        
        # Compare first few lines (headers should be the same)
        echo "First 5 lines comparison:"
        echo "Original:"
        head -5 "$original_file"
        echo "Time decay:"
        head -5 "$decay_file"
        
        # Simple statistical comparison
        if command -v python3 &> /dev/null; then
            python3 - << EOF
import pandas as pd
import numpy as np

try:
    # Load both files
    df_orig = pd.read_csv('$original_file')
    df_decay = pd.read_csv('$decay_file')
    
    print(f"Original ELO stats:")
    print(f"  Mean ELO: {df_orig['Elo'].mean():.2f}")
    print(f"  Std ELO: {df_orig['Elo'].std():.2f}")
    print(f"  Min ELO: {df_orig['Elo'].min():.2f}")
    print(f"  Max ELO: {df_orig['Elo'].max():.2f}")
    
    print(f"Time Decay ELO stats:")
    print(f"  Mean ELO: {df_decay['Elo'].mean():.2f}")
    print(f"  Std ELO: {df_decay['Elo'].std():.2f}")
    print(f"  Min ELO: {df_decay['Elo'].min():.2f}")
    print(f"  Max ELO: {df_decay['Elo'].max():.2f}")
    
    # Compare ELO ranges
    elo_diff = abs(df_orig['Elo'].mean() - df_decay['Elo'].mean())
    print(f"Mean ELO difference: {elo_diff:.2f}")
    
except Exception as e:
    print(f"Error comparing files: {e}")
EOF
        fi
    else
        echo "One or both result files not found:"
        echo "  Original: $original_file"
        echo "  Time decay: $decay_file"
    fi
    echo
}

# Main test loop
echo "Starting comparison tests..."
echo

total_original_time=0
total_decay_time=0

for config in "${test_configs[@]}"; do
    echo "=========================================="
    echo "Testing configuration: $config"
    echo "=========================================="
    
    # Run original version
    echo "🏃‍♂️ Running ORIGINAL ELO implementation..."
    start_time=$(date +%s.%N)
    python3 elo.py "$config" 2>&1
    end_time=$(date +%s.%N)
    original_time=$(echo "$end_time - $start_time" | bc -l)
    total_original_time=$(echo "$total_original_time + $original_time" | bc -l)
    echo "✅ Original completed in: ${original_time} seconds"
    echo
    
    # Run time decay version
    echo "🏃‍♂️ Running TIME DECAY ELO implementation..."
    start_time=$(date +%s.%N)
    python3 elo_test_decay.py "$config" 2>&1
    end_time=$(date +%s.%N)
    decay_time=$(echo "$end_time - $start_time" | bc -l)
    total_decay_time=$(echo "$total_decay_time + $decay_time" | bc -l)
    echo "✅ Time decay completed in: ${decay_time} seconds"
    echo
    
    # Calculate speedup/slowdown
    if (( $(echo "$original_time > 0" | bc -l) )); then
        ratio=$(echo "scale=2; $decay_time / $original_time" | bc -l)
        if (( $(echo "$ratio > 1" | bc -l) )); then
            echo "⏱️  Time decay is ${ratio}x SLOWER than original"
        else
            speedup=$(echo "scale=2; $original_time / $decay_time" | bc -l)
            echo "⚡ Time decay is ${speedup}x FASTER than original"
        fi
    fi
    echo
    
    # Determine output filenames based on config
    sex=$(echo "$config" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('sex', 'M'))")
    
    # Build filename based on config
    filename=""
    for key in distance date1 date2 city country event place1 place2 name season1 season2 nation; do
        value=$(echo "$config" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('$key', ''))" 2>/dev/null)
        if [[ -n "$value" && "$value" != "None" ]]; then
            if [[ -n "$filename" ]]; then
                filename="${filename}_${value}"
            else
                filename="$value"
            fi
        fi
    done
    
    if [[ -z "$filename" ]]; then
        filename="$sex"
    fi
    
    original_file="excel365/${filename}.csv"
    decay_file="excel365/${filename}_time_decay.csv"
    
    # Compare results
    compare_results "$config" "$original_file" "$decay_file"
    
    echo "=========================================="
    echo
done

# Summary
echo "🎯 FINAL SUMMARY"
echo "=========================================="
echo "Total original ELO time: ${total_original_time} seconds"
echo "Total time decay ELO time: ${total_decay_time} seconds"

if (( $(echo "$total_original_time > 0" | bc -l) )); then
    overall_ratio=$(echo "scale=2; $total_decay_time / $total_original_time" | bc -l)
    if (( $(echo "$overall_ratio > 1" | bc -l) )); then
        echo "📊 Overall: Time decay is ${overall_ratio}x SLOWER"
    else
        overall_speedup=$(echo "scale=2; $total_original_time / $total_decay_time" | bc -l)
        echo "📊 Overall: Time decay is ${overall_speedup}x FASTER"
    fi
fi

echo
echo "📁 Result files are stored in:"
echo "   - Original: excel365/*.csv"
echo "   - Time decay: excel365/*_time_decay.csv"
echo
echo "🔍 To examine specific results, check the files in excel365/ directory"
echo "✅ Test completed!"