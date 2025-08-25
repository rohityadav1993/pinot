#!/bin/bash

# Extensive benchmark script for column-wise vs row-wise segment refresh
# Tests: 3 schema scenarios × 3 column counts × 3 row counts = 27 combinations

echo "=== EXTENSIVE REFRESH SEGMENT BENCHMARK ==="
echo "Testing all combinations of:"
echo "  Schema Evolution: NONE, ADD_COLUMNS, CHANGE_DATATYPES"
echo "  Columns: 30, 40, 50"
echo "  Rows: 500000, 1000000, 5000000"
echo "  Total test cases: 27"
echo ""

# Arrays for test parameters
SCHEMA_TYPES=("NONE" "ADD_COLUMNS" "CHANGE_DATATYPES")
COLUMN_COUNTS=(30 40 50)
ROW_COUNTS=(500000 1000000 5000000)

# Results storage
RESULTS_FILE="extensive_benchmark_results_$(date +%Y%m%d_%H%M%S).csv"
echo "Schema,Columns,Rows,RowWiseTime(ms),ColumnWiseTime(ms),Improvement(%),SpeedupFactor" > $RESULTS_FILE

# Progress tracking
TOTAL_TESTS=27
CURRENT_TEST=0

echo "Results will be saved to: $RESULTS_FILE"
echo ""

# Main benchmark loop
for schema in "${SCHEMA_TYPES[@]}"; do
    for columns in "${COLUMN_COUNTS[@]}"; do
        for rows in "${ROW_COUNTS[@]}"; do
            CURRENT_TEST=$((CURRENT_TEST + 1))

            echo "=== Test $CURRENT_TEST/$TOTAL_TESTS ==="
            echo "Schema: $schema, Columns: $columns, Rows: $rows"
            echo "Started at: $(date)"

            # Run the benchmark and capture output
            OUTPUT=$(./target/pinot-perf-pkg/bin/pinot-SimpleRefreshSegmentBenchmark.sh \
                --rows $rows \
                --columns $columns \
                --evolution $schema 2>&1)

            # Extract timing results from output
            ROW_TIME=$(echo "$OUTPUT" | grep "Row-wise refresh time:" | sed 's/.*: \([0-9]*\) ms/\1/')
            COL_TIME=$(echo "$OUTPUT" | grep "Column-wise refresh time:" | sed 's/.*: \([0-9]*\) ms/\1/')
            IMPROVEMENT=$(echo "$OUTPUT" | grep "Performance improvement:" | sed 's/.*: \([0-9.]*\)%.*/\1/')
            SPEEDUP=$(echo "$OUTPUT" | grep "Speedup factor:" | sed 's/.*: \([0-9.]*\)x.*/\1/')

            # Save results to CSV
            echo "$schema,$columns,$rows,$ROW_TIME,$COL_TIME,$IMPROVEMENT,$SPEEDUP" >> $RESULTS_FILE

            # Display progress
            echo "  Row-wise: ${ROW_TIME}ms, Column-wise: ${COL_TIME}ms"
            echo "  Improvement: ${IMPROVEMENT}%, Speedup: ${SPEEDUP}x"
            echo "  Completed at: $(date)"
            echo ""

            # Brief pause between tests to avoid resource contention
            sleep 2
        done
    done
done

echo "=== EXTENSIVE BENCHMARK COMPLETED ==="
echo "All $TOTAL_TESTS test cases completed!"
echo "Results saved to: $RESULTS_FILE"
echo ""

# Generate summary report
echo "=== PERFORMANCE SUMMARY ==="
echo ""

# Calculate averages by schema type
for schema in "${SCHEMA_TYPES[@]}"; do
    echo "--- $schema Scenario ---"
    grep "^$schema," $RESULTS_FILE | while IFS=, read -r s c r rt ct imp speed; do
        printf "  %2d cols, %7s rows: %5dms → %4dms (%5.1f%% improvement, %.2fx speedup)\n" \
               $c $r $rt $ct $imp $speed
    done
    echo ""
done

echo "Detailed results available in: $RESULTS_FILE"
