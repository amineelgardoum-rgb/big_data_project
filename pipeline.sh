#!/bin/bash

##############################################
# Daily Procurement Pipeline (Git Bash)
##############################################

# Get today's date (or use argument if provided)
if [ -z "$1" ]; then
    DATE=$(date +%Y-%m-%d)
else
    DATE=$1
fi

# Configuration - Windows paths work in Git Bash
OUTPUT_DIR="output/supplier_orders/${DATE}"
LOGS_DIR="logs/exceptions"
TEMP_DIR="tmp/procurement_${DATE}"

# Create directories
mkdir -p ${OUTPUT_DIR}
mkdir -p ${LOGS_DIR}
mkdir -p ${TEMP_DIR}

echo "=============================================="
echo "Procurement Pipeline - ${DATE}"
echo "=============================================="
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "[1/5] Creating the data and ingest Stage"
python main.py
echo ""

##############################################
# Step 1: Calculate Net Demand
##############################################
echo "[2/5] Calculating net demand..."

# Replace date in SQL file
sed "s/\${DATE}/${DATE}/g" ./sql/net_demand_query.sql > ${TEMP_DIR}/query_${DATE}.sql

# Execute query and save to JSONL
docker exec -i trino trino --execute "$(cat ${TEMP_DIR}/query_${DATE}.sql)" \
      --output-format JSON \
      > ${OUTPUT_DIR}/all_orders_${DATE}.jsonl

if [ $? -eq 0 ]; then
    TOTAL_ORDERS=$(wc -l < ${OUTPUT_DIR}/all_orders_${DATE}.jsonl | tr -d ' ')
    echo "✓ Net demand calculated: ${TOTAL_ORDERS} order lines"
else
    echo "✗ ERROR: Net demand calculation failed"
    exit 1
fi

##############################################
# Step 2: Split by Supplier
##############################################
echo "[3/5] Splitting orders by supplier..."

# Extract unique suppliers
SUPPLIERS=$(grep -o '"supplier_id":"[^"]*"' ${OUTPUT_DIR}/all_orders_${DATE}.jsonl | \
            cut -d'"' -f4 | sort -u)

SUPPLIER_COUNT=0
for SUPPLIER in $SUPPLIERS; do
    # Create file for each supplier
    grep "\"supplier_id\":\"${SUPPLIER}\"" ${OUTPUT_DIR}/all_orders_${DATE}.jsonl \
        > ${OUTPUT_DIR}/order_${SUPPLIER}_${DATE}.jsonl
    
    LINE_COUNT=$(wc -l < ${OUTPUT_DIR}/order_${SUPPLIER}_${DATE}.jsonl | tr -d ' ')
    echo "  ✓ ${SUPPLIER}: ${LINE_COUNT} items"
    SUPPLIER_COUNT=$((SUPPLIER_COUNT + 1))
done

echo "✓ Generated orders for ${SUPPLIER_COUNT} suppliers"

##############################################
# Step 3: Generate Exception Report
##############################################
echo "[4/5] Generating exception report..."

sed "s/\${DATE}/${DATE}/g" exception_check.sql > ${TEMP_DIR}/exceptions_${DATE}.sql

docker exec -i trino trino --execute "$(cat ${TEMP_DIR}/exceptions_${DATE}.sql)" \
      --output-format JSON \
      > ${LOGS_DIR}/${DATE}_exceptions.jsonl 2>/dev/null

if [ -f ${LOGS_DIR}/${DATE}_exceptions.jsonl ]; then
    TOTAL_EXCEPTIONS=$(wc -l < ${LOGS_DIR}/${DATE}_exceptions.jsonl | tr -d ' ')
    if [ ${TOTAL_EXCEPTIONS} -gt 0 ]; then
        echo "${TOTAL_EXCEPTIONS} exceptions detected"
    else
        echo " No exceptions found"
    fi
else
    echo "No exceptions found"
    # echo "" > ${LOGS_DIR}/${DATE}_exceptions.jsonl
    TOTAL_EXCEPTIONS=0
fi

##############################################
# Step 4: Summary Report
##############################################
echo "[5/5] Generating summary..."

SUMMARY_FILE="./summary_${DATE}.txt"

cat > ${SUMMARY_FILE} << EOF
================================================
PROCUREMENT PIPELINE SUMMARY
================================================
Date: ${DATE}
Execution Time: $(date '+%Y-%m-%d %H:%M:%S')

RESULTS:
--------
Total Order Lines: ${TOTAL_ORDERS}
Suppliers Processed: ${SUPPLIER_COUNT}
Exceptions Found: ${TOTAL_EXCEPTIONS}

OUTPUT FILES:
-------------
All Orders: ${OUTPUT_DIR}/all_orders_${DATE}.jsonl
Supplier Orders: ${OUTPUT_DIR}/order_*_${DATE}.jsonl
Exceptions: ${LOGS_DIR}/${DATE}_exceptions.jsonl

SUPPLIER BREAKDOWN:
-------------------
EOF

for SUPPLIER in $SUPPLIERS; do
    LINE_COUNT=$(wc -l < ${OUTPUT_DIR}/order_${SUPPLIER}_${DATE}.jsonl | tr -d ' ')
    printf "%-20s %5d items\n" "${SUPPLIER}:" "${LINE_COUNT}" >> ${SUMMARY_FILE}
done



cat ${SUMMARY_FILE}

# Cleanup
rm -rf ${TEMP_DIR}
# path to hdfs
# Variables inside the script
# Variables for exceptions
# Variables
EXCEPTION_FILE="$(pwd)/logs/exceptions/${DATE}_exceptions.jsonl"
TMP_EXCEPTION_DIR="/tmp/exceptions/${DATE}"
HDFS_EXCEPTION_DIR="/processed/exceptions/${DATE}"

# Ensure exception file exists
if [ ! -f ${EXCEPTION_FILE} ]; then
    echo "[]" > ${EXCEPTION_FILE}
fi

echo "Copying exception files to temporary container folder for HDFS..."

# Create tmp folder in container
docker exec namenode mkdir -p ${TMP_EXCEPTION_DIR}

# Copy file into container
docker cp "${EXCEPTION_FILE}" namenode:"${TMP_EXCEPTION_DIR}/"

# Create HDFS directory
docker exec namenode hdfs dfs -mkdir -p "${HDFS_EXCEPTION_DIR}"

# Put file into HDFS
docker exec namenode hdfs dfs -put -f "${TMP_EXCEPTION_DIR}/*" "${HDFS_EXCEPTION_DIR}/"

# Cleanup tmp folder
docker exec namenode rm -rf "${TMP_EXCEPTION_DIR}"

echo "The exception data is pushed to HDFS under ${HDFS_EXCEPTION_DIR}."


echo ""
echo "=============================================="
echo "Pipeline completed successfully!"
echo "End time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="