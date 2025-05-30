#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
yaml_file="${SCRIPT_DIR}""/config.yaml"
argo_variants=(
  "PHY"
  "BGC"
)

# If variants are provided, use those instead
if [ $# -gt 0 ]; then
    # Clear default array
    argo_variants=()

    # Add each command-line argument to the array
    for arg in "$@"; do
        argo_variants+=("$arg")
    done
fi


# Loop through each variant of target CrocoLake directories
for var in "${argo_variants[@]}"; do

    IN_PATH=$(yq ".\"ARGO-GDAC_${var}\".input_path" "$yaml_file")
    IN_PATH=$(echo "$IN_PATH" | sed 's/^"//;s/"$//')
    IN_PATH=$(realpath "${CONFIG_DIR}/${IN_PATH}")

    OUT_PATH=$(yq ".\"ARGO-GDAC_${var}\".outdir_pq" "$yaml_file")
    OUT_PATH=$(echo "$OUT_PATH" | sed 's/^"//;s/"$//')
    OUT_PATH=$(realpath "${CONFIG_DIR}/${OUT_PATH}")

    echo "Processing $var"
    echo "Input path: $IN_PATH"
    echo "Output path: $OUT_PATH"

    echo "${var}|${IN_PATH}|${OUT_PATH}"

done
