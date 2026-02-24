#!/bin/bash
set -e

INPUT_FILE=$1
OUTPUT_DB=${2:-output.db}
LAYER=${3:-}

if [ -z "$INPUT_FILE" ]; then
    echo "Usage: docker run ... <input_file> [output_db] [layer]"
    exit 1
fi

echo "Input file: $INPUT_FILE"
echo "Output DB: $OUTPUT_DB"

# Detect file type
EXT="${INPUT_FILE##*.}"

if [ "$EXT" = "geojson" ] || [ "$EXT" = "json" ]; then
    FORMAT="GeoJSON"
elif [ "$EXT" = "gpkg" ]; then
    FORMAT="GPKG"
else
    echo "Unsupported file format"
    exit 1
fi

# If layer not provided for GPKG, list layers
if [ "$FORMAT" = "GPKG" ] && [ -z "$LAYER" ]; then
    echo "Available layers:"
    ogrinfo "$INPUT_FILE"
    echo "Please specify layer name."
    exit 1
fi

# Default layer for GeoJSON
if [ "$FORMAT" = "GeoJSON" ]; then
    LAYER=$(ogrinfo "$INPUT_FILE" -ro -so | grep "Layer name:" | awk '{print $3}')
fi

echo "Importing layer: $LAYER"

if [ "$FORMAT" = "GPKG" ]; then
    if [ -z "$LAYER" ]; then
        echo "Available layers:"
        ogrinfo "$INPUT_FILE" | grep "1:"
        echo "Please specify layer name."
        exit 1
    fi

    ogr2ogr \
        -f SQLite "$OUTPUT_DB" "$INPUT_FILE" "$LAYER" \
        -dsco SPATIALITE=YES \
        -nln spatial_data \
        -gt 65536 \
        -progress

elif [ "$FORMAT" = "GeoJSON" ]; then

    ogr2ogr \
        -f SQLite "$OUTPUT_DB" "$INPUT_FILE" \
        -dsco SPATIALITE=YES \
        -nln spatial_data \
        -gt 65536 \
        -progress
fi

echo "Creating spatial index..."

sqlite3 "$OUTPUT_DB" <<EOF
SELECT load_extension('mod_spatialite');
EOF

echo "Done."