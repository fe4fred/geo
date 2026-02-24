docker run --rm -v $(pwd):/data spatialite-builder \
    gadm_410-levels.gpkg \
    global.db \
    ADM_1