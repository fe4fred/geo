#!/usr/bin/env python3

import argparse
import sqlite3
import geopandas as gpd
import os
from shapely import wkb

def connect_spatialite(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)

    try:
        conn.load_extension("mod_spatialite")
    except sqlite3.OperationalError:
        # macOS alternative
        conn.load_extension("libspatialite")
    
    conn.execute("SELECT InitSpatialMetadata(1);")
    return conn


def create_table(conn, table_name, gdf, srid):
    cursor = conn.cursor()

    # Drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Create attribute table (without geometry first)
    columns = []
    for col, dtype in zip(gdf.columns, gdf.dtypes):
        if col == "geometry":
            continue
        if "int" in str(dtype):
            coltype = "INTEGER"
        elif "float" in str(dtype):
            coltype = "REAL"
        else:
            coltype = "TEXT"
        columns.append(f'"{col}" {coltype}')

    column_sql = ", ".join(columns)
    cursor.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {column_sql});")

    # Add geometry column
    geom_type = gdf.geometry.geom_type.unique()[0].upper()
    cursor.execute(
        f"SELECT AddGeometryColumn('{table_name}', 'geometry', {srid}, '{geom_type}', 'XY');"
    )

    conn.commit()


def insert_features(conn, table_name, gdf):
    cursor = conn.cursor()

    attribute_cols = [col for col in gdf.columns if col != "geometry"]
    placeholders = ", ".join(["?"] * len(attribute_cols))

    for _, row in gdf.iterrows():
        values = [row[col] for col in attribute_cols]
        geom_wkb = row.geometry.wkb

        cursor.execute(
            f"""
            INSERT INTO {table_name}
            ({", ".join(attribute_cols)}, geometry)
            VALUES ({placeholders}, GeomFromWKB(?, 4326));
            """,
            values + [geom_wkb],
        )

    conn.commit()


def create_spatial_index(conn, table_name):
    conn.execute(f"SELECT CreateSpatialIndex('{table_name}', 'geometry');")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Import GADM GeoJSON into SQLite SpatiaLite database"
    )
    parser.add_argument("geojson", help="Path to GADM GeoJSON file")
    parser.add_argument("database", help="Output SQLite database file")
    parser.add_argument("--table", default="gadm", help="Table name")
    args = parser.parse_args()

    if not os.path.exists(args.geojson):
        raise FileNotFoundError("GeoJSON file not found.")

    print("Reading GeoJSON...")
    gdf = gpd.read_file(args.geojson)

    if gdf.crs is None:
        print("No CRS detected. Assuming EPSG:4326")
        gdf.set_crs(epsg=4326, inplace=True)

    srid = 4326

    print("Connecting to database...")
    conn = connect_spatialite(args.database)

    print("Creating table...")
    create_table(conn, args.table, gdf, srid)

    print("Inserting features...")
    insert_features(conn, args.table, gdf)

    print("Creating spatial index...")
    create_spatial_index(conn, args.table)

    conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
