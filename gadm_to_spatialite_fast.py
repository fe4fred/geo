#!/usr/bin/env python3

import argparse
import sqlite3
import fiona
import os
from shapely.geometry import shape
from shapely import wkb

BATCH_SIZE = 2000


def connect_spatialite(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)

    try:
        conn.load_extension("mod_spatialite")
    except sqlite3.OperationalError:
        conn.load_extension("libspatialite")

    cur = conn.cursor()

    # Performance PRAGMAs
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")  # ~200MB
    cur.execute("PRAGMA locking_mode=EXCLUSIVE;")

    cur.execute("SELECT InitSpatialMetadata(1);")

    conn.commit()
    return conn


def create_table(conn, table_name, schema, srid, geom_type):
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    columns = []
    for name, field_type in schema["properties"].items():
        if field_type.startswith("int"):
            sql_type = "INTEGER"
        elif field_type.startswith("float"):
            sql_type = "REAL"
        else:
            sql_type = "TEXT"
        columns.append(f'"{name}" {sql_type}')

    column_sql = ", ".join(columns)

    cur.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {column_sql}
        );
    """)

    cur.execute(
        f"SELECT AddGeometryColumn('{table_name}', 'geometry', {srid}, '{geom_type.upper()}', 'XY');"
    )

    conn.commit()


def bulk_insert(conn, table_name, features, property_names, srid):
    cur = conn.cursor()

    placeholders = ", ".join(["?"] * len(property_names))

    insert_sql = f"""
        INSERT INTO {table_name}
        ({", ".join(property_names)}, geometry)
        VALUES ({placeholders}, GeomFromWKB(?, {srid}));
    """

    batch = []
    count = 0

    for feature in features:
        geom = shape(feature["geometry"])
        geom_wkb = geom.wkb

        props = [feature["properties"].get(p) for p in property_names]

        batch.append(props + [geom_wkb])

        if len(batch) >= BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            conn.commit()
            count += len(batch)
            print(f"Inserted {count} features...")
            batch.clear()

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
        print(f"Inserted {count} features...")

    return count


def create_spatial_index(conn, table_name):
    conn.execute(f"SELECT CreateSpatialIndex('{table_name}', 'geometry');")
    conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Fast streaming GADM GeoJSON → SpatiaLite importer"
    )
    parser.add_argument("geojson", help="Path to GADM GeoJSON")
    parser.add_argument("database", help="SQLite output DB")
    parser.add_argument("--table", default="gadm", help="Table name")
    args = parser.parse_args()

    if not os.path.exists(args.geojson):
        raise FileNotFoundError("GeoJSON not found")

    print("Opening GeoJSON stream...")
    with fiona.open(args.geojson) as src:

        srid = 4326
        geom_type = src.schema["geometry"]
        property_names = list(src.schema["properties"].keys())

        conn = connect_spatialite(args.database)

        print("Creating table...")
        create_table(conn, args.table, src.schema, srid, geom_type)

        print("Bulk inserting features...")
        total = bulk_insert(conn, args.table, src, property_names, srid)

        print("Creating spatial index...")
        create_spatial_index(conn, args.table)

        conn.close()

    print(f"Done. Total features inserted: {total}")


if __name__ == "__main__":
    main()
