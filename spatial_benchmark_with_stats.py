#!/usr/bin/env python3

import argparse
import sqlite3
import time
import statistics
import os
from collections import defaultdict


def load_coordinates(file_path):
    coords = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.lower().startswith("lon"):
                continue
            lon, lat = map(float, line.split(","))
            coords.append((lon, lat))
    return coords


def connect_spatialite(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)

    try:
        conn.load_extension("mod_spatialite")
    except sqlite3.OperationalError:
        conn.load_extension("libspatialite")

    return conn


def benchmark(conn, table, region_column, coords, srid=4326):
    cursor = conn.cursor()

    times = []
    region_hits = defaultdict(int)
    outside_count = 0

    query = f"""
        SELECT {region_column}
        FROM {table}
        WHERE MbrContains(
                geometry,
                MakePoint(?, ?, {srid})
        )
        AND ST_Contains(
                geometry,
                MakePoint(?, ?, {srid})
        )
        LIMIT 1;
    """

    start_total = time.perf_counter()

    for lon, lat in coords:
        t0 = time.perf_counter()
        cursor.execute(query, (lon, lat, lon, lat))
        result = cursor.fetchone()
        t1 = time.perf_counter()

        times.append(t1 - t0)

        if result and result[0] is not None:
            region_hits[result[0]] += 1
        else:
            outside_count += 1

    end_total = time.perf_counter()

    total_time = end_total - start_total
    avg_time = statistics.mean(times)
    stddev_time = statistics.stdev(times) if len(times) > 1 else 0.0
    min_time = min(times)
    max_time = max(times)
    qps = len(coords) / total_time if total_time > 0 else 0

    return {
        "total": total_time,
        "average": avg_time,
        "stddev": stddev_time,
        "min": min_time,
        "max": max_time,
        "qps": qps,
        "count": len(coords),
        "region_hits": region_hits,
        "outside": outside_count,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Spatial benchmark with region hit statistics"
    )
    parser.add_argument("database", help="SpatiaLite DB file")
    parser.add_argument("table", help="Table name")
    parser.add_argument("region_column", help="Column to aggregate by (e.g., NAME_1)")
    parser.add_argument("coords_file", help="File containing lon,lat per line")
    args = parser.parse_args()

    if not os.path.exists(args.database):
        raise FileNotFoundError("Database not found")

    if not os.path.exists(args.coords_file):
        raise FileNotFoundError("Coordinate file not found")

    coords = load_coordinates(args.coords_file)
    print(f"Loaded {len(coords)} coordinate points")

    conn = connect_spatialite(args.database)

    results = benchmark(conn, args.table, args.region_column, coords)

    conn.close()

    print("\n=== Performance Metrics ===")
    print(f"Total queries      : {results['count']}")
    print(f"Total time (s)     : {results['total']:.6f}")
    print(f"Average time (ms)  : {results['average'] * 1000:.6f}")
    print(f"Std deviation (ms) : {results['stddev'] * 1000:.6f}")
    print(f"Min time (ms)      : {results['min'] * 1000:.6f}")
    print(f"Max time (ms)      : {results['max'] * 1000:.6f}")
    print(f"Queries/sec (QPS)  : {results['qps']:.2f}")

    print("\n=== Region Hit Statistics ===")
    total_hits = sum(results["region_hits"].values())

    for region, count in sorted(results["region_hits"].items(), key=lambda x: -x[1]):
        percentage = (count / results['count']) * 100
        print(f"{region}: {count} hits ({percentage:.2f}%)")

    print(f"\nOutside all regions: {results['outside']} "
          f"({(results['outside'] / results['count']) * 100:.2f}%)")


if __name__ == "__main__":
    main()
