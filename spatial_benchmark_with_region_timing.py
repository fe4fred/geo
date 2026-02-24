#!/usr/bin/env python3

import argparse
import sqlite3
import time
import statistics
import os
from collections import defaultdict


def parse_line(line):
    gid0, lon, lat = line.strip().split(",")
    return gid0, float(lon), float(lat)

def load_coordinates(file_path):
    coords = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.lower().startswith("lon"):
                continue
            sline = line.split(",")
            cc = ""
            if (len(sline) == 2):
                lon, lat = float(sline[0]), float(sline[1])
            else:
                lon, lat, cc = float(sline[0]), float(sline[1]), sline[2]

            coords.append((lon, lat, cc))
    return coords


def connect_spatialite(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)

    try:
        conn.load_extension("mod_spatialite")
    except sqlite3.OperationalError:
        conn.load_extension("libspatialite")

    return conn


def benchmark(conn, table, region_column, coords, srid=4326, country_code=None):
    cursor = conn.cursor()

    global_times = []

    region_hits = defaultdict(int)
    region_times = defaultdict(list)

    outside_count = 0
    outside_times = []

    geometry = "geometry"
    #geometry = "geom"

    query1 = f"""
        SELECT {region_column}
        FROM {table}
        WHERE MbrContains(
                {geometry},
                MakePoint(?, ?, {srid})
        )
        AND ST_Contains(
                {geometry},
                MakePoint(?, ?, {srid})
        )
        LIMIT 1;
    """

    start_total = time.perf_counter()

    for lon, lat, cc in coords:
        if (cc == ""):
            query = f"""
                SELECT {region_column}
                FROM {table}
                WHERE MbrContains(
                    {geometry},
                    MakePoint(?, ?, {srid})
                )
                AND ST_Contains(
                    {geometry},
                    MakePoint(?, ?, {srid})
                )
                LIMIT 1;
            """
        else:
            CC = "'"+cc+"'"
            query = f"""
                SELECT {region_column}
                FROM {table}
                WHERE GID_0 = {CC}
                AND MbrContains(
                    {geometry},
                    MakePoint(?, ?, {srid})
                )
                AND ST_Contains(
                    {geometry},
                    MakePoint(?, ?, {srid})
                )
                LIMIT 1;
            """
        #print("SQL: " + query)
        t0 = time.perf_counter()
        cursor.execute(query, (lon, lat, lon, lat))
        result = cursor.fetchone()
        t1 = time.perf_counter()

        elapsed = t1 - t0
        global_times.append(elapsed)

        if result and result[0] is not None:
            region = result[0]
            region_hits[region] += 1
            region_times[region].append(elapsed)
        else:
            outside_count += 1
            outside_times.append(elapsed)

    end_total = time.perf_counter()

    total_time = end_total - start_total

    return {
        "global_times": global_times,
        "region_hits": region_hits,
        "region_times": region_times,
        "outside_count": outside_count,
        "outside_times": outside_times,
        "total_time": total_time,
        "count": len(coords),
    }


def print_results(results):
    global_times = results["global_times"]
    total_time = results["total_time"]
    count = results["count"]

    print("\n=== Global Performance ===")
    print(f"Total queries      : {count}")
    print(f"Total time (s)     : {total_time:.6f}")
    print(f"Average time (ms)  : {statistics.mean(global_times)*1000:.6f}")
    print(f"Std deviation (ms) : {statistics.stdev(global_times)*1000:.6f}")
    print(f"Min time (ms)      : {min(global_times)*1000:.6f}")
    print(f"Max time (ms)      : {max(global_times)*1000:.6f}")
    print(f"Queries/sec (QPS)  : {count / total_time:.2f}")

    print("\n=== Region Statistics ===")

    for region, hits in sorted(results["region_hits"].items(), key=lambda x: -x[1]):
        times = results["region_times"][region]
        avg = statistics.mean(times) * 1000
        std = statistics.stdev(times) * 1000 if len(times) > 1 else 0
        pct = (hits / count) * 100

        print(
            f"{region}: "
            f"{hits} hits ({pct:.2f}%) | "
            f"Avg: {avg:.6f} ms | "
            f"Std: {std:.6f} ms"
        )

    outside_count = results["outside_count"]
    if outside_count > 0:
        outside_avg = statistics.mean(results["outside_times"]) * 1000
        print(
            f"\nOutside all regions: "
            f"{outside_count} ({(outside_count/count)*100:.2f}%) | "
            f"Avg: {outside_avg:.6f} ms"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Spatial benchmark with per-region timing stats"
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

    print_results(results)


if __name__ == "__main__":
    main()
