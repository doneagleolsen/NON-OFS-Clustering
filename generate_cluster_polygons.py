"""
Generate Convex-Hull Polygons for All Clusters

Reads v6_clusters_cache.json + all_nonofs_12m.csv, builds convex hull per cluster,
outputs GeoJSON with full metadata properties.

Usage:
    python generate_cluster_polygons.py
"""
import csv, json, math, os, sys, time
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m.csv')

# ILEC bounding box for filtering bad coordinates
BBOX = {'lat_min': 36.3, 'lat_max': 47.5, 'lon_min': -83.0, 'lon_max': -66.5}


def convex_hull(points):
    """Compute convex hull of 2D points using Graham scan.
    Returns list of (lon, lat) in counter-clockwise order.
    """
    if len(points) < 3:
        return points

    def cross(O, A, B):
        return (A[0] - O[0]) * (B[1] - O[1]) - (A[1] - O[1]) * (B[0] - O[0])

    points = sorted(set(points))
    if len(points) <= 2:
        return points

    # Build lower hull
    lower = []
    for p in points:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    # Build upper hull
    upper = []
    for p in reversed(points):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    return lower[:-1] + upper[:-1]


def generate_polygons():
    """Generate convex hull polygons for all clusters."""
    print("Loading clusters...", flush=True)
    with open(CLUSTER_CACHE) as f:
        clusters = json.load(f)
    print(f"  {len(clusters):,} clusters", flush=True)

    # Load address coordinates
    print("Loading address coordinates...", flush=True)
    addr_coords = {}
    n = 0
    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            laid = row['LOCUS_ADDRESS_ID']
            lat = float(row['LATITUDE'])
            lon = float(row['LONGITUDE'])
            # Filter bad coordinates
            if (BBOX['lat_min'] <= lat <= BBOX['lat_max'] and
                BBOX['lon_min'] <= lon <= BBOX['lon_max']):
                addr_coords[laid] = (lat, lon)
            n += 1
            if n % 3000000 == 0:
                print(f"  ...{n:,}", flush=True)
    print(f"  {len(addr_coords):,} addresses with valid coordinates", flush=True)

    # Build polygons
    print("Generating polygons...", flush=True)
    features = []
    skipped = 0
    point_features = 0  # clusters with < 3 unique coords -> point or line

    for i, c in enumerate(clusters):
        laids = c.get('addresses', [])
        points = []
        for laid in laids:
            coord = addr_coords.get(str(laid))
            if coord:
                points.append(coord)

        if not points:
            # Fall back to cluster centroid
            lat = c.get('lat', 0)
            lon = c.get('lon', 0)
            if (BBOX['lat_min'] <= lat <= BBOX['lat_max'] and
                BBOX['lon_min'] <= lon <= BBOX['lon_max']):
                points = [(lat, lon)]
            else:
                skipped += 1
                continue

        unique_points = list(set(points))

        # Properties for this cluster
        props = {
            'CLUSTER_ID': c['cluster_id'],
            'CLLI': c['clli'],
            'REGION': c.get('region', ''),
            'SUB_REGION': c.get('sub_region', ''),
            'MARKET_DENSITY': c.get('market_density', ''),
            'TOTAL_UNITS': c['total_units'],
            'TOTAL_ADDRS': c['total_addrs'],
            'TOTAL_CAPEX': round(c.get('total_capex', 0)),
            'AVG_CPP': round(c.get('avg_cpp', 0)),
            'MEDIAN_IRR': round(c.get('median_irr', 0), 2),
            'COPPER_CIRCUITS': c.get('copper_circuits', 0),
            'URGENCY_SCORE': c.get('urgency_score', 0),
            'VALUE_SCORE': c.get('value_score', 0),
            'BUILD_PRIORITY': c.get('build_priority_tier', ''),
            'TOP_OBLIGATION': c.get('top_obligation', ''),
            'OBLIG_FRACTION': round(c.get('obligation_fraction', 0), 3),
            'AVG_FORM_SCORE': round(c.get('avg_formation_score', 0), 1),
        }

        # Add AUI breakdown
        aui = c.get('aui_units', {})
        props['AUI_SFU'] = aui.get('SFU', 0)
        props['AUI_SBU'] = aui.get('SBU', 0)
        props['AUI_MDU'] = aui.get('MDU', 0)
        props['AUI_MTU'] = aui.get('MTU', 0)

        if len(unique_points) >= 3:
            # Convex hull -> polygon
            hull = convex_hull([(lon, lat) for lat, lon in unique_points])
            if len(hull) >= 3:
                # Close the ring
                ring = hull + [hull[0]]
                geometry = {
                    'type': 'Polygon',
                    'coordinates': [ring]
                }
            else:
                # Degenerate hull
                geometry = {
                    'type': 'Point',
                    'coordinates': [unique_points[0][1], unique_points[0][0]]
                }
                point_features += 1
        elif len(unique_points) == 2:
            # Line between two points
            geometry = {
                'type': 'LineString',
                'coordinates': [[lon, lat] for lat, lon in unique_points]
            }
            point_features += 1
        else:
            # Single point
            geometry = {
                'type': 'Point',
                'coordinates': [unique_points[0][1], unique_points[0][0]]
            }
            point_features += 1

        features.append({
            'type': 'Feature',
            'geometry': geometry,
            'properties': props,
        })

        if (i + 1) % 10000 == 0:
            print(f"  ...{i+1:,} polygons", flush=True)

    geojson = {
        'type': 'FeatureCollection',
        'features': features,
    }

    # Write GeoJSON
    out_path = os.path.join(OUT, 'all_nonofs_cluster_polygons.geojson')
    with open(out_path, 'w') as f:
        json.dump(geojson, f)

    file_size = os.path.getsize(out_path) / 1024 / 1024
    polygon_count = sum(1 for f in features if f['geometry']['type'] == 'Polygon')

    print(f"\n  Written: {out_path}", flush=True)
    print(f"  Total features: {len(features):,}", flush=True)
    print(f"  Polygons: {polygon_count:,}", flush=True)
    print(f"  Point/Line features: {point_features:,}", flush=True)
    print(f"  Skipped (bad coords): {skipped:,}", flush=True)
    print(f"  File size: {file_size:.1f} MB", flush=True)


if __name__ == '__main__':
    generate_polygons()
    print("\nDone.", flush=True)
