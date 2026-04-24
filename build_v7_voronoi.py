"""
Build non-overlapping territory polygons for V7 clusters.
Hybrid approach: Voronoi tessellation constrained by address convex hulls.
This ensures territories follow actual address assignments and don't cross highways.

RUN WITH: arcgispro-py3 (for scipy + arcpy)

Reads:  ofs_output/*_v7_hub_data.json  (centroids + per-hub addr_coords)
Writes: ofs_output/*_v7_voronoi.geojson
"""
import json, os, sys, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import numpy as np
from scipy.spatial import Voronoi, ConvexHull
import arcpy

BASE = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE, 'ofs_output')
SR_WGS84 = arcpy.SpatialReference(4326)

# Buffer distance around address convex hulls (degrees)
# ~500ft at 40N latitude — enough to fill gaps between addresses
HULL_BUFFER = 0.0015


def _make_arcpy_polygon(ring_lonlat):
    """Create arcpy Polygon from list of (lon, lat) tuples."""
    arr = arcpy.Array([arcpy.Point(lon, lat) for lon, lat in ring_lonlat])
    return arcpy.Polygon(arr, SR_WGS84)


def _make_arcpy_polyline(coords_lonlat):
    """Create arcpy Polyline from list of (lon, lat) tuples."""
    arr = arcpy.Array([arcpy.Point(lon, lat) for lon, lat in coords_lonlat])
    return arcpy.Polyline(arr, SR_WGS84)


def _ring_area(ring):
    """Compute signed area of a ring using the shoelace formula (in degrees^2)."""
    n = len(ring)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += ring[i][0] * ring[j][1]
        area -= ring[j][0] * ring[i][1]
    return abs(area) / 2.0


def _geom_to_geojson(geom):
    """Convert arcpy Polygon to GeoJSON geometry dict (Polygon or MultiPolygon).
    Handles multi-part results from difference operations.
    Filters out tiny sliver fragments."""
    parts = []
    for part in geom:
        ring = []
        for pt in part:
            if pt:
                ring.append([round(pt.X, 6), round(pt.Y, 6)])
        if ring:
            if ring[0] != ring[-1]:
                ring.append(ring[0])
            parts.append(ring)

    if not parts:
        return None

    # Filter out sliver fragments: drop parts with area < 1% of largest part
    if len(parts) > 1:
        areas = [_ring_area(p) for p in parts]
        max_area = max(areas)
        threshold = max(max_area * 0.01, 1e-8)
        parts = [p for p, a in zip(parts, areas) if a >= threshold]

    if not parts:
        return None
    elif len(parts) == 1:
        return {'type': 'Polygon', 'coordinates': [parts[0]]}
    else:
        return {'type': 'MultiPolygon', 'coordinates': [[p] for p in parts]}


def _convex_hull_polygon(points_lonlat):
    """Build a convex hull polygon from (lon, lat) points using scipy.
    Returns arcpy Polygon or None if < 3 points."""
    pts = np.array(points_lonlat)
    if len(pts) < 3:
        return None
    # Remove duplicate points
    pts = np.unique(pts, axis=0)
    if len(pts) < 3:
        return None
    try:
        hull = ConvexHull(pts)
        verts = [(float(pts[v][0]), float(pts[v][1])) for v in hull.vertices]
        verts.append(verts[0])  # close ring
        return _make_arcpy_polygon(verts)
    except Exception:
        return None


def voronoi_polygons(centroids, clip_ring, ofs_zone_rings=None,
                     barrier_lines=None, hub_addr_coords=None):
    """
    Generate territory polygons from hub centroids and address assignments.

    If hub_addr_coords is provided, uses hybrid approach:
      Voronoi cell ∩ buffered_convex_hull(addresses) — then subtract OFS + barriers.
    This ensures territories follow actual address assignments.

    Args:
        centroids: dict of hub_id -> (lat, lon)
        clip_ring: list of [lon, lat] -- outer boundary ring (GeoJSON order)
        ofs_zone_rings: list of [[lon, lat], ...] rings for OFS exclusion zones
        barrier_lines: list of [[lon, lat], ...] coordinate arrays for highway barriers
        hub_addr_coords: dict of hub_id -> [[lon, lat], ...] per-hub address coords

    Returns:
        dict of hub_id -> GeoJSON geometry dict (Polygon or MultiPolygon), or None
    """
    if len(centroids) < 2:
        hid = list(centroids.keys())[0]
        ring = list(clip_ring)
        if ring[0] != ring[-1]:
            ring.append(ring[0])
        return {hid: {'type': 'Polygon', 'coordinates': [ring]}}

    # Build WC boundary as arcpy Polygon
    wc_poly = _make_arcpy_polygon([(p[0], p[1]) for p in clip_ring])

    # Build merged OFS exclusion geometry (union of all OFS zones)
    ofs_union = None
    if ofs_zone_rings:
        for zr in ofs_zone_rings:
            if len(zr) < 3:
                continue
            try:
                zpoly = _make_arcpy_polygon([(p[0], p[1]) for p in zr])
                if ofs_union is None:
                    ofs_union = zpoly
                else:
                    ofs_union = ofs_union.union(zpoly)
            except Exception:
                pass
        if ofs_union is not None:
            try:
                ofs_union = ofs_union.buffer(0)
                ofs_union = ofs_union.generalize(0.00005)
            except Exception:
                pass

    # Build merged highway barrier geometry (buffered lines -> polygons)
    highway_union = None
    if barrier_lines:
        buf_dist = 0.0002  # ~73ft at 40N latitude
        for bl in barrier_lines:
            if len(bl) < 2:
                continue
            try:
                pline = _make_arcpy_polyline([(p[0], p[1]) for p in bl])
                buffered = pline.buffer(buf_dist)
                if highway_union is None:
                    highway_union = buffered
                else:
                    highway_union = highway_union.union(buffered)
            except Exception:
                pass
        if highway_union is not None:
            try:
                highway_union = highway_union.buffer(0)
                highway_union = highway_union.generalize(0.00005)
            except Exception:
                pass
        print(f"  Highway barrier union: {len(barrier_lines)} lines buffered")

    # Build per-hub address convex hulls (buffered)
    hub_hulls = {}
    if hub_addr_coords:
        for hid, coords in hub_addr_coords.items():
            if len(coords) >= 3:
                hull_poly = _convex_hull_polygon(coords)
                if hull_poly is not None:
                    hub_hulls[hid] = hull_poly.buffer(HULL_BUFFER)
        print(f"  Address hulls: {len(hub_hulls)}/{len(hub_addr_coords)} hubs with convex hulls")

    hub_ids = sorted(centroids.keys())
    # Voronoi in (x=lon, y=lat) space
    points = np.array([[centroids[hid][1], centroids[hid][0]] for hid in hub_ids])

    # Add far-away dummy points to bound all Voronoi regions
    xmin, xmax = points[:, 0].min(), points[:, 0].max()
    ymin, ymax = points[:, 1].min(), points[:, 1].max()
    buf = max(xmax - xmin, ymax - ymin, 0.05) * 3
    dummies = np.array([
        [xmin - buf, ymin - buf],
        [xmax + buf, ymin - buf],
        [xmax + buf, ymax + buf],
        [xmin - buf, ymax + buf],
    ])
    all_pts = np.vstack([points, dummies])

    vor = Voronoi(all_pts)

    result = {}
    for idx, hid in enumerate(hub_ids):
        region_idx = vor.point_region[idx]
        region = vor.regions[region_idx]

        if -1 in region or not region:
            result[hid] = None
            continue

        # Get Voronoi cell vertices as (lon, lat)
        verts = [(float(vor.vertices[v][0]), float(vor.vertices[v][1])) for v in region]
        verts.append(verts[0])

        try:
            cell_poly = _make_arcpy_polygon(verts)
            clipped = cell_poly.intersect(wc_poly, 4)  # clip to WC boundary

            if clipped is None or clipped.area == 0:
                result[hid] = None
                continue

            # Constrain to address convex hull (if available)
            if hid in hub_hulls:
                clipped = clipped.intersect(hub_hulls[hid], 4)
                if clipped is None or clipped.area == 0:
                    result[hid] = None
                    continue

            # Subtract OFS exclusion zones
            if ofs_union is not None:
                clipped = clipped.difference(ofs_union)
                if clipped is None or clipped.area == 0:
                    result[hid] = None
                    continue

            # Subtract highway barrier buffers
            if highway_union is not None:
                clipped = clipped.difference(highway_union)
                if clipped is None or clipped.area == 0:
                    result[hid] = None
                    continue

            result[hid] = _geom_to_geojson(clipped)
        except Exception:
            result[hid] = None

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Standalone: process hub data files
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    hub_files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith('_v7_hub_data.json'))
    if not hub_files:
        print("No *_v7_hub_data.json files found in ofs_output/.")
        sys.exit(1)

    # Load WC boundaries
    WC_BOUNDARY_PATH = r'C:\Users\v267429\Documents\claudework\wc_boundaries.geojson'
    with open(WC_BOUNDARY_PATH) as f:
        all_bounds = json.load(f)
    wc_rings = {}
    for feat in all_bounds['features']:
        clli = feat['properties'].get('CLLI', '')
        wc_rings[clli] = feat['geometry']['coordinates'][0]
    print(f"Loaded {len(wc_rings)} WC boundaries")

    for hf in hub_files:
        clli = hf.replace('_v7_hub_data.json', '')
        print(f"\n[{clli}] Generating territory polygons...")
        t0 = time.time()

        with open(os.path.join(OUTPUT_DIR, hf)) as f:
            hub_data = json.load(f)

        centroids = {int(h['hub_id']): (h['lat'], h['lon']) for h in hub_data['centroids']}
        ring = wc_rings.get(clli)
        if not ring:
            print(f"  No WC boundary for {clli}, skipping")
            continue

        # Load per-hub address coordinates (for hull-constrained territories)
        hub_addr_coords = {}
        for h in hub_data['centroids']:
            coords = h.get('addr_coords', [])
            if coords:
                hub_addr_coords[int(h['hub_id'])] = coords

        # Load OFS zone polygons for subtraction
        ofs_zones_path = os.path.join(OUTPUT_DIR, f'{clli}_ofs_zones.geojson')
        ofs_zone_rings = []
        if os.path.exists(ofs_zones_path):
            with open(ofs_zones_path) as f:
                ofs_gj = json.load(f)
            for feat in ofs_gj.get('features', []):
                geom = feat.get('geometry', {})
                if geom.get('type') == 'Polygon' and geom.get('coordinates'):
                    ofs_zone_rings.append(geom['coordinates'][0])
            print(f"  {len(ofs_zone_rings)} OFS zones to subtract")

        # Load highway barrier lines for subtraction
        barrier_path = os.path.join(OUTPUT_DIR, f'{clli}_barriers.geojson')
        barrier_lines = []
        if os.path.exists(barrier_path):
            with open(barrier_path) as f:
                bar_gj = json.load(f)
            for feat in bar_gj.get('features', []):
                geom = feat.get('geometry', {})
                if geom.get('type') == 'LineString' and geom.get('coordinates'):
                    barrier_lines.append(geom['coordinates'])
                elif geom.get('type') == 'MultiLineString' and geom.get('coordinates'):
                    barrier_lines.extend(geom['coordinates'])
            print(f"  {len(barrier_lines)} highway barrier lines to subtract")

        polys = voronoi_polygons(centroids, ring, ofs_zone_rings=ofs_zone_rings,
                                 barrier_lines=barrier_lines,
                                 hub_addr_coords=hub_addr_coords)
        good = sum(1 for v in polys.values() if v is not None)
        print(f"  {good}/{len(polys)} valid polygons")

        # Build GeoJSON
        features = []
        for hid, geom in polys.items():
            if geom is None:
                continue
            info = next((h for h in hub_data['centroids'] if h['hub_id'] == hid), {})
            features.append({
                'type': 'Feature',
                'geometry': geom,
                'properties': {
                    'hub_id': f'H{hid:04d}',
                    'color': info.get('color', '#ef4444'),
                    'units': info.get('units', 0),
                    'addresses': info.get('addresses', 0),
                    'avg_score': info.get('avg_score', 0),
                },
            })

        out_path = os.path.join(OUTPUT_DIR, f'{clli}_v7_voronoi.geojson')
        with open(out_path, 'w') as f:
            json.dump({'type': 'FeatureCollection', 'features': features}, f)

        print(f"  {len(features)} polygons in {time.time()-t0:.1f}s -> {os.path.basename(out_path)}")

    print("\nDone.")
