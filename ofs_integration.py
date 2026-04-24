"""
ofs_integration.py — OFS Hub & Address Integration Module
=========================================================
Phase 1 of FiOS Network Planner: Import real OFS hubs + addresses as frozen
clusters. NON-OFS clustering builds AROUND them, not through them.

Key functions:
  load_ofs_hubs(clli)              — Query IVAPP hub inventory
  load_ofs_addresses(clli)         — Query OFS addresses from Oracle
  assign_addresses_to_hubs(...)    — Nearest-hub by Haversine
  build_ofs_exclusion_zones(...)   — Convex hull + buffer per OFS hub
  detect_infill_opportunities(...) — Find "donut holes" surrounded by OFS
  get_ofs_data_for_wc(clli)        — All-in-one: load → assign → zone → infill

Usage:
  python ofs_integration.py LGNRPALI           # single WC
  python ofs_integration.py LGNRPALI NYCRNYWS  # multiple WCs
  python ofs_integration.py --all              # all ILEC WCs with OFS hubs
"""

import math
import sys
import os
import json
import csv
from collections import defaultdict

# ---------------------------------------------------------------------------
# Oracle connection
# ---------------------------------------------------------------------------
DSN = "f1btpap-scan.verizon.com:1521/NARPROD"
USER = "tableau_user"
PASS = "Verizon1#"

def get_connection():
    import oracledb
    return oracledb.connect(user=USER, password=PASS, dsn=DSN)

# ---------------------------------------------------------------------------
# Haversine (returns feet)
# ---------------------------------------------------------------------------
EARTH_RADIUS_FT = 20902231

def haversine_ft(lat1, lon1, lat2, lon2):
    """Haversine distance in feet between two lat/lon points."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return EARTH_RADIUS_FT * 2 * math.asin(math.sqrt(a))

# ---------------------------------------------------------------------------
# 1. Load OFS hubs from IF_IVAPP_HUB_CAP
# ---------------------------------------------------------------------------
def load_ofs_hubs(clli, conn=None):
    """
    Query real FiOS hub inventory from IVAPP for a wire center.
    Returns list of dicts with hub metadata.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    sql = """
        SELECT CLLI_CODE, HUB_NAME, HUB_LOCATION, LATITUDE, LONGITUDE,
               FDH_SIZE, SLOTS,
               NVL(BPON_WORKING, 0) AS BPON_WORKING,
               NVL(GPON_WORKING, 0) AS GPON_WORKING,
               NVL(SESGPON_WORKING, 0) AS SESGPON_WORKING,
               NVL(BPON_UNASSIGNED, 0) AS BPON_UNASSIGNED,
               NVL(GPON_UNASSIGNED, 0) AS GPON_UNASSIGNED
        FROM GPSAA.IF_IVAPP_HUB_CAP
        WHERE CLLI_CODE = :clli
        ORDER BY HUB_NAME
    """
    cur = conn.cursor()
    cur.execute(sql, {'clli': clli})
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    cur.close()

    hubs = []
    for row in rows:
        d = dict(zip(cols, row))
        # Skip hubs without coordinates
        if d['LATITUDE'] is None or d['LONGITUDE'] is None:
            continue
        d['TOTAL_WORKING'] = d['BPON_WORKING'] + d['GPON_WORKING'] + d['SESGPON_WORKING']
        d['TOTAL_SPARE'] = d['BPON_UNASSIGNED'] + d['GPON_UNASSIGNED']
        d['TOTAL_PORTS'] = d['TOTAL_WORKING'] + d['TOTAL_SPARE']
        hubs.append(d)

    if close_conn:
        conn.close()

    return hubs

# ---------------------------------------------------------------------------
# 2. Load OFS addresses
# ---------------------------------------------------------------------------
def load_ofs_addresses(clli, conn=None):
    """
    Query OFS addresses for a wire center from Oracle.
    Returns list of dicts with address metadata.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    # OFS addresses are NOT in S_BEYOND_INFINITY_RANKING_SCORING (BI only scores NON-OFS).
    # Query directly from G_ALL_LOCUS_ADDRESS_KEY. Left-join BI for any that happen to exist.
    sql = """
        SELECT ak.LOCUS_ADDRESS_ID,
               ak.LATITUDE, ak.LONGITUDE,
               ak.AUI, ak.NO_OF_UNITS,
               ak.CATEGORY,
               ak.ODN_FLAG, ak.PP_IND,
               ak.COPPER_IND, ak.FIOS_IND,
               ak.ADDR_LINE_1, ak.CITY, ak.STATE,
               r.CPO_PRED, r.CPO_NTAS, r.NTAS_CNT,
               r.PRIORITY_RANK, r.MARKET_DENSITY
        FROM GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
        LEFT JOIN GPSAA.S_BEYOND_INFINITY_RANKING_SCORING r
            ON ak.LOCUS_ADDRESS_ID = r.LOCUS_ADDRESS_ID
        WHERE ak.CLLI = :clli
          AND ak.CATEGORY IN ('OFS', 'MIXED_OFS')
          AND ak.LATITUDE IS NOT NULL
          AND ak.LONGITUDE IS NOT NULL
    """
    cur = conn.cursor()
    cur.execute(sql, {'clli': clli})
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    cur.close()

    addresses = []
    for row in rows:
        d = dict(zip(cols, row))
        d['NO_OF_UNITS'] = d['NO_OF_UNITS'] or 1
        d['NTAS_CNT'] = d.get('NTAS_CNT') or 1
        addresses.append(d)

    if close_conn:
        conn.close()

    return addresses

# ---------------------------------------------------------------------------
# 2b. Load ALL addresses (OFS + NON-OFS) for a WC
# ---------------------------------------------------------------------------
def load_all_addresses(clli, conn=None):
    """
    Query ALL addresses (OFS + NON-OFS) for a wire center.
    Returns list of dicts. Each has IS_OFS boolean.
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    # Use G_ALL_LOCUS as base (has ALL addresses), left-join BI scoring (NON-OFS only)
    sql = """
        SELECT ak.LOCUS_ADDRESS_ID,
               ak.LATITUDE, ak.LONGITUDE,
               NVL(r.AUI, ak.AUI) AS AUI,
               NVL(r.NO_OF_UNITS, ak.NO_OF_UNITS) AS NO_OF_UNITS,
               r.NTAS_CNT,
               r.CPO_PRED, r.CPO_NTAS,
               r.PRIORITY_RANK, r.IRR_GROUP,
               r.COPPER_CIR_COUNT,
               NVL(r.MARKET_DENSITY, 'UNKNOWN') AS MARKET_DENSITY,
               ak.CATEGORY,
               ak.ODN_FLAG, ak.PP_IND
        FROM GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
        LEFT JOIN GPSAA.S_BEYOND_INFINITY_RANKING_SCORING r
            ON ak.LOCUS_ADDRESS_ID = r.LOCUS_ADDRESS_ID
        WHERE ak.CLLI = :clli
          AND ak.LATITUDE IS NOT NULL
          AND ak.LONGITUDE IS NOT NULL
    """
    cur = conn.cursor()
    cur.execute(sql, {'clli': clli})
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    cur.close()

    addresses = []
    for row in rows:
        d = dict(zip(cols, row))
        d['NO_OF_UNITS'] = d['NO_OF_UNITS'] or 1
        d['NTAS_CNT'] = d.get('NTAS_CNT') or 1
        d['IS_OFS'] = (d['CATEGORY'] in ('OFS', 'MIXED_OFS'))
        addresses.append(d)

    if close_conn:
        conn.close()

    return addresses

# ---------------------------------------------------------------------------
# 3. Assign addresses to nearest hub (Haversine)
# ---------------------------------------------------------------------------
def assign_addresses_to_hubs(addresses, hubs):
    """
    Assign each address to its nearest hub by Haversine distance.
    Mutates each address dict: adds HUB_NAME, DIST_TO_HUB_FT.
    Returns dict of hub_name → list of assigned addresses.
    """
    hub_assignments = defaultdict(list)

    for addr in addresses:
        best_hub = None
        best_dist = float('inf')

        for hub in hubs:
            d = haversine_ft(addr['LATITUDE'], addr['LONGITUDE'],
                             hub['LATITUDE'], hub['LONGITUDE'])
            if d < best_dist:
                best_dist = d
                best_hub = hub['HUB_NAME']

        addr['HUB_NAME'] = best_hub
        addr['DIST_TO_HUB_FT'] = best_dist
        if best_hub:
            hub_assignments[best_hub].append(addr)

    return hub_assignments

# ---------------------------------------------------------------------------
# 4. Build OFS exclusion zones (convex hull + buffer)
# ---------------------------------------------------------------------------
def _convex_hull(points):
    """
    Compute convex hull of 2D points using Andrew's monotone chain.
    Returns list of (lon, lat) in counter-clockwise order.
    """
    pts = sorted(set(points))
    if len(pts) <= 1:
        return pts
    if len(pts) == 2:
        return pts

    # Build lower hull
    lower = []
    for p in pts:
        while len(lower) >= 2 and _cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    # Build upper hull
    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and _cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    return lower[:-1] + upper[:-1]


def _cross(o, a, b):
    """2D cross product of vectors OA and OB."""
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _buffer_polygon(hull, buffer_ft=200):
    """
    Expand a convex hull outward by buffer_ft.
    Simple approach: offset each vertex outward from centroid.
    """
    if not hull:
        return hull

    cx = sum(p[0] for p in hull) / len(hull)
    cy = sum(p[1] for p in hull) / len(hull)

    # Convert buffer_ft to approximate degrees (~1 degree ≈ 364,000 ft at 40°N lat)
    buffer_deg = buffer_ft / 364000.0

    buffered = []
    for lon, lat in hull:
        dx = lon - cx
        dy = lat - cy
        dist = math.sqrt(dx * dx + dy * dy)
        if dist < 1e-10:
            buffered.append((lon, lat))
        else:
            scale = (dist + buffer_deg) / dist
            buffered.append((cx + dx * scale, cy + dy * scale))

    return buffered


def build_ofs_exclusion_zones(hubs, hub_assignments, buffer_ft=200):
    """
    Build convex hull exclusion zone per OFS hub.

    Returns list of dicts:
      {hub_name, hub_lat, hub_lon, fdh_size, total_working, total_spare,
       addr_count, unit_count, hull: [(lon,lat),...], centroid: (lon,lat)}
    """
    # Build hub lookup
    hub_lookup = {h['HUB_NAME']: h for h in hubs}

    zones = []
    for hub_name, addrs in hub_assignments.items():
        if not addrs:
            continue

        hub = hub_lookup.get(hub_name, {})
        points = [(a['LONGITUDE'], a['LATITUDE']) for a in addrs]

        # Add hub location itself
        if hub.get('LONGITUDE') and hub.get('LATITUDE'):
            points.append((hub['LONGITUDE'], hub['LATITUDE']))

        hull = _convex_hull(points)
        buffered = _buffer_polygon(hull, buffer_ft)

        # Centroid
        cx = sum(p[0] for p in points) / len(points)
        cy = sum(p[1] for p in points) / len(points)

        # AUI breakdown
        aui_counts = defaultdict(int)
        total_units = 0
        for a in addrs:
            aui = a.get('AUI', 'SFU')
            units = a.get('NO_OF_UNITS', 1)
            aui_counts[aui] += units
            total_units += units

        zones.append({
            'hub_name': hub_name,
            'hub_lat': hub.get('LATITUDE'),
            'hub_lon': hub.get('LONGITUDE'),
            'fdh_size': hub.get('FDH_SIZE'),
            'total_working': hub.get('TOTAL_WORKING', 0),
            'total_spare': hub.get('TOTAL_SPARE', 0),
            'addr_count': len(addrs),
            'unit_count': total_units,
            'aui': dict(aui_counts),
            'hull': buffered,
            'centroid': (cx, cy),
            'median_dist_ft': sorted([a['DIST_TO_HUB_FT'] for a in addrs])[len(addrs) // 2] if addrs else 0,
        })

    return zones

# ---------------------------------------------------------------------------
# 5. Point-in-polygon test (for exclusion zone checking)
# ---------------------------------------------------------------------------
def _point_in_polygon(lon, lat, polygon):
    """
    Ray-casting algorithm. polygon is list of (lon, lat).
    """
    n = len(polygon)
    if n < 3:
        return False

    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i

    return inside


def filter_nonofs_from_exclusion(nonofs_addresses, exclusion_zones):
    """
    Tag NON-OFS addresses that fall inside OFS exclusion zones.
    Mutates each address: adds IN_OFS_ZONE (bool), OFS_HUB_NAME (str or None).
    Returns count of excluded addresses.
    """
    excluded = 0
    for addr in nonofs_addresses:
        addr['IN_OFS_ZONE'] = False
        addr['OFS_HUB_NAME'] = None

        for zone in exclusion_zones:
            if _point_in_polygon(addr['LONGITUDE'], addr['LATITUDE'], zone['hull']):
                addr['IN_OFS_ZONE'] = True
                addr['OFS_HUB_NAME'] = zone['hub_name']
                excluded += 1
                break

    return excluded

# ---------------------------------------------------------------------------
# 6. Detect infill "donut hole" opportunities
# ---------------------------------------------------------------------------
def detect_infill_opportunities(exclusion_zones, nonofs_addresses, max_dist_ft=2000):
    """
    Find NON-OFS addresses that are surrounded by OFS zones (donut holes).
    These are low-marginal-cost infill opportunities because feeder fiber
    already passes through.

    A "donut hole" address is one where:
      - It is NOT inside any OFS zone (already filtered out)
      - Its nearest 2+ OFS hubs are within max_dist_ft
      - It's "surrounded" = average angle to nearest 3 hubs > 120° spread

    Returns list of infill opportunity dicts.
    """
    # Build zone centroid list for distance checks
    zone_centroids = []
    for z in exclusion_zones:
        zone_centroids.append({
            'hub_name': z['hub_name'],
            'lat': z['centroid'][1],
            'lon': z['centroid'][0],
        })

    infill = []
    for addr in nonofs_addresses:
        if addr.get('IN_OFS_ZONE'):
            continue

        # Find distances to all OFS hub centroids
        distances = []
        for zc in zone_centroids:
            d = haversine_ft(addr['LATITUDE'], addr['LONGITUDE'],
                             zc['lat'], zc['lon'])
            distances.append((d, zc['hub_name'], zc['lat'], zc['lon']))

        distances.sort()

        # Need at least 2 nearby OFS hubs
        nearby = [d for d in distances if d[0] <= max_dist_ft]
        if len(nearby) < 2:
            continue

        # Check angular spread — is the address "surrounded"?
        if len(nearby) >= 3:
            angles = []
            for _, _, hlat, hlon in nearby[:4]:
                angle = math.atan2(hlon - addr['LONGITUDE'],
                                   hlat - addr['LATITUDE'])
                angles.append(angle)
            angles.sort()

            # Max angular gap — if < 180°, address is surrounded
            max_gap = 0
            for i in range(len(angles)):
                gap = angles[(i + 1) % len(angles)] - angles[i]
                if gap < 0:
                    gap += 2 * math.pi
                max_gap = max(max_gap, gap)

            is_surrounded = max_gap < math.pi
        else:
            # 2 nearby hubs on opposite sides is still an infill opportunity
            is_surrounded = True

        if is_surrounded:
            infill.append({
                'LOCUS_ADDRESS_ID': addr['LOCUS_ADDRESS_ID'],
                'LATITUDE': addr['LATITUDE'],
                'LONGITUDE': addr['LONGITUDE'],
                'AUI': addr.get('AUI', 'SFU'),
                'NO_OF_UNITS': addr.get('NO_OF_UNITS', 1),
                'CPO_PRED': addr.get('CPO_PRED'),
                'nearest_ofs_hubs': [(d[1], round(d[0])) for d in nearby[:3]],
                'nearest_dist_ft': round(nearby[0][0]),
                'num_nearby_hubs': len(nearby),
            })

    return infill

# ---------------------------------------------------------------------------
# 7. All-in-one: get complete OFS data for a wire center
# ---------------------------------------------------------------------------
def get_ofs_data_for_wc(clli, buffer_ft=200, infill_max_dist_ft=2000):
    """
    All-in-one function: load hubs, load addresses, assign, build zones,
    detect infill opportunities.

    Returns dict:
      {clli, hubs, addresses, hub_assignments, exclusion_zones,
       nonofs_in_zone_count, infill_opportunities, stats}
    """
    conn = get_connection()

    print(f"[{clli}] Loading OFS hubs...")
    hubs = load_ofs_hubs(clli, conn)
    print(f"  > {len(hubs)} hubs")

    print(f"[{clli}] Loading OFS addresses...")
    ofs_addrs = load_ofs_addresses(clli, conn)
    print(f"  >{len(ofs_addrs)} OFS addresses")

    if not hubs and not ofs_addrs:
        conn.close()
        return {
            'clli': clli,
            'hubs': hubs,
            'addresses': ofs_addrs,
            'nonofs_addresses': [],
            'hub_assignments': {},
            'exclusion_zones': [],
            'nonofs_in_zone_count': 0,
            'infill_opportunities': [],
            'stats': {
                'hub_count': len(hubs), 'ofs_addr_count': len(ofs_addrs),
                'ofs_unit_count': 0, 'nonofs_addr_count': 0,
                'nonofs_unit_count': 0, 'zones_count': 0,
                'nonofs_in_zones': 0, 'infill_count': 0,
            },
        }

    print(f"[{clli}] Assigning addresses to hubs...")
    hub_assignments = assign_addresses_to_hubs(ofs_addrs, hubs)

    print(f"[{clli}] Building exclusion zones...")
    zones = build_ofs_exclusion_zones(hubs, hub_assignments, buffer_ft)

    # Load NON-OFS addresses to check exclusion + infill
    print(f"[{clli}] Loading NON-OFS addresses for exclusion check...")
    all_addrs = load_all_addresses(clli, conn)
    nonofs_addrs = [a for a in all_addrs if not a['IS_OFS']]
    print(f"  >{len(nonofs_addrs)} NON-OFS addresses")

    conn.close()

    excluded = filter_nonofs_from_exclusion(nonofs_addrs, zones)
    print(f"  >{excluded} NON-OFS addresses inside OFS zones")

    print(f"[{clli}] Detecting infill opportunities...")
    infill = detect_infill_opportunities(zones, nonofs_addrs, infill_max_dist_ft)
    print(f"  >{len(infill)} infill (donut hole) addresses")

    # Compute summary stats
    total_ofs_units = sum(a.get('NO_OF_UNITS', 1) for a in ofs_addrs)
    total_nonofs_units = sum(a.get('NO_OF_UNITS', 1) for a in nonofs_addrs)
    hub_stats = []
    for z in zones:
        hub_stats.append({
            'hub_name': z['hub_name'],
            'fdh_size': z['fdh_size'],
            'addrs': z['addr_count'],
            'units': z['unit_count'],
            'working_ports': z['total_working'],
            'spare_ports': z['total_spare'],
            'median_dist_ft': round(z['median_dist_ft']),
        })

    stats = {
        'hub_count': len(hubs),
        'ofs_addr_count': len(ofs_addrs),
        'ofs_unit_count': total_ofs_units,
        'nonofs_addr_count': len(nonofs_addrs),
        'nonofs_unit_count': total_nonofs_units,
        'zones_count': len(zones),
        'nonofs_in_zones': excluded,
        'infill_count': len(infill),
        'hub_details': hub_stats,
    }

    return {
        'clli': clli,
        'hubs': hubs,
        'addresses': ofs_addrs,
        'nonofs_addresses': nonofs_addrs,
        'hub_assignments': hub_assignments,
        'exclusion_zones': zones,
        'nonofs_in_zone_count': excluded,
        'infill_opportunities': infill,
        'stats': stats,
    }

# ---------------------------------------------------------------------------
# 8. Export to GeoJSON for visual verification
# ---------------------------------------------------------------------------
def export_geojson(data, output_dir):
    """
    Export OFS data to GeoJSON files for map overlay.
    Creates:
      {clli}_ofs_hubs.geojson       — hub point markers
      {clli}_ofs_zones.geojson      — exclusion zone polygons
      {clli}_ofs_infill.geojson     — infill opportunity points
    """
    clli = data['clli']
    os.makedirs(output_dir, exist_ok=True)

    # Hub points
    hub_features = []
    for hub in data['hubs']:
        hub_features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [hub['LONGITUDE'], hub['LATITUDE']],
            },
            'properties': {
                'hub_name': hub['HUB_NAME'],
                'fdh_size': hub.get('FDH_SIZE'),
                'working': hub.get('TOTAL_WORKING', 0),
                'spare': hub.get('TOTAL_SPARE', 0),
            },
        })

    with open(os.path.join(output_dir, f'{clli}_ofs_hubs.geojson'), 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': hub_features}, f)

    # Exclusion zone polygons
    zone_features = []
    for z in data['exclusion_zones']:
        if len(z['hull']) < 3:
            continue
        # Close the ring
        ring = list(z['hull']) + [z['hull'][0]]
        zone_features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [ring],
            },
            'properties': {
                'hub_name': z['hub_name'],
                'fdh_size': z['fdh_size'],
                'addr_count': z['addr_count'],
                'unit_count': z['unit_count'],
                'working': z['total_working'],
                'spare': z['total_spare'],
                'median_dist_ft': round(z['median_dist_ft']),
                'aui': z['aui'],
            },
        })

    with open(os.path.join(output_dir, f'{clli}_ofs_zones.geojson'), 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': zone_features}, f)

    # Infill opportunity points
    infill_features = []
    for inf in data['infill_opportunities']:
        infill_features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [inf['LONGITUDE'], inf['LATITUDE']],
            },
            'properties': {
                'address_id': str(inf['LOCUS_ADDRESS_ID']),
                'aui': inf['AUI'],
                'units': inf['NO_OF_UNITS'],
                'cpp': inf.get('CPO_PRED'),
                'nearest_dist_ft': inf['nearest_dist_ft'],
                'nearby_hubs': inf['num_nearby_hubs'],
                'nearest_hubs': inf['nearest_ofs_hubs'],
            },
        })

    with open(os.path.join(output_dir, f'{clli}_ofs_infill.geojson'), 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': infill_features}, f)

    print(f"[{clli}] Exported GeoJSON to {output_dir}/")
    print(f"  >{len(hub_features)} hubs, {len(zone_features)} zones, {len(infill_features)} infill points")

# ---------------------------------------------------------------------------
# 9. Print summary report
# ---------------------------------------------------------------------------
def print_summary(data):
    """Print a formatted summary of OFS integration results."""
    s = data['stats']
    clli = data['clli']

    print(f"\n{'='*60}")
    print(f"  OFS Integration Summary: {clli}")
    print(f"{'='*60}")
    print(f"  OFS Hubs:           {s['hub_count']:>8,}")
    print(f"  OFS Addresses:      {s['ofs_addr_count']:>8,}")
    print(f"  OFS Units:          {s['ofs_unit_count']:>8,}")
    print(f"  NON-OFS Addresses:  {s['nonofs_addr_count']:>8,}")
    print(f"  NON-OFS Units:      {s['nonofs_unit_count']:>8,}")
    print(f"  Exclusion Zones:    {s['zones_count']:>8,}")
    print(f"  NON-OFS in Zones:   {s['nonofs_in_zones']:>8,}  (will be excluded from clustering)")
    print(f"  Infill Donut Holes: {s['infill_count']:>8,}  (low-cost fill opportunities)")
    print(f"{'='*60}")

    if s.get('hub_details'):
        print(f"\n  Top Hubs by Address Count:")
        print(f"  {'Hub':<20} {'FDH':>6} {'Addrs':>7} {'Units':>7} {'Work':>6} {'Spare':>6} {'Med Dist':>9}")
        print(f"  {'-'*20} {'-'*6} {'-'*7} {'-'*7} {'-'*6} {'-'*6} {'-'*9}")
        sorted_hubs = sorted(s['hub_details'], key=lambda x: -x['addrs'])
        for h in sorted_hubs[:15]:
            print(f"  {h['hub_name']:<20} {h['fdh_size'] or '':>6} {h['addrs']:>7,} "
                  f"{h['units']:>7,} {h['working_ports']:>6} {h['spare_ports']:>6} "
                  f"{h['median_dist_ft']:>8,} ft")

    if data['infill_opportunities']:
        print(f"\n  Infill Opportunities (closest to OFS):")
        sorted_infill = sorted(data['infill_opportunities'], key=lambda x: x['nearest_dist_ft'])
        for inf in sorted_infill[:10]:
            hubs_str = ', '.join(f"{h[0]}({h[1]}ft)" for h in inf['nearest_ofs_hubs'][:2])
            print(f"    {inf['AUI']:<4} {inf['NO_OF_UNITS']:>3}u  "
                  f"nearest: {inf['nearest_dist_ft']:>5,}ft  "
                  f"hubs: {hubs_str}")

    print()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ofs_integration.py <CLLI> [CLLI2 ...] [--all]")
        print("       python ofs_integration.py LGNRPALI")
        print("       python ofs_integration.py LGNRPALI NYCRNYWS NYCKNY77")
        sys.exit(1)

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ofs_output')

    cllis = [a for a in sys.argv[1:] if not a.startswith('--')]

    if '--all' in sys.argv:
        # Query all WCs that have OFS hubs
        print("Querying all WCs with OFS hubs...")
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT CLLI_CODE FROM GPSAA.IF_IVAPP_HUB_CAP ORDER BY CLLI_CODE")
        cllis = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"Found {len(cllis)} WCs with OFS hubs")

    for clli in cllis:
        try:
            data = get_ofs_data_for_wc(clli)
            print_summary(data)
            export_geojson(data, output_dir)
        except Exception as e:
            print(f"[{clli}] ERROR: {e}")
            import traceback
            traceback.print_exc()

    print(f"\nDone. GeoJSON files in: {output_dir}/")
