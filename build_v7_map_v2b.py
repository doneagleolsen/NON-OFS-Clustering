"""
V7 Clustering Map v2b — Non-overlapping Voronoi + Address-Level Detail
======================================================================
3-step pipeline:
  Step 1 (this script, system python): Oracle data + V7 clustering + export JSONs
  Step 2 (build_v7_voronoi.py, arcgispro-py3): scipy Voronoi → GeoJSON polygons
  Step 3 (this script continued): assemble final HTML map

Usage:
  python build_v7_map_v2b.py           # runs step 1 + 3 (step 2 called via subprocess)
"""
import sys, os, json, time, csv, copy, math, subprocess
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
OUTPUT_DIR = os.path.join(BASE, 'ofs_output')
TIGER_DIR = os.path.join(BASE, 'TIGER')
os.makedirs(OUTPUT_DIR, exist_ok=True)

ARCGIS_PYTHON = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe'

TEST_WCS = [
    {'clli': 'LGNRPALI', 'name': 'Ligonier PA',    'morph': 'RURAL',    'fips': '42129'},
    {'clli': 'ARTNVAAR', 'name': 'Arlington VA',    'morph': 'URBAN',    'fips': '51013'},
    {'clli': 'SPFDVASP', 'name': 'Springfield VA',  'morph': 'SUBURBAN', 'fips': '51059'},
    {'clli': 'CLFDPACL', 'name': 'Clearfield PA',   'morph': 'RURAL',    'fips': '42033'},
]

from ofs_integration import get_ofs_data_for_wc, _convex_hull
from telecom_clustering_v7 import cluster_addresses_v7
from telecom_clustering_v5 import get_morphology_params
from road_graph import RoadGraph

# ═══════════════════════════════════════════════════════════════════════════
# Load WC boundaries
# ═══════════════════════════════════════════════════════════════════════════
WC_BOUNDARY_PATH = r'C:\Users\v267429\Documents\claudework\wc_boundaries.geojson'
print("Loading WC boundaries...")
with open(WC_BOUNDARY_PATH) as f:
    all_bounds = json.load(f)

wc_polys = {}
wc_boundary_features = {}
for feat in all_bounds.get('features', []):
    clli = feat.get('properties', {}).get('CLLI', '')
    if clli in [w['clli'] for w in TEST_WCS]:
        wc_polys[clli] = feat['geometry']['coordinates'][0]
        wc_boundary_features[clli] = feat
print(f"  Loaded boundaries for: {list(wc_polys.keys())}")


def point_in_wc(lat, lon, ring):
    n = len(ring)
    inside = False
    x, y = lon, lat
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def validate_clustering(clli, v7_centroids, v7_rg, road_graph):
    """Self-test: detect barrier violations before map generation."""
    # --- Test 1: Centroid line-of-sight barrier check ---
    violations = 0
    problem_hubs = []
    for hid, (hlat, hlon) in v7_centroids.items():
        addrs = [a for u in v7_rg.values() if u['hub_id'] == hid for a in u['addrs']]
        crosses = sum(1 for a in addrs
                      if road_graph.crosses_highway(hlat, hlon, a['lat'], a['lon'])
                      or (hasattr(road_graph, 'crosses_rail') and
                          road_graph.crosses_rail(hlat, hlon, a['lat'], a['lon'])))
        if crosses > 0:
            violations += crosses
            problem_hubs.append((hid, crosses, len(addrs)))

    if problem_hubs:
        print(f"  BARRIER VIOLATIONS (centroid LOS): {violations} addresses in {len(problem_hubs)} hubs")
        for hid, xing, total in problem_hubs:
            pct = xing * 100 / max(total, 1)
            print(f"    H{hid:04d}: {xing}/{total} ({pct:.0f}%) cross highway from centroid")
    else:
        print(f"  Centroid LOS test: PASS (0 violations across {len(v7_centroids)} hubs)")

    # --- Test 2: Component integrity (definitive — no false positives) ---
    comp_violations = 0
    multi_comp_hubs = []
    if hasattr(road_graph, 'node_component') and road_graph.node_component:
        for hid in v7_centroids:
            groups = [u for u in v7_rg.values() if u['hub_id'] == hid]
            comps = set()
            for g in groups:
                c = g.get('component_id', -1)
                if c != -1:
                    comps.add(c)
            if len(comps) > 1:
                comp_violations += 1
                multi_comp_hubs.append((hid, len(comps), len(groups)))

        if multi_comp_hubs:
            print(f"  COMPONENT VIOLATIONS: {comp_violations} hubs span multiple barrier components")
            for hid, n_comps, n_groups in multi_comp_hubs:
                print(f"    H{hid:04d}: {n_comps} components across {n_groups} road groups")
        else:
            print(f"  Component test: PASS (0 multi-component hubs out of {len(v7_centroids)})")
    else:
        print(f"  Component test: SKIPPED (no barrier components computed)")

    return problem_hubs


CLUSTER_COLORS = [
    '#ef4444', '#f97316', '#f59e0b', '#84cc16', '#22c55e',
    '#14b8a6', '#06b6d4', '#3b82f6', '#8b5cf6', '#d946ef',
    '#ec4899', '#f43f5e', '#fb923c', '#a3e635', '#34d399',
    '#2dd4bf', '#38bdf8', '#818cf8', '#c084fc', '#f472b6',
]

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Process each WC — Oracle data + V7 clustering + export
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  STEP 1: Data extraction + V7 clustering")
print("=" * 60)

all_wc_layers = {}  # clli → dict of GeoJSON layers (except v7_polys)

for wc in TEST_WCS:
    clli = wc['clli']
    morph = wc['morph']
    fips = wc['fips']
    ring = wc_polys.get(clli)

    print(f"\n{'#'*60}")
    print(f"#  {clli} — {wc['name']} ({morph})")
    print(f"{'#'*60}")
    t0 = time.time()

    # 1. Load OFS data
    ofs_data = get_ofs_data_for_wc(clli)
    ofs_addrs = ofs_data['addresses']
    nonofs_addrs = ofs_data['nonofs_addresses']

    # 2. Clip to WC boundary
    if ring:
        b_ofs, b_non = len(ofs_addrs), len(nonofs_addrs)
        ofs_addrs = [a for a in ofs_addrs if point_in_wc(a['LATITUDE'], a['LONGITUDE'], ring)]
        nonofs_addrs = [a for a in nonofs_addrs if point_in_wc(a['LATITUDE'], a['LONGITUDE'], ring)]
        print(f"  WC clip: OFS {b_ofs}→{len(ofs_addrs)}, NON-OFS {b_non}→{len(nonofs_addrs)}")

    if len(nonofs_addrs) < 20:
        print(f"  SKIP: too few addresses")
        continue

    # 3. Prepare for clustering
    cluster_addrs = []
    for a in nonofs_addrs:
        cluster_addrs.append({
            'lat': a['LATITUDE'], 'lon': a['LONGITUDE'],
            'address_id': a['LOCUS_ADDRESS_ID'],
            'units': a.get('NO_OF_UNITS', 1),
            'irr_v2': 0, 'copper_salvage': 0,
            'priority_rank': a.get('PRIORITY_RANK', 50),
            'cofs': 0, 'copper_cir': 0,
            '_aui': a.get('AUI', ''), '_ntas': a.get('NTAS_CNT', 1),
            '_cpp': a.get('CPO_PRED'), '_category': a.get('CATEGORY', ''),
        })

    # 4. Road graph + snap
    roads_path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
    if not os.path.exists(roads_path):
        print(f"  ERROR: No TIGER roads for {fips}"); continue

    lats = [a['lat'] for a in cluster_addrs]
    lons = [a['lon'] for a in cluster_addrs]
    bbox = (min(lats)-0.05, min(lons)-0.05, max(lats)+0.05, max(lons)+0.05)
    rg = RoadGraph(roads_path, bbox=bbox)
    rg.classify_barriers(morphology=morph)  # before snap so road groups split at highways

    # Load rail barriers if available
    rails_path = os.path.join(os.path.dirname(__file__), 'TIGER', 'rails', 'tl_2024_us_rails.shp')
    if os.path.exists(rails_path):
        rg.load_rail_barriers(rails_path, bbox=bbox)

    rg.snap_addresses(cluster_addrs)

    # 4b. Load copper cable data if available
    copper_path = os.path.join(os.path.dirname(__file__), f'{clli}_copper_cable.csv')
    if os.path.exists(copper_path):
        rg.load_copper_cable(copper_path)

    # 5. V7 clustering
    params = get_morphology_params(morph)
    infill_ids = set(inf['LOCUS_ADDRESS_ID'] for inf in ofs_data['infill_opportunities'])
    v7_centroids, v7_rg, v7_stats = cluster_addresses_v7(
        cluster_addrs, rg, params,
        ofs_exclusion_zones=ofs_data['exclusion_zones'],
        infill_ids=infill_ids,
        morphology=morph,
        use_network_distance=True,
    )

    # 6. Map address → hub
    addr_hub_map = {}
    for a in cluster_addrs:
        rg_id = a.get('road_group', -1)
        if rg_id >= 0 and rg_id in v7_rg:
            addr_hub_map[a['address_id']] = v7_rg[rg_id].get('hub_id', -1)

    hub_ids = sorted(set(v7_centroids.keys()))
    v7_color_map = {hid: CLUSTER_COLORS[i % len(CLUSTER_COLORS)] for i, hid in enumerate(hub_ids)}

    # 7. Export hub data JSON for Voronoi step (with per-hub address coords)
    hub_addr_coords = defaultdict(list)  # hub_id -> [(lon, lat), ...]
    for a in cluster_addrs:
        hid = addr_hub_map.get(a['address_id'], -1)
        if hid >= 0:
            hub_addr_coords[hid].append([round(a['lon'], 6), round(a['lat'], 6)])

    hub_data_list = []
    for hid, (lat, lon) in v7_centroids.items():
        grps = [u for u in v7_rg.values() if u['hub_id'] == hid]
        units = sum(g['units'] for g in grps)
        addrs = sum(g['n_addrs'] for g in grps)
        avg_s = round(sum(g['avg_score'] for g in grps) / max(len(grps), 1), 1)
        hub_data_list.append({
            'hub_id': hid, 'lat': lat, 'lon': lon,
            'units': units, 'addresses': addrs, 'avg_score': avg_s,
            'color': v7_color_map.get(hid, '#ef4444'),
            'addr_coords': hub_addr_coords.get(hid, []),
        })

    hub_json_path = os.path.join(OUTPUT_DIR, f'{clli}_v7_hub_data.json')
    with open(hub_json_path, 'w') as f:
        json.dump({'clli': clli, 'centroids': hub_data_list}, f)

    # 8. Build address-level GeoJSON layers
    # OFS hub colors
    ofs_hub_names = sorted(set(a.get('HUB_NAME', '?') for a in ofs_addrs if a.get('HUB_NAME')))
    ofs_color_map = {n: CLUSTER_COLORS[i % len(CLUSTER_COLORS)] for i, n in enumerate(ofs_hub_names)}

    # OFS addresses
    ofs_feat = []
    for a in ofs_addrs:
        hub = a.get('HUB_NAME', '?')
        ofs_feat.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [a['LONGITUDE'], a['LATITUDE']]},
            'properties': {
                'id': str(a['LOCUS_ADDRESS_ID']), 'hub': hub,
                'color': ofs_color_map.get(hub, '#3b82f6'),
                'aui': a.get('AUI', ''), 'units': a.get('NO_OF_UNITS', 1),
                'ntas': a.get('NTAS_CNT', 1),
                'cpp': round(a['CPO_PRED'], 0) if a.get('CPO_PRED') else None,
                'dist_ft': round(a.get('DIST_TO_HUB_FT', 0)),
            },
        })

    # NON-OFS addresses (clustered + excluded)
    clustered_ids = set(a['address_id'] for a in cluster_addrs)
    nonofs_feat = []
    excl_feat = []
    for a in cluster_addrs:
        hid = addr_hub_map.get(a['address_id'], -1)
        f = {
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [a['lon'], a['lat']]},
            'properties': {
                'id': str(a['address_id']),
                'hub_id': f'H{hid:04d}' if hid >= 0 else 'unassigned',
                'color': v7_color_map.get(hid, '#6b7280'),
                'aui': a.get('_aui', ''), 'units': a.get('units', 1),
                'ntas': a.get('_ntas', 1),
                'cpp': round(a['_cpp'], 0) if a.get('_cpp') else None,
                'score': round(a.get('fin_score', 0), 1),
                'infill': a.get('is_infill', False),
            },
        }
        if hid >= 0:
            nonofs_feat.append(f)
        else:
            excl_feat.append(f)

    # Excluded (not in cluster_addrs = filtered by OFS zone)
    for a in nonofs_addrs:
        if a['LOCUS_ADDRESS_ID'] not in clustered_ids:
            excl_feat.append({
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [a['LONGITUDE'], a['LATITUDE']]},
                'properties': {
                    'id': str(a['LOCUS_ADDRESS_ID']), 'hub_id': 'ofs_excluded',
                    'color': '#4b5563', 'aui': a.get('AUI', ''),
                    'units': a.get('NO_OF_UNITS', 1), 'ntas': a.get('NTAS_CNT', 1),
                    'cpp': round(a['CPO_PRED'], 0) if a.get('CPO_PRED') else None,
                    'score': 0, 'infill': False,
                },
            })

    # OFS zone polygons
    ofs_zone_feat = []
    for z in ofs_data['exclusion_zones']:
        if len(z['hull']) < 3: continue
        r = list(z['hull']) + [z['hull'][0]]
        ofs_zone_feat.append({
            'type': 'Feature',
            'geometry': {'type': 'Polygon', 'coordinates': [r]},
            'properties': {
                'hub_name': z['hub_name'], 'color': ofs_color_map.get(z['hub_name'], '#3b82f6'),
                'fdh_size': z['fdh_size'], 'addr_count': z['addr_count'],
                'unit_count': z['unit_count'], 'working': z['total_working'],
                'spare': z['total_spare'],
            },
        })

    # OFS hubs
    ofs_hub_feat = []
    for hub in ofs_data['hubs']:
        if ring and not point_in_wc(hub['LATITUDE'], hub['LONGITUDE'], ring): continue
        ofs_hub_feat.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [hub['LONGITUDE'], hub['LATITUDE']]},
            'properties': {
                'hub_name': hub['HUB_NAME'], 'color': ofs_color_map.get(hub['HUB_NAME'], '#3b82f6'),
                'fdh_size': hub.get('FDH_SIZE'), 'working': hub.get('TOTAL_WORKING', 0),
                'spare': hub.get('TOTAL_SPARE', 0), 'ports': hub.get('TOTAL_PORTS', 0),
            },
        })

    # V7 hubs
    v7_hub_feat = []
    for hid, (lat, lon) in v7_centroids.items():
        grps = [u for u in v7_rg.values() if u['hub_id'] == hid]
        v7_hub_feat.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
            'properties': {
                'hub_id': f'H{hid:04d}', 'color': v7_color_map.get(hid, '#ef4444'),
                'units': sum(g['units'] for g in grps),
                'addresses': sum(g['n_addrs'] for g in grps),
            },
        })

    # Highway barrier segments (for map visualization)
    barrier_gj = rg.get_barrier_geojson() if hasattr(rg, 'get_barrier_geojson') else {'type': 'FeatureCollection', 'features': []}

    # Export barrier GeoJSON to disk for Voronoi clipping step
    barrier_path = os.path.join(OUTPUT_DIR, f'{clli}_barriers.geojson')
    with open(barrier_path, 'w') as f:
        json.dump(barrier_gj, f)

    # Self-test: detect barrier violations
    validate_clustering(clli, v7_centroids, v7_rg, rg)

    # Rail barrier GeoJSON for map visualization
    rail_feat = []
    if hasattr(rg, 'rail_segments') and rg.rail_segments:
        for seg in rg.rail_segments:
            rail_feat.append({
                'type': 'Feature',
                'geometry': {'type': 'LineString',
                             'coordinates': [[seg[0][0], seg[0][1]], [seg[1][0], seg[1][1]]]},
                'properties': {'type': 'rail'}
            })

    # Copper cable GeoJSON for map visualization
    copper_feat = []
    if hasattr(rg, 'copper_segments') and rg.copper_segments:
        for flat, flon, tlat, tlon, qty in rg.copper_segments:
            copper_feat.append({
                'type': 'Feature',
                'geometry': {'type': 'LineString', 'coordinates': [[flon, flat], [tlon, tlat]]},
                'properties': {'qty': qty}
            })

    all_wc_layers[clli] = {
        'ofs_addrs': {'type': 'FeatureCollection', 'features': ofs_feat},
        'nonofs_addrs': {'type': 'FeatureCollection', 'features': nonofs_feat},
        'excluded_addrs': {'type': 'FeatureCollection', 'features': excl_feat},
        'ofs_zones': {'type': 'FeatureCollection', 'features': ofs_zone_feat},
        'ofs_hubs': {'type': 'FeatureCollection', 'features': ofs_hub_feat},
        'v7_hubs': {'type': 'FeatureCollection', 'features': v7_hub_feat},
        'barriers': barrier_gj,
        'rails': {'type': 'FeatureCollection', 'features': rail_feat},
        'copper': {'type': 'FeatureCollection', 'features': copper_feat},
    }

    print(f"  [{clli}] Done in {time.time()-t0:.1f}s — "
          f"OFS: {len(ofs_feat):,} addrs, {len(ofs_zone_feat)} zones | "
          f"V7: {len(nonofs_feat):,} addrs, {len(v7_hub_feat)} hubs, {len(excl_feat):,} excluded")


# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Run Voronoi generation (arcgispro-py3)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  STEP 2: Voronoi polygon generation (scipy)")
print("=" * 60)

voronoi_script = os.path.join(BASE, 'build_v7_voronoi.py')
result = subprocess.run(
    [ARCGIS_PYTHON, voronoi_script],
    capture_output=True, text=True, cwd=BASE, timeout=600
)
print(result.stdout)
if result.returncode != 0:
    print(f"  Voronoi STDERR: {result.stderr}")
    print("  WARNING: Voronoi failed, will use convex hull fallback")

# Load Voronoi results
for clli in list(all_wc_layers.keys()):
    voronoi_path = os.path.join(OUTPUT_DIR, f'{clli}_v7_voronoi.geojson')
    if os.path.exists(voronoi_path) and os.path.getsize(voronoi_path) > 100:
        with open(voronoi_path) as f:
            all_wc_layers[clli]['v7_polys'] = json.load(f)
        print(f"  [{clli}] Voronoi loaded: {len(all_wc_layers[clli]['v7_polys']['features'])} polygons")
    else:
        # Fallback: no polygons
        all_wc_layers[clli]['v7_polys'] = {'type': 'FeatureCollection', 'features': []}
        print(f"  [{clli}] No Voronoi available")


# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Build HTML map
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  STEP 3: Building HTML map")
print("=" * 60)

stats = {}
csv_path = os.path.join(OUTPUT_DIR, 'v7_comparison_results.csv')
if os.path.exists(csv_path):
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            stats[row['clli']] = row

wc_meta = {w['clli']: {'name': w['name'], 'morph': w['morph']} for w in TEST_WCS}
wc_list = [w['clli'] for w in TEST_WCS if w['clli'] in all_wc_layers]

html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>V7 OFS-Aware Clustering — Voronoi Territories</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'Segoe UI',system-ui,sans-serif; background:#0f1117; color:#e1e4ea; }
#header { background:linear-gradient(135deg,#1a1d28,#252836); padding:10px 16px;
          display:flex; align-items:center; gap:14px; border-bottom:2px solid #3b82f6; flex-wrap:wrap; }
#header h1 { font-size:16px; font-weight:600; color:#fff; white-space:nowrap; }
.wc-tabs { display:flex; gap:4px; }
.wc-tab { padding:5px 12px; border-radius:5px; cursor:pointer; font-size:12px;
          background:#2a2d3a; color:#9ca3af; border:1px solid #3a3d4a; transition:all 0.2s; }
.wc-tab:hover { background:#3a3d4a; color:#e1e4ea; }
.wc-tab.active { background:#3b82f6; color:#fff; border-color:#3b82f6; }
.wc-tab .morph { font-size:10px; opacity:0.7; margin-left:3px; }
.layer-toggles { display:flex; gap:2px; margin-left:auto; flex-wrap:wrap; }
.ltoggle { padding:4px 8px; border-radius:4px; cursor:pointer; font-size:11px;
           background:#2a2d3a; color:#9ca3af; border:1px solid #3a3d4a; transition:all 0.15s; user-select:none; }
.ltoggle.on { color:#e1e4ea; border-color:#6b7280; }
.ltoggle .dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:4px; vertical-align:middle; }
.ltoggle .sq { display:inline-block; width:8px; height:8px; margin-right:4px; vertical-align:middle; opacity:0.7; }
#map { height:calc(100vh - 90px); width:100%; }
#stats-bar { background:#1a1d28; padding:6px 16px; display:flex; gap:18px; font-size:11px;
            border-top:1px solid #2a2d3a; overflow-x:auto; white-space:nowrap; }
.stat { display:flex; align-items:center; gap:4px; }
.stat-label { color:#6b7280; }
.stat-value { color:#e1e4ea; font-weight:600; }
.leaflet-popup-content-wrapper { background:#1a1d28; color:#e1e4ea; border-radius:8px; border:1px solid #3a3d4a; }
.leaflet-popup-tip { background:#1a1d28; }
.leaflet-popup-content { font-size:11px; line-height:1.5; min-width:180px; }
.popup-title { font-weight:700; font-size:13px; margin-bottom:3px; }
.popup-row { display:flex; justify-content:space-between; gap:12px; }
.popup-label { color:#6b7280; }
.popup-val { color:#e1e4ea; font-weight:600; }
</style>
</head>
<body>
<div id="header">
  <h1>V7 OFS-Aware Clustering — Voronoi Territories</h1>
  <div class="wc-tabs" id="wc-tabs"></div>
  <div class="layer-toggles" id="layer-toggles"></div>
</div>
<div id="map"></div>
<div id="stats-bar"></div>
<script>
"""

html += f"const WC_DATA={json.dumps(all_wc_layers)};\n"
html += f"const WC_BOUNDS={json.dumps(wc_boundary_features)};\n"
html += f"const WC_META={json.dumps(wc_meta)};\n"
html += f"const WC_STATS={json.dumps(stats)};\n"
html += f"const WC_LIST={json.dumps(wc_list)};\n"

html += """
const map=L.map('map',{zoomControl:true}).setView([39.5,-77],8);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{attribution:'&copy; CARTO',maxZoom:19}).addTo(map);

let layers={},layerVisible={},currentClli=null;
const LAYER_DEFS=[
    {key:'wc_boundary',label:'WC Boundary',dot:'#6b7280',shape:'sq',on:true},
    {key:'ofs_zones',  label:'OFS Zones',   dot:'#3b82f6',shape:'sq',on:true},
    {key:'ofs_addrs',  label:'OFS Addrs',   dot:'#60a5fa',shape:'dot',on:true},
    {key:'ofs_hubs',   label:'OFS Hubs',    dot:'#2563eb',shape:'dot',on:true},
    {key:'v7_polys',   label:'V7 Territories',dot:'#f97316',shape:'sq',on:true},
    {key:'nonofs_addrs',label:'V7 Addrs',   dot:'#ef4444',shape:'dot',on:true},
    {key:'v7_hubs',    label:'V7 Hubs',     dot:'#dc2626',shape:'dot',on:true},
    {key:'barriers',   label:'Hwy Barriers',dot:'#fb923c',shape:'sq',on:true},
    {key:'rails',      label:'Rail Barriers',dot:'#78350f',shape:'sq',on:true},
    {key:'copper',     label:'Copper Cable',dot:'#9ca3af',shape:'sq',on:false},
    {key:'excluded',   label:'Excluded',    dot:'#4b5563',shape:'dot',on:false},
];

const toggleDiv=document.getElementById('layer-toggles');
LAYER_DEFS.forEach(ld=>{
    layerVisible[ld.key]=ld.on;
    const el=document.createElement('div');
    el.className='ltoggle'+(ld.on?' on':'');
    el.dataset.key=ld.key;
    const sh=ld.shape==='sq'?`<span class="sq" style="background:${ld.dot}"></span>`
                             :`<span class="dot" style="background:${ld.dot}"></span>`;
    el.innerHTML=sh+ld.label;
    el.onclick=()=>{
        layerVisible[ld.key]=!layerVisible[ld.key];
        el.classList.toggle('on',layerVisible[ld.key]);
        if(layers[ld.key]){if(layerVisible[ld.key])map.addLayer(layers[ld.key]);else map.removeLayer(layers[ld.key]);}
    };
    toggleDiv.appendChild(el);
});

function clearAll(){Object.values(layers).forEach(lg=>map.removeLayer(lg));layers={};}

function popup(p,type){
    const c=p.color||'#999';
    const title=type==='ofs'?`<span style="color:${c}">${p.hub||'OFS'}</span>`
                             :`<span style="color:${c}">${p.hub_id||'—'}</span>`;
    let r=`<div class="popup-row"><span class="popup-label">NTAS ID:</span><span class="popup-val">${p.id}</span></div>
        <div class="popup-row"><span class="popup-label">AUI:</span><span class="popup-val">${p.aui||'—'}</span></div>
        <div class="popup-row"><span class="popup-label">Units:</span><span class="popup-val">${p.units||1}</span></div>
        <div class="popup-row"><span class="popup-label">NTAS Count:</span><span class="popup-val">${p.ntas||1}</span></div>`;
    if(p.cpp)r+=`<div class="popup-row"><span class="popup-label">CPP:</span><span class="popup-val">$${p.cpp.toLocaleString()}</span></div>`;
    if(type==='ofs'&&p.dist_ft)r+=`<div class="popup-row"><span class="popup-label">Dist to Hub:</span><span class="popup-val">${p.dist_ft.toLocaleString()} ft</span></div>`;
    if(type==='nonofs'&&p.score)r+=`<div class="popup-row"><span class="popup-label">Score:</span><span class="popup-val">${p.score}</span></div>`;
    if(p.infill)r+=`<div class="popup-row"><span class="popup-label">Infill:</span><span class="popup-val" style="color:#f59e0b">Yes</span></div>`;
    return`<div class="popup-title">${title}</div>${r}`;
}

function showWC(clli){
    clearAll(); currentClli=clli;
    document.querySelectorAll('.wc-tab').forEach(t=>t.classList.toggle('active',t.dataset.clli===clli));
    const d=WC_DATA[clli]; if(!d)return;

    // WC Boundary
    if(WC_BOUNDS[clli]){const lg=L.layerGroup();
        L.geoJSON(WC_BOUNDS[clli],{style:{color:'#6b7280',weight:2,fillOpacity:0,dashArray:'6,4'}}).addTo(lg);
        layers['wc_boundary']=lg;}

    // OFS Zones
    if(d.ofs_zones&&d.ofs_zones.features.length){const lg=L.layerGroup();
        L.geoJSON(d.ofs_zones,{
            style:f=>({color:f.properties.color||'#3b82f6',weight:1.5,fillColor:f.properties.color||'#3b82f6',fillOpacity:0.12}),
            onEachFeature:(f,l)=>{const p=f.properties;l.bindPopup(`
                <div class="popup-title" style="color:${p.color}">${p.hub_name}</div>
                <div class="popup-row"><span class="popup-label">FDH:</span><span class="popup-val">${p.fdh_size||'—'}</span></div>
                <div class="popup-row"><span class="popup-label">Addrs:</span><span class="popup-val">${(p.addr_count||0).toLocaleString()}</span></div>
                <div class="popup-row"><span class="popup-label">Units:</span><span class="popup-val">${(p.unit_count||0).toLocaleString()}</span></div>
                <div class="popup-row"><span class="popup-label">Working:</span><span class="popup-val">${p.working||0}</span></div>
                <div class="popup-row"><span class="popup-label">Spare:</span><span class="popup-val">${p.spare||0}</span></div>`);}
        }).addTo(lg); layers['ofs_zones']=lg;}

    // OFS Addresses
    if(d.ofs_addrs&&d.ofs_addrs.features.length){const lg=L.layerGroup();
        L.geoJSON(d.ofs_addrs,{
            pointToLayer:(f,ll)=>{const u=f.properties.units||1;const r=u>10?5:u>3?3.5:2.5;
                return L.circleMarker(ll,{radius:r,color:f.properties.color,fillColor:f.properties.color,fillOpacity:0.6,weight:0.5});},
            onEachFeature:(f,l)=>l.bindPopup(popup(f.properties,'ofs'))
        }).addTo(lg); layers['ofs_addrs']=lg;}

    // OFS Hubs
    if(d.ofs_hubs&&d.ofs_hubs.features.length){const lg=L.layerGroup();
        L.geoJSON(d.ofs_hubs,{
            pointToLayer:(f,ll)=>L.circleMarker(ll,{radius:7,color:'#fff',fillColor:f.properties.color||'#3b82f6',fillOpacity:0.95,weight:2}),
            onEachFeature:(f,l)=>{const p=f.properties;l.bindPopup(`
                <div class="popup-title" style="color:${p.color}">${p.hub_name}</div>
                <div class="popup-row"><span class="popup-label">FDH:</span><span class="popup-val">${p.fdh_size||'—'}</span></div>
                <div class="popup-row"><span class="popup-label">Working:</span><span class="popup-val">${p.working}</span></div>
                <div class="popup-row"><span class="popup-label">Spare:</span><span class="popup-val">${p.spare}</span></div>
                <div class="popup-row"><span class="popup-label">Ports:</span><span class="popup-val">${p.ports}</span></div>`);}
        }).addTo(lg); layers['ofs_hubs']=lg;}

    // V7 Voronoi Territories (non-overlapping!)
    if(d.v7_polys&&d.v7_polys.features.length){const lg=L.layerGroup();
        L.geoJSON(d.v7_polys,{
            style:f=>({color:f.properties.color||'#ef4444',weight:2,fillColor:f.properties.color||'#ef4444',fillOpacity:0.10}),
            onEachFeature:(f,l)=>{const p=f.properties;l.bindPopup(`
                <div class="popup-title" style="color:${p.color}">${p.hub_id} (V7 Territory)</div>
                <div class="popup-row"><span class="popup-label">Units:</span><span class="popup-val">${(p.units||0).toLocaleString()}</span></div>
                <div class="popup-row"><span class="popup-label">Addresses:</span><span class="popup-val">${(p.addresses||0).toLocaleString()}</span></div>
                <div class="popup-row"><span class="popup-label">Avg Score:</span><span class="popup-val">${p.avg_score}</span></div>`);}
        }).addTo(lg); layers['v7_polys']=lg;}

    // V7 Addresses
    if(d.nonofs_addrs&&d.nonofs_addrs.features.length){const lg=L.layerGroup();
        L.geoJSON(d.nonofs_addrs,{
            pointToLayer:(f,ll)=>{const u=f.properties.units||1;const r=u>10?5:u>3?3.5:2.5;
                return L.circleMarker(ll,{radius:r,color:f.properties.color,fillColor:f.properties.color,fillOpacity:0.75,weight:0.5});},
            onEachFeature:(f,l)=>l.bindPopup(popup(f.properties,'nonofs'))
        }).addTo(lg); layers['nonofs_addrs']=lg;}

    // V7 Hubs
    if(d.v7_hubs&&d.v7_hubs.features.length){const lg=L.layerGroup();
        L.geoJSON(d.v7_hubs,{
            pointToLayer:(f,ll)=>{const u=f.properties.units||1;const r=7+Math.min(u/50,8);
                return L.circleMarker(ll,{radius:r,color:'#fff',fillColor:f.properties.color||'#ef4444',fillOpacity:0.95,weight:2});},
            onEachFeature:(f,l)=>{const p=f.properties;l.bindPopup(`
                <div class="popup-title" style="color:${p.color}">${p.hub_id} (V7 Hub)</div>
                <div class="popup-row"><span class="popup-label">Units:</span><span class="popup-val">${(p.units||0).toLocaleString()}</span></div>
                <div class="popup-row"><span class="popup-label">Addresses:</span><span class="popup-val">${(p.addresses||0).toLocaleString()}</span></div>`);}
        }).addTo(lg); layers['v7_hubs']=lg;}

    // Highway Barriers
    if(d.barriers&&d.barriers.features.length){const lg=L.layerGroup();
        L.geoJSON(d.barriers,{
            style:f=>({color:'#fb923c',weight:2.5,opacity:0.8,dashArray:'8,4'}),
            onEachFeature:(f,l)=>{const p=f.properties;l.bindPopup(`
                <div class="popup-title" style="color:#fb923c">${p.name||'Highway'}</div>
                <div class="popup-row"><span class="popup-label">MTFCC:</span><span class="popup-val">${p.mtfcc}</span></div>`);}
        }).addTo(lg); layers['barriers']=lg;}

    // Rail Barriers
    if(d.rails&&d.rails.features.length){const lg=L.layerGroup();
        L.geoJSON(d.rails,{
            style:f=>({color:'#78350f',weight:2.5,opacity:0.7,dashArray:'4,6'}),
            onEachFeature:(f,l)=>{l.bindPopup('Railroad');}
        }).addTo(lg); layers['rails']=lg;}

    // Copper Cable
    if(d.copper&&d.copper.features.length){const lg=L.layerGroup();
        L.geoJSON(d.copper,{
            style:f=>({color:'#9ca3af',weight:1.5,opacity:0.6}),
            onEachFeature:(f,l)=>{l.bindPopup(`Copper: ${f.properties.qty} pairs`);}
        }).addTo(lg); layers['copper']=lg;}

    // Excluded
    if(d.excluded_addrs&&d.excluded_addrs.features.length){const lg=L.layerGroup();
        L.geoJSON(d.excluded_addrs,{
            pointToLayer:(f,ll)=>L.circleMarker(ll,{radius:2,color:'#4b5563',fillColor:'#4b5563',fillOpacity:0.4,weight:0.5}),
            onEachFeature:(f,l)=>l.bindPopup(popup(f.properties,'nonofs'))
        }).addTo(lg); layers['excluded']=lg;}

    // Show visible
    LAYER_DEFS.forEach(ld=>{if(layerVisible[ld.key]&&layers[ld.key])map.addLayer(layers[ld.key]);});

    // Fit bounds
    let ab=[];
    Object.entries(layers).forEach(([k,lg])=>{if(map.hasLayer(lg)){
        lg.eachLayer(l=>{if(l.getBounds)ab.push(l.getBounds());else if(l.getLatLng)ab.push(L.latLngBounds([l.getLatLng(),l.getLatLng()]));});
    }});
    if(ab.length){let b=ab[0];ab.forEach(bb=>b.extend(bb));map.fitBounds(b,{padding:[30,30]});}
    updateStats(clli);
}

function updateStats(clli){
    const s=WC_STATS[clli],bar=document.getElementById('stats-bar'),meta=WC_META[clli]||{},d=WC_DATA[clli]||{};
    if(!s){bar.innerHTML='';return;}
    const oA=(d.ofs_addrs||{features:[]}).features.length;
    const nA=(d.nonofs_addrs||{features:[]}).features.length;
    const eA=(d.excluded_addrs||{features:[]}).features.length;
    const vP=(d.v7_polys||{features:[]}).features.length;
    bar.innerHTML=`
        <div class="stat"><span class="stat-label">WC:</span><span class="stat-value">${clli} — ${meta.name||''} (${meta.morph||''})</span></div>
        <div class="stat"><span class="stat-label">OFS Addrs:</span><span class="stat-value">${oA.toLocaleString()}</span></div>
        <div class="stat"><span class="stat-label">V7 Addrs:</span><span class="stat-value">${nA.toLocaleString()}</span></div>
        <div class="stat"><span class="stat-label">Excluded:</span><span class="stat-value">${eA.toLocaleString()}</span></div>
        <div class="stat"><span class="stat-label">V7 Territories:</span><span class="stat-value">${vP}</span></div>
        <div class="stat"><span class="stat-label">OFS Zones:</span><span class="stat-value">${(d.ofs_zones||{features:[]}).features.length}</span></div>`;
}

const tabsDiv=document.getElementById('wc-tabs');
WC_LIST.forEach(clli=>{
    const meta=WC_META[clli]||{};const tab=document.createElement('div');
    tab.className='wc-tab';tab.dataset.clli=clli;
    tab.innerHTML=`${clli}<span class="morph">${meta.morph||''}</span>`;
    tab.onclick=()=>showWC(clli);tabsDiv.appendChild(tab);
});
showWC(WC_LIST[0]);
</script>
</body>
</html>
"""

out_html = os.path.join(OUTPUT_DIR, 'v7_clustering_map_v2b.html')
with open(out_html, 'w', encoding='utf-8') as f:
    f.write(html)

size_mb = os.path.getsize(out_html) / (1024*1024)
print(f"\nMap saved: {out_html}")
print(f"  Size: {size_mb:.1f} MB")
print("DONE.")
