"""
Build NON-OFS Cluster Explorer Data Files

Generates tiered JS data files for the interactive map dashboard:
  - data/summary.js         (~15 KB)  National/region/sub-region aggregates
  - data/sr/{key}.js        (13 files) WC boundaries + cluster centroids per sub-region
  - data/wc/{CLLI}.js       (2,078 files) Cluster polygons + address points per WC

Inputs (all in AI_Sessions/):
  - v6_cluster_summary.csv
  - v6_cluster_eisenhower.csv
  - all_nonofs_cluster_polygons.geojson
  - all_nonofs_12m_sorted.csv  (2.7 GB, streamed)
  - addr_obligation_tags.csv
  - v6_clusters_cache.json + v6_clusters_P{1-5}.json (for LAID->cluster mapping)
  - Portfolio-Simulator/data-wc.js (WC boundaries)

Usage:
    python build_explorer_data.py
"""
import csv, json, os, sys, time, re
from collections import defaultdict

csv.field_size_limit(2**31 - 1)

BASE = r'C:\Users\v267429\Downloads\AI_Sessions'
OUT = r'C:\Users\v267429\Downloads\NON-OFS-Explorer\data'
WC_JS = r'C:\Users\v267429\Downloads\Portfolio-Simulator\data-wc.js'

CLUSTER_SUMMARY = os.path.join(BASE, 'v6_cluster_summary.csv')
EISENHOWER_CSV = os.path.join(BASE, 'v6_cluster_eisenhower.csv')
POLYGON_GEOJSON = os.path.join(BASE, 'all_nonofs_cluster_polygons.geojson')
ADDR_CSV = os.path.join(BASE, 'all_nonofs_12m_sorted.csv')
OBLIGATION_CSV = os.path.join(BASE, 'addr_obligation_tags.csv')
SUBREGION_MAP = os.path.join(BASE, 'clli_subregion_map.json')

# Region -> sub-region mapping
REGIONS = {
    'Northeast Metro': ['Eastern MA', 'EN PA', 'MAN/BK/SI', 'NJ North', 'QNS/BRX/LI'],
    'Potomac': ['MD/DE', 'VA', 'WE MD/DC Metro'],
    'Tri-State': ['NJ South/Phila', 'NY Midstate', 'NY Upstate', 'WE/CE MA RI', 'WE/CE PA'],
}

# Sub-region -> safe filename key
def sr_key(name):
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')

AUI_MAP = ['SFU', 'SBU', 'MDU', 'MTU']
AUI_IDX = {v: i for i, v in enumerate(AUI_MAP)}
OBLIG_MAP = ['DISCRETIONARY', 'SBB_OBLIG', 'COP_2026_OBLIG', 'COP_2027_OBLIG',
             'COP_FUTURE_OBLIG', 'NSI_OBLIG', 'LFA_OBLIG']
OBLIG_IDX = {v: i for i, v in enumerate(OBLIG_MAP)}


def load_cluster_data():
    """Load cluster summary + eisenhower scores, indexed by cluster_id."""
    print("Loading cluster summary + eisenhower...", flush=True)
    clusters = {}
    with open(CLUSTER_SUMMARY, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            cid = row['cluster_id']
            clusters[cid] = {
                'clli': row['clli'],
                'region': row['region'],
                'sub_region': row['sub_region'],
                'density': row['market_density'],
                'lat': float(row['lat']),
                'lon': float(row['lon']),
                'units': int(row['total_units']),
                'addrs': int(row['total_addrs']),
                'capex': round(float(row['total_capex'])),
                'cpp': round(float(row['avg_cpp'])),
                'irr': round(float(row['median_irr']), 1),
                'copper': int(row['copper_circuits']),
                'fscore': round(float(row['avg_formation_score']), 1),
                'sfu': int(row['aui_SFU']),
                'sbu': int(row['aui_SBU']),
                'mdu': int(row['aui_MDU']),
                'mtu': int(row['aui_MTU']),
            }

    with open(EISENHOWER_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            cid = row['cluster_id']
            if cid in clusters:
                clusters[cid]['urg'] = round(float(row['urgency_score']), 1)
                clusters[cid]['val'] = round(float(row['value_score']), 1)
                clusters[cid]['quad'] = row['build_priority_tier']
                clusters[cid]['top_ob'] = row['top_obligation']
                clusters[cid]['ob_frac'] = round(float(row['obligation_fraction']), 2)

    print(f"  {len(clusters):,} clusters loaded", flush=True)
    return clusters


def load_laid_to_cluster():
    """Build LAID -> cluster_id mapping from partition + cache files."""
    print("Loading LAID->cluster mappings...", flush=True)
    laid_map = {}

    # Partition files first (most complete)
    for i in range(1, 6):
        f = os.path.join(BASE, f'v6_clusters_P{i}.json')
        if os.path.exists(f):
            with open(f, encoding='utf-8') as fh:
                data = json.load(fh)
            for c in data:
                cid = c['cluster_id']
                for laid in c.get('addresses', []):
                    laid_map[str(laid)] = cid

    # Main cache for any stragglers
    cache_f = os.path.join(BASE, 'v6_clusters_cache.json')
    if os.path.exists(cache_f):
        with open(cache_f, encoding='utf-8') as fh:
            data = json.load(fh)
        for c in data:
            cid = c['cluster_id']
            for laid in c.get('addresses', []):
                s = str(laid)
                if s not in laid_map:
                    laid_map[s] = cid

    print(f"  {len(laid_map):,} LAID->cluster mappings", flush=True)
    return laid_map


def load_obligations():
    """Load obligation tags: LAID -> bucket index."""
    print("Loading obligation tags...", flush=True)
    obligs = {}
    with open(OBLIGATION_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            obligs[row['LOCUS_ADDRESS_ID']] = OBLIG_IDX.get(row['OBLIGATION_BUCKET'], 0)
    print(f"  {len(obligs):,} obligation tags", flush=True)
    return obligs


def load_polygons():
    """Load polygon GeoJSON, index by cluster_id."""
    print("Loading cluster polygons...", flush=True)
    with open(POLYGON_GEOJSON, encoding='utf-8') as f:
        geo = json.load(f)
    polys = {}
    for feat in geo['features']:
        props = feat['properties']
        cid = props['CLUSTER_ID']
        geom = feat['geometry']
        if geom['type'] == 'Polygon':
            ring = geom['coordinates'][0]
            polys[cid] = [[round(c[0], 5), round(c[1], 5)] for c in ring]
        elif geom['type'] == 'Point':
            # Single-address cluster: no polygon to render
            pass
    print(f"  {len(polys):,} polygon features", flush=True)
    return polys


def load_wc_boundaries():
    """Load WC boundaries from full GeoJSON and simplify coordinates."""
    WC_GEOJSON = r'C:\Users\v267429\Documents\claudework\wc_boundaries.geojson'
    print("Loading WC boundaries (full GeoJSON, simplifying)...", flush=True)

    # Also try Portfolio Simulator's pre-simplified version first
    wc_features = {}

    # Load simplified WCs from Portfolio Simulator JS
    if os.path.exists(WC_JS):
        with open(WC_JS, encoding='utf-8') as f:
            content = f.read()
        json_start = content.index('{')
        decoder = json.JSONDecoder()
        geo, _ = decoder.raw_decode(content, json_start)
        for feat in geo['features']:
            clli = feat['properties']['CLLI']
            wc_features[clli] = feat
        print(f"  {len(wc_features)} WCs from Portfolio Simulator (pre-simplified)", flush=True)

    # Load remaining WCs from full GeoJSON with coordinate simplification
    if os.path.exists(WC_GEOJSON):
        with open(WC_GEOJSON, encoding='utf-8') as f:
            full_geo = json.load(f)
        added = 0
        for feat in full_geo['features']:
            clli = feat['properties'].get('CLLI', '')
            if clli and clli not in wc_features:
                # Simplify coordinates: reduce to every Nth point
                simplified = simplify_feature(feat)
                wc_features[clli] = simplified
                added += 1
        print(f"  +{added} WCs from full GeoJSON (simplified)", flush=True)

    print(f"  {len(wc_features):,} total WC boundary features", flush=True)
    return wc_features


def simplify_feature(feat):
    """Simplify polygon coordinates by keeping every Nth point."""
    geom = feat['geometry']
    if geom['type'] == 'Polygon':
        new_coords = []
        for ring in geom['coordinates']:
            simplified_ring = simplify_ring(ring)
            new_coords.append(simplified_ring)
        return {
            'type': 'Feature',
            'properties': feat['properties'],
            'geometry': {'type': 'Polygon', 'coordinates': new_coords}
        }
    elif geom['type'] == 'MultiPolygon':
        new_polys = []
        for poly in geom['coordinates']:
            new_rings = [simplify_ring(ring) for ring in poly]
            new_polys.append(new_rings)
        return {
            'type': 'Feature',
            'properties': feat['properties'],
            'geometry': {'type': 'MultiPolygon', 'coordinates': new_polys}
        }
    return feat


def simplify_ring(ring):
    """Keep every Nth point to reduce coordinate count. Ensure ring is closed."""
    n = len(ring)
    if n <= 20:
        return [[round(c[0], 5), round(c[1], 5)] for c in ring]
    # Keep approximately 60 points per ring
    step = max(1, n // 60)
    simplified = [ring[i] for i in range(0, n, step)]
    # Ensure ring is closed
    if simplified[0] != simplified[-1]:
        simplified.append(simplified[0])
    return [[round(c[0], 5), round(c[1], 5)] for c in simplified]


def build_summary(clusters, wc_features):
    """Build summary.js with national/region/sub-region aggregates."""
    print("Building summary.js...", flush=True)

    # Aggregate by sub-region
    sr_data = defaultdict(lambda: {
        'wcs': set(), 'clusters': 0, 'units': 0, 'addrs': 0, 'capex': 0,
        'irr_sum': 0, 'irr_count': 0, 'lats': [], 'lons': [],
        'q1': 0, 'q2': 0, 'q3': 0, 'q4': 0,
    })

    for cid, c in clusters.items():
        sr = c['sub_region']
        d = sr_data[sr]
        d['wcs'].add(c['clli'])
        d['clusters'] += 1
        d['units'] += c['units']
        d['addrs'] += c['addrs']
        d['capex'] += c['capex']
        if c['irr'] > 0:
            d['irr_sum'] += c['irr'] * c['units']
            d['irr_count'] += c['units']
        d['lats'].append(c['lat'])
        d['lons'].append(c['lon'])
        q = c.get('quad', 'Q4_Deprioritize')
        if 'Q1' in q: d['q1'] += 1
        elif 'Q2' in q: d['q2'] += 1
        elif 'Q3' in q: d['q3'] += 1
        else: d['q4'] += 1

    # Build sub-region list
    sub_regions = []
    for sr_name, d in sorted(sr_data.items()):
        avg_irr = round(d['irr_sum'] / d['irr_count'], 1) if d['irr_count'] else 0
        sub_regions.append({
            'key': sr_key(sr_name),
            'name': sr_name,
            'region': next((r for r, subs in REGIONS.items() if sr_name in subs), 'Unknown'),
            'wcs': len(d['wcs']),
            'clusters': d['clusters'],
            'units': d['units'],
            'addrs': d['addrs'],
            'capex': d['capex'],
            'avg_irr': avg_irr,
            'center': [round(sum(d['lats'])/len(d['lats']), 4),
                        round(sum(d['lons'])/len(d['lons']), 4)],
            'eisenhower': {'Q1': d['q1'], 'Q2': d['q2'], 'Q3': d['q3'], 'Q4': d['q4']},
        })

    # Build region list
    regions = []
    for rname, sr_names in REGIONS.items():
        rd = {'name': rname, 'subs': [], 'wcs': 0, 'clusters': 0, 'units': 0,
              'addrs': 0, 'capex': 0, 'q1': 0, 'q2': 0, 'q3': 0, 'q4': 0}
        for sr in sub_regions:
            if sr['name'] in sr_names:
                rd['subs'].append(sr['key'])
                rd['wcs'] += sr['wcs']
                rd['clusters'] += sr['clusters']
                rd['units'] += sr['units']
                rd['addrs'] += sr['addrs']
                rd['capex'] += sr['capex']
                rd['q1'] += sr['eisenhower']['Q1']
                rd['q2'] += sr['eisenhower']['Q2']
                rd['q3'] += sr['eisenhower']['Q3']
                rd['q4'] += sr['eisenhower']['Q4']
        regions.append(rd)

    # National totals
    national = {
        'wcs': sum(r['wcs'] for r in regions),
        'clusters': sum(r['clusters'] for r in regions),
        'units': sum(r['units'] for r in regions),
        'addrs': sum(r['addrs'] for r in regions),
        'capex': sum(r['capex'] for r in regions),
        'q1': sum(r['q1'] for r in regions),
        'q2': sum(r['q2'] for r in regions),
        'q3': sum(r['q3'] for r in regions),
        'q4': sum(r['q4'] for r in regions),
    }

    # CLLI search index: [clli, wc_name, sub_region_key, units, irr]
    clli_index = []
    wc_agg = defaultdict(lambda: {'units': 0, 'irr_sum': 0, 'irr_n': 0, 'sr': '', 'name': ''})
    for cid, c in clusters.items():
        w = wc_agg[c['clli']]
        w['units'] += c['units']
        if c['irr'] > 0:
            w['irr_sum'] += c['irr'] * c['units']
            w['irr_n'] += c['units']
        w['sr'] = sr_key(c['sub_region'])
    for clli in sorted(wc_agg):
        w = wc_agg[clli]
        wc_name = ''
        if clli in wc_features:
            wc_name = wc_features[clli]['properties'].get('WIRECENTER', '')
        avg_irr = round(w['irr_sum'] / w['irr_n'], 1) if w['irr_n'] else 0
        clli_index.append([clli, wc_name, w['sr'], w['units'], avg_irr])

    # Build simplified region outlines for L1 map
    # Merge WC polygons per region into bounding boxes
    region_bounds = {}
    for rname, sr_names in REGIONS.items():
        lats, lons = [], []
        for sr in sub_regions:
            if sr['name'] in sr_names:
                lats.append(sr['center'][0])
                lons.append(sr['center'][1])
        if lats:
            region_bounds[rname] = {
                'center': [round(sum(lats)/len(lats), 3), round(sum(lons)/len(lons), 3)],
            }

    summary = {
        'national': national,
        'regions': regions,
        'sub_regions': sub_regions,
        'clli_index': clli_index,
        'region_bounds': region_bounds,
    }

    outpath = os.path.join(OUT, 'summary.js')
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write('window._summary=')
        json.dump(summary, f, separators=(',', ':'))
        f.write(';\n')
    size = os.path.getsize(outpath)
    print(f"  summary.js: {size:,} bytes", flush=True)


def build_subregion_files(clusters, wc_features):
    """Build per-sub-region JS files with WC boundaries + cluster centroids."""
    print("Building sub-region files...", flush=True)

    # Group clusters by sub-region
    sr_clusters = defaultdict(list)
    for cid, c in clusters.items():
        sr_clusters[c['sub_region']].append((cid, c))

    # Group WC stats by sub-region
    for sr_name, cluster_list in sorted(sr_clusters.items()):
        key = sr_key(sr_name)

        # Collect WC boundaries for this sub-region
        wc_cllis = set(c['clli'] for _, c in cluster_list)
        wc_geo_features = []
        for clli in sorted(wc_cllis):
            if clli in wc_features:
                wc_geo_features.append(wc_features[clli])

        wc_geojson = {'type': 'FeatureCollection', 'features': wc_geo_features}

        # Cluster centroids: [lat, lon, cluster_id, quadrant, units, capex]
        centroids = []
        for cid, c in cluster_list:
            q_short = 'Q1' if 'Q1' in c.get('quad', '') else \
                      'Q2' if 'Q2' in c.get('quad', '') else \
                      'Q3' if 'Q3' in c.get('quad', '') else 'Q4'
            centroids.append([
                round(c['lat'], 5), round(c['lon'], 5),
                cid, q_short, c['units'], c['capex']
            ])

        # Per-WC summary stats
        wc_stats = defaultdict(lambda: {
            'u': 0, 'a': 0, 'x': 0, 'cl': 0,
            'irr_s': 0, 'irr_n': 0,
            'q1': 0, 'q2': 0, 'q3': 0, 'q4': 0,
            'ob': 0,  # obligated clusters
        })
        for cid, c in cluster_list:
            w = wc_stats[c['clli']]
            w['u'] += c['units']
            w['a'] += c['addrs']
            w['x'] += c['capex']
            w['cl'] += 1
            if c['irr'] > 0:
                w['irr_s'] += c['irr'] * c['units']
                w['irr_n'] += c['units']
            q = c.get('quad', 'Q4')
            if 'Q1' in q: w['q1'] += 1
            elif 'Q2' in q: w['q2'] += 1
            elif 'Q3' in q: w['q3'] += 1
            else: w['q4'] += 1
            if c.get('ob_frac', 0) > 0:
                w['ob'] += 1

        wc_out = {}
        for clli, w in sorted(wc_stats.items()):
            avg_irr = round(w['irr_s'] / w['irr_n'], 1) if w['irr_n'] else 0
            cpp = round(w['x'] / w['u']) if w['u'] else 0
            wc_out[clli] = {
                'u': w['u'], 'a': w['a'], 'x': w['x'], 'cl': w['cl'],
                'irr': avg_irr, 'cpp': cpp,
                'q1': w['q1'], 'q2': w['q2'], 'q3': w['q3'], 'q4': w['q4'],
                'ob': w['ob'],
            }

        outpath = os.path.join(OUT, 'sr', f'{key}.js')
        with open(outpath, 'w', encoding='utf-8') as f:
            f.write(f'window._onSR("{key}",')
            json.dump({
                'wcs': wc_geojson,
                'centroids': centroids,
                'wc_stats': wc_out,
            }, f, separators=(',', ':'))
            f.write(');\n')

        size = os.path.getsize(outpath)
        print(f"  {key}.js: {size:,} bytes  ({len(wc_cllis)} WCs, {len(centroids):,} centroids)", flush=True)


def build_wc_files(clusters, polys, laid_map, obligs):
    """Build per-WC JS files with cluster polygons + compact address arrays."""
    print("Building per-WC files (streaming addresses)...", flush=True)

    # Group clusters by CLLI
    clli_clusters = defaultdict(list)
    for cid, c in clusters.items():
        clli_clusters[c['clli']].append((cid, c))

    # Build cluster index per WC: cluster_id -> index in array
    clli_cidx = {}
    for clli, cl_list in clli_clusters.items():
        cl_list.sort(key=lambda x: x[0])
        clli_cidx[clli] = {cid: i for i, (cid, _) in enumerate(cl_list)}

    # Build per-WC cluster data (without addresses)
    wc_data = {}
    for clli, cl_list in clli_clusters.items():
        meta = cl_list[0][1]
        wc_clusters = []
        for cid, c in cl_list:
            q_short = 'Q1' if 'Q1' in c.get('quad', '') else \
                      'Q2' if 'Q2' in c.get('quad', '') else \
                      'Q3' if 'Q3' in c.get('quad', '') else 'Q4'
            entry = {
                'id': cid.split('_H')[-1] if '_H' in cid else cid,
                'u': c['units'], 'a': c['addrs'], 'x': c['capex'],
                'cpp': c['cpp'], 'irr': c['irr'], 'cu': c['copper'],
                'urg': c.get('urg', 0), 'val': c.get('val', 0),
                'q': q_short,
                'ob': c.get('top_ob', 'DISCRETIONARY'),
                'of': c.get('ob_frac', 0),
                'au': {'S': c['sfu'], 'B': c['sbu'], 'M': c['mdu'], 'T': c['mtu']},
            }
            if cid in polys:
                entry['poly'] = polys[cid]
            wc_clusters.append(entry)
        wc_data[clli] = {
            'meta': {
                'region': meta['region'],
                'sub_region': meta['sub_region'],
                'density': meta['density'],
            },
            'clusters': wc_clusters,
            'addrs': [],
        }

    # Stream sorted address CSV and accumulate per-WC address arrays
    t0 = time.time()
    n = 0
    current_clli = None
    current_addrs = []

    def flush_wc(clli, addrs):
        if clli in wc_data:
            wc_data[clli]['addrs'] = addrs

    with open(ADDR_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            clli = row['CLLI']
            laid = row['LOCUS_ADDRESS_ID']

            if clli != current_clli:
                if current_clli and current_addrs:
                    flush_wc(current_clli, current_addrs)
                    # Write previous WC file immediately to free memory
                    write_wc_file(current_clli, wc_data.get(current_clli))
                current_clli = clli
                current_addrs = []

            # Compact address: [lat, lon, cluster_idx, aui_idx, units, cpp, irr, oblig_idx]
            try:
                lat = round(float(row['LATITUDE']), 5)
                lon = round(float(row['LONGITUDE']), 5)
            except (ValueError, KeyError):
                continue

            cluster_id = laid_map.get(laid, '')
            cidx = clli_cidx.get(clli, {}).get(cluster_id, -1)
            aui = AUI_IDX.get(row.get('AUI', 'SFU'), 0)
            units = int(row['NO_OF_UNITS']) if row.get('NO_OF_UNITS') else 1
            try:
                cpp = round(float(row['CPO_NTAS'])) if row.get('CPO_NTAS') else 0
            except ValueError:
                cpp = 0
            try:
                irr = round(float(row['COMPUTED_IRR']), 1) if row.get('COMPUTED_IRR') else 0
            except ValueError:
                irr = 0
            oblig = obligs.get(laid, 0)

            current_addrs.append([lat, lon, cidx, aui, units, cpp, irr, oblig])

            n += 1
            if n % 2000000 == 0:
                elapsed = time.time() - t0
                print(f"  ...{n:,} addresses ({elapsed:.0f}s)", flush=True)

    # Flush last WC
    if current_clli and current_addrs:
        flush_wc(current_clli, current_addrs)
        write_wc_file(current_clli, wc_data.get(current_clli))

    # Write any remaining WC files that had no addresses in the CSV
    for clli in sorted(wc_data):
        outpath = os.path.join(OUT, 'wc', f'{clli}.js')
        if not os.path.exists(outpath):
            write_wc_file(clli, wc_data[clli])

    elapsed = time.time() - t0
    wc_count = len([f for f in os.listdir(os.path.join(OUT, 'wc')) if f.endswith('.js')])
    total_size = sum(os.path.getsize(os.path.join(OUT, 'wc', f))
                     for f in os.listdir(os.path.join(OUT, 'wc')) if f.endswith('.js'))
    print(f"  {n:,} addresses -> {wc_count} WC files ({total_size/1e6:.1f} MB) in {elapsed:.0f}s", flush=True)


def write_wc_file(clli, data):
    """Write a single WC JS file."""
    if not data:
        return
    outpath = os.path.join(OUT, 'wc', f'{clli}.js')
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(f'window._onWC("{clli}",')
        # Add compact encoding maps
        data['aui_map'] = AUI_MAP
        data['oblig_map'] = OBLIG_MAP
        json.dump(data, f, separators=(',', ':'))
        f.write(');\n')
    # Free the address data from memory after writing
    data['addrs'] = []


if __name__ == '__main__':
    t0 = time.time()
    print("NON-OFS Cluster Explorer — Data Builder", flush=True)
    print(f"  Output: {OUT}", flush=True)

    # Step 1: Load all reference data
    clusters = load_cluster_data()
    laid_map = load_laid_to_cluster()
    obligs = load_obligations()
    polys = load_polygons()
    wc_features = load_wc_boundaries()

    # Step 2: Build summary.js
    build_summary(clusters, wc_features)

    # Step 3: Build sub-region files
    build_subregion_files(clusters, wc_features)

    # Step 4: Build per-WC files (streams 2.7 GB CSV)
    build_wc_files(clusters, polys, laid_map, obligs)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min", flush=True)
