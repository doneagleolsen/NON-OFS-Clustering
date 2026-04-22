"""
NON-OFS ILEC Master Clustering — ALL 12.3M Addresses (v6)

Memory-optimized: streams sorted CSV one WC at a time.
Requires: all_nonofs_12m_sorted.csv (sorted by CLLI)

Usage:
    python cluster_all_nonofs.py                  # Full run (all WCs)
    python cluster_all_nonofs.py --clli KGTNPAES  # Single WC test
    python cluster_all_nonofs.py --resume          # Resume from checkpoint
"""
import csv, json, os, sys, time, traceback, gc
from collections import Counter, defaultdict

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m_sorted.csv')
FIPS_CSV = os.path.join(OUT, 'clli_county_fips_all.csv')
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')
CHECKPOINT_FILE = os.path.join(OUT, 'v6_checkpoint.json')
TIGER_DIR = os.path.join(OUT, 'TIGER')

from road_graph import RoadGraph
from telecom_clustering_v6 import cluster_addresses_v6, get_morphology_params, haversine_ft

COPPER_SALVAGE_PER_CIRCUIT = 200


def load_fips_map():
    clli_to_fips = {}
    with open(FIPS_CSV) as f:
        for row in csv.DictReader(f):
            clli_to_fips[row['CLLI']] = {
                'primary': row['PRIMARY_FIPS'],
                'all': row['ALL_FIPS'].split(';') if row['ALL_FIPS'] else [],
            }
    return clli_to_fips


def parse_addr(row):
    """Parse CSV row dict into minimal address dict for clustering."""
    copper_cir = int(row['COPPER_CIR_COUNT']) if row['COPPER_CIR_COUNT'] else 0
    units = int(row['NO_OF_UNITS']) if row['NO_OF_UNITS'] else 1
    return {
        'laid': row['LOCUS_ADDRESS_ID'],
        'lat': float(row['LATITUDE']),
        'lon': float(row['LONGITUDE']),
        'units': max(units, 1),
        'aui': row['AUI'] or 'SFU',
        'irr_v2': float(row['COMPUTED_IRR']) if row['COMPUTED_IRR'] else 0,
        'copper_salvage': copper_cir * COPPER_SALVAGE_PER_CIRCUIT,
        'priority_rank': float(row['PRIORITY_RANK']) if row['PRIORITY_RANK'] else None,
        'copper_cir': copper_cir,
        'cpo_ntas': float(row['CPO_NTAS']) if row['CPO_NTAS'] else 0,
        'total_capex': float(row['TOTAL_CAPEX']) if row['TOTAL_CAPEX'] else 0,
        'cofs': float(row['COFS_PRED_SCORE']) if row['COFS_PRED_SCORE'] else 0,
        'market_density': row['MARKET_DENSITY'] or 'SUBURBAN',
    }


def stream_wcs():
    """Stream sorted CSV yielding (clli, meta, addrs_list) one WC at a time.
    Peak memory: addresses for 1 WC only (~50K max).
    """
    current_clli = None
    current_addrs = []
    current_meta = None

    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            clli = row['CLLI']

            if clli != current_clli:
                if current_clli is not None:
                    yield current_clli, current_meta, current_addrs
                current_clli = clli
                current_addrs = []
                current_meta = (
                    row.get('REGION', ''), row.get('SUB_REGION', ''),
                    row.get('MARKET_DENSITY', 'SUBURBAN'), row.get('STATE', ''),
                )

            current_addrs.append(parse_addr(row))

    if current_clli is not None:
        yield current_clli, current_meta, current_addrs


def load_merged_roads(clli, addr_bbox, clli_to_fips):
    if clli not in clli_to_fips:
        return None

    fips_info = clli_to_fips[clli]
    paths = []
    for fips in fips_info['all']:
        path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        if os.path.exists(path):
            paths.append(path)

    if not paths:
        path = os.path.join(TIGER_DIR, f'roads_{fips_info["primary"]}.geojson')
        if os.path.exists(path):
            paths = [path]
        else:
            return None

    if len(paths) == 1:
        return RoadGraph(paths[0], bbox=addr_bbox)

    all_features = []
    for p in paths:
        try:
            with open(p) as f:
                gj = json.load(f)
            all_features.extend(gj.get('features', []))
        except Exception:
            pass

    if not all_features:
        return None

    merged_path = os.path.join(OUT, f'_merged_roads_{clli}.geojson')
    with open(merged_path, 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': all_features}, f)

    rg = RoadGraph(merged_path, bbox=addr_bbox)
    try:
        os.remove(merged_path)
    except Exception:
        pass
    return rg


def make_passthrough(clli, addrs, region, sub_region, mkt):
    total_units = sum(a['units'] for a in addrs)
    total_capex = sum(a.get('total_capex', 0) or a.get('cpo_ntas', 0) * a['units'] for a in addrs)
    aui = Counter(a['aui'] for a in addrs for _ in range(a['units']))
    lat = sum(a['lat'] for a in addrs) / len(addrs)
    lon = sum(a['lon'] for a in addrs) / len(addrs)
    irr_vals = sorted(a['irr_v2'] for a in addrs if a['irr_v2'] > 0)
    copper = sum(a['copper_cir'] for a in addrs)
    scores = [a.get('fin_score', 0) for a in addrs if a.get('fin_score', 0) > 0]

    return {
        'cluster_id': f'{clli}_H000', 'hub_id': 0, 'clli': clli,
        'region': region, 'sub_region': sub_region, 'market_density': mkt,
        'lat': lat, 'lon': lon,
        'total_units': total_units, 'total_addrs': len(addrs),
        'total_capex': total_capex, 'avg_cpp': total_capex / max(total_units, 1),
        'median_irr': irr_vals[len(irr_vals)//2] if irr_vals else 0,
        'copper_circuits': copper,
        'avg_formation_score': sum(scores) / len(scores) if scores else 0,
        'aui_units': dict(aui),
        'addresses': [a['laid'] for a in addrs],
    }


def build_cluster(hid, centroid, rg_units, clli, region, sub_region, mkt):
    all_addrs = []
    total_units = 0; total_capex = 0; copper = 0
    aui = Counter(); irr_vals = []; scores = []

    for rg in rg_units.values():
        if rg['hub_id'] != hid:
            continue
        for a in rg['addrs']:
            all_addrs.append(a)
            u = a['units']
            total_units += u
            capex = a.get('total_capex', 0) or a.get('cpo_ntas', 0) * u
            total_capex += capex
            aui[a['aui']] += u
            if a.get('irr_v2', 0) > 0: irr_vals.append(a['irr_v2'])
            copper += a.get('copper_cir', 0)
            if a.get('fin_score', 0) > 0: scores.append(a['fin_score'])

    lat, lon = centroid
    irr_vals.sort()
    return {
        'cluster_id': f'{clli}_H{hid:03d}', 'hub_id': hid, 'clli': clli,
        'region': region, 'sub_region': sub_region, 'market_density': mkt,
        'lat': lat, 'lon': lon,
        'total_units': total_units, 'total_addrs': len(all_addrs),
        'total_capex': total_capex, 'avg_cpp': total_capex / max(total_units, 1),
        'median_irr': irr_vals[len(irr_vals)//2] if irr_vals else 0,
        'copper_circuits': copper,
        'avg_formation_score': sum(scores) / len(scores) if scores else 0,
        'aui_units': dict(aui),
        'addresses': [a['laid'] for a in all_addrs],
    }


def cluster_wc(clli, addrs, clli_to_fips, region, sub_region, mkt):
    if len(addrs) < 3:
        return [make_passthrough(clli, addrs, region, sub_region, mkt)]

    try:
        params = get_morphology_params(mkt)
    except KeyError:
        params = get_morphology_params('SUBURBAN')

    bbox = (min(a['lat'] for a in addrs), min(a['lon'] for a in addrs),
            max(a['lat'] for a in addrs), max(a['lon'] for a in addrs))

    rg = load_merged_roads(clli, bbox, clli_to_fips)
    if not rg:
        return [make_passthrough(clli, addrs, region, sub_region, mkt)]

    rg.snap_addresses(addrs)
    hub_centroids, rg_units, _ = cluster_addresses_v6(addrs, rg, params)

    clusters = [build_cluster(hid, hub_centroids[hid], rg_units, clli, region, sub_region, mkt)
                for hid in hub_centroids]
    return clusters if clusters else [make_passthrough(clli, addrs, region, sub_region, mkt)]


def save_checkpoint(completed, all_clusters):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({'completed': sorted(completed),
                    'n_clusters': len(all_clusters),
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')}, f, indent=2)
    with open(CLUSTER_CACHE, 'w') as f:
        json.dump(all_clusters, f)


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set(), []
    with open(CHECKPOINT_FILE) as f:
        cp = json.load(f)
    clusters = []
    if os.path.exists(CLUSTER_CACHE):
        with open(CLUSTER_CACHE) as f:
            clusters = json.load(f)
    return set(cp['completed']), clusters


def run_all(target_cllis=None, resume=False):
    clli_to_fips = load_fips_map()
    print(f"FIPS map: {len(clli_to_fips)} CLLIs", flush=True)

    completed = set()
    all_clusters = []
    if resume:
        completed, all_clusters = load_checkpoint()
        if completed:
            print(f"Resuming: {len(completed)} WCs done, "
                  f"{len(all_clusters):,} clusters", flush=True)

    target_set = set(target_cllis) if target_cllis else None
    t0 = time.time()
    wi = 0
    n_remaining = 0
    failed = []
    no_roads = []
    total_addrs = 0

    # First pass: count remaining WCs
    print("Counting WCs...", flush=True)
    wc_counts = Counter()
    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            clli = row['CLLI']
            if target_set and clli not in target_set:
                continue
            wc_counts[clli] += 1
    n_remaining = len([c for c in wc_counts if c not in completed])
    total_addrs = sum(wc_counts[c] for c in wc_counts if c not in completed)
    print(f"  {n_remaining} WCs to process, {total_addrs:,} addresses", flush=True)

    print(f"\n{'='*70}", flush=True)
    print(f"V6 Clustering — streaming one WC at a time", flush=True)
    print(f"{'='*70}", flush=True)

    clustered_addrs = 0
    for clli, meta, addrs in stream_wcs():
        if target_set and clli not in target_set:
            continue
        if clli in completed:
            continue

        region, sub_region, mkt, state = meta
        n_addrs = len(addrs)

        t1 = time.time()
        try:
            clusters = cluster_wc(clli, addrs, clli_to_fips, region, sub_region, mkt)
            elapsed = time.time() - t1
            n_hubs = len(clusters)

            if n_hubs == 1 and n_addrs >= 3 and clusters[0]['cluster_id'].endswith('_H000'):
                no_roads.append(clli)

            all_clusters.extend(clusters)
            completed.add(clli)
            clustered_addrs += n_addrs

            if (wi + 1) % 25 == 0 or (wi + 1) == n_remaining:
                tel = time.time() - t0
                rate = (wi + 1) / tel * 60
                eta = (n_remaining - wi - 1) / rate if rate > 0 else 0
                pct = clustered_addrs / max(total_addrs, 1) * 100
                print(f"  [{wi+1}/{n_remaining}] {clli} ({mkt[:4]}): "
                      f"{n_addrs:,} -> {n_hubs} hubs ({elapsed:.0f}s) | "
                      f"{len(all_clusters):,} clusters, {pct:.0f}%, "
                      f"{rate:.1f}/min, ETA {eta:.0f}m", flush=True)
            elif (wi + 1) % 5 == 0:
                print(f"  [{wi+1}/{n_remaining}] {clli}: "
                      f"{n_addrs:,} -> {n_hubs} ({elapsed:.0f}s)", flush=True)

        except Exception as e:
            elapsed = time.time() - t1
            print(f"  [{wi+1}/{n_remaining}] {clli}: FAILED ({elapsed:.0f}s): {e}", flush=True)
            traceback.print_exc()
            failed.append((clli, str(e)))
            try:
                c = make_passthrough(clli, addrs, region, sub_region, mkt)
                all_clusters.append(c)
                completed.add(clli)
                clustered_addrs += n_addrs
            except Exception:
                pass

        # Free WC memory
        del addrs
        wi += 1

        if wi % 50 == 0:
            gc.collect()
            save_checkpoint(completed, all_clusters)
            print(f"  -- checkpoint ({len(completed)} WCs, "
                  f"{len(all_clusters):,} clusters) --", flush=True)

    save_checkpoint(completed, all_clusters)

    tel = time.time() - t0
    tu = sum(c['total_units'] for c in all_clusters)
    ta = sum(c['total_addrs'] for c in all_clusters)

    print(f"\n{'='*70}", flush=True)
    print(f"V6 CLUSTERING COMPLETE", flush=True)
    print(f"  WCs:      {len(completed):,}", flush=True)
    print(f"  Clusters: {len(all_clusters):,}", flush=True)
    print(f"  Units:    {tu:,}", flush=True)
    print(f"  Addrs:    {ta:,}", flush=True)
    print(f"  Failed:   {len(failed)}", flush=True)
    print(f"  No-road:  {len(no_roads)} (passthrough)", flush=True)
    print(f"  Time:     {tel/60:.1f} min", flush=True)

    if failed:
        print(f"\n  Failed:", flush=True)
        for c, e in failed[:20]:
            print(f"    {c}: {e}", flush=True)

    if all_clusters:
        sizes = sorted(c['total_units'] for c in all_clusters)
        n = len(sizes)
        print(f"\n  Hub sizes: min={sizes[0]}, p10={sizes[n//10]}, "
              f"med={sizes[n//2]}, p90={sizes[n*9//10]}, max={sizes[-1]}", flush=True)

    # Write summary CSV
    sp = os.path.join(OUT, 'v6_cluster_summary.csv')
    with open(sp, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['cluster_id', 'clli', 'region', 'sub_region', 'market_density',
                     'lat', 'lon', 'total_units', 'total_addrs', 'total_capex',
                     'avg_cpp', 'median_irr', 'copper_circuits', 'avg_formation_score',
                     'aui_SFU', 'aui_SBU', 'aui_MDU', 'aui_MTU'])
        for c in all_clusters:
            aui = c.get('aui_units', {})
            w.writerow([c['cluster_id'], c['clli'], c.get('region',''),
                         c.get('sub_region',''), c.get('market_density',''),
                         f"{c['lat']:.6f}", f"{c['lon']:.6f}",
                         c['total_units'], c['total_addrs'],
                         f"{c.get('total_capex',0):.0f}", f"{c.get('avg_cpp',0):.0f}",
                         f"{c.get('median_irr',0):.2f}", c.get('copper_circuits',0),
                         f"{c.get('avg_formation_score',0):.1f}",
                         aui.get('SFU',0), aui.get('SBU',0),
                         aui.get('MDU',0), aui.get('MTU',0)])
    print(f"\n  Summary: {sp}", flush=True)

    return all_clusters, failed


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--clli' in args:
        idx = args.index('--clli')
        target = args[idx + 1].split(',')
        clusters, failed = run_all(target_cllis=target)
        if clusters:
            print(f"\nClusters:")
            for c in clusters:
                print(f"  {c['cluster_id']}: {c['total_units']} units, "
                      f"${c.get('avg_cpp',0):.0f}/unit, "
                      f"IRR={c.get('median_irr',0):.1f}%")
    else:
        resume = '--resume' in args
        clusters, failed = run_all(resume=resume)

    print("\nDone.", flush=True)
