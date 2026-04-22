"""
NON-OFS ILEC Master Clustering — Parallel by Sub-Region

Runs clustering for a specific set of sub-regions, writing results
to a partition-specific cache file. Multiple instances can run
simultaneously on different partitions.

Usage:
    python cluster_parallel.py --partition 1 --of 5
    python cluster_parallel.py --subregions "VA" "MD/DE" "MAN/BK/SI"
    python cluster_parallel.py --merge   # Merge all partition caches
"""
import csv, json, os, sys, time, traceback, gc
from collections import Counter

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m_sorted.csv')
FIPS_CSV = os.path.join(OUT, 'clli_county_fips_all.csv')
TIGER_DIR = os.path.join(OUT, 'TIGER')
SR_MAP = os.path.join(OUT, 'clli_subregion_map.json')

# Existing checkpoint from the first 150 WCs
MAIN_CHECKPOINT = os.path.join(OUT, 'v6_checkpoint.json')

from road_graph import RoadGraph
from telecom_clustering_v6 import cluster_addresses_v6, get_morphology_params, haversine_ft

COPPER_SALVAGE_PER_CIRCUIT = 200

# Sub-region partitions — balanced by CLLI count (~385 each for 5 partitions)
PARTITIONS = {
    1: ['NY Upstate', 'MAN/BK/SI'],                    # 262 + 36 = 298
    2: ['WE/CE PA', 'QNS/BRX/LI'],                     # 267 + 74 = 341
    3: ['VA', 'Eastern MA'],                             # 244 + 88 = 332
    4: ['WE/CE MA RI', 'NJ South/Phila'],               # 183 + 124 = 307
    5: ['MD/DE', 'EN PA', 'WE MD/DC Metro', 'NY Midstate', 'NJ North'],  # 178+178+104+100+90 = 650
}


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


def stream_wcs(target_cllis):
    """Stream sorted CSV yielding only WCs in target_cllis."""
    current_clli = None
    current_addrs = []
    current_meta = None

    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            clli = row['CLLI']

            if clli != current_clli:
                if current_clli is not None and current_clli in target_cllis:
                    yield current_clli, current_meta, current_addrs
                current_clli = clli
                current_addrs = [] if clli in target_cllis else None
                current_meta = (
                    row.get('REGION', ''), row.get('SUB_REGION', ''),
                    row.get('MARKET_DENSITY', 'SUBURBAN'), row.get('STATE', ''),
                )

            if current_addrs is not None:
                current_addrs.append(parse_addr(row))

    if current_clli is not None and current_clli in target_cllis and current_addrs:
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


def run_partition(partition_id, subregions):
    """Run clustering for specific sub-regions."""
    tag = f"P{partition_id}"
    cache_file = os.path.join(OUT, f'v6_clusters_P{partition_id}.json')
    checkpoint_file = os.path.join(OUT, f'v6_checkpoint_P{partition_id}.json')

    # Load CLLI -> sub-region map
    with open(SR_MAP) as f:
        clli_sr = json.load(f)

    # Get target CLLIs for these sub-regions
    target_cllis = set(c for c, sr in clli_sr.items() if sr in subregions)

    # Exclude already-completed CLLIs from the main run
    already_done = set()
    if os.path.exists(MAIN_CHECKPOINT):
        with open(MAIN_CHECKPOINT) as f:
            already_done = set(json.load(f).get('completed', []))

    # Also check partition checkpoint for resume
    partition_done = set()
    partition_clusters = []
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            pd = json.load(f)
            partition_done = set(pd.get('completed', []))
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                partition_clusters = json.load(f)

    skip = already_done | partition_done
    target_cllis -= skip
    print(f"[{tag}] Sub-regions: {subregions}", flush=True)
    print(f"[{tag}] Target CLLIs: {len(target_cllis) + len(partition_done)} "
          f"(skip {len(skip & set(c for c, sr in clli_sr.items() if sr in subregions))} already done, "
          f"{len(target_cllis)} remaining)", flush=True)

    if not target_cllis:
        print(f"[{tag}] Nothing to do!", flush=True)
        return

    clli_to_fips = load_fips_map()

    t0 = time.time()
    all_clusters = list(partition_clusters)
    completed = set(partition_done)
    wi = 0
    failed = []
    n_total = len(target_cllis)

    for clli, meta, addrs in stream_wcs(target_cllis):
        if clli in completed:
            continue

        region, sub_region, mkt, state = meta
        n_addrs = len(addrs)
        t1 = time.time()

        try:
            clusters = cluster_wc(clli, addrs, clli_to_fips, region, sub_region, mkt)
            elapsed = time.time() - t1
            all_clusters.extend(clusters)
            completed.add(clli)

            if (wi + 1) % 10 == 0 or (wi + 1) == n_total:
                tel = time.time() - t0
                rate = (wi + 1) / tel * 60
                eta = (n_total - wi - 1) / rate if rate > 0 else 0
                print(f"  [{tag}][{wi+1}/{n_total}] {clli} ({mkt[:4]}): "
                      f"{n_addrs:,} -> {len(clusters)} hubs ({elapsed:.0f}s) | "
                      f"{len(all_clusters):,} clusters, "
                      f"{rate:.1f}/min, ETA {eta:.0f}m", flush=True)

        except Exception as e:
            elapsed = time.time() - t1
            print(f"  [{tag}][{wi+1}/{n_total}] {clli}: FAILED ({elapsed:.0f}s): {e}", flush=True)
            traceback.print_exc()
            failed.append((clli, str(e)))
            try:
                c = make_passthrough(clli, addrs, region, sub_region, mkt)
                all_clusters.append(c)
                completed.add(clli)
            except Exception:
                pass

        del addrs
        wi += 1

        if wi % 25 == 0:
            gc.collect()
            with open(checkpoint_file, 'w') as f:
                json.dump({'completed': sorted(completed),
                           'n_clusters': len(all_clusters),
                           'subregions': subregions,
                           'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')}, f, indent=2)
            with open(cache_file, 'w') as f:
                json.dump(all_clusters, f)
            print(f"  [{tag}] -- checkpoint ({len(completed)} WCs, "
                  f"{len(all_clusters):,} clusters) --", flush=True)

    # Final save
    with open(checkpoint_file, 'w') as f:
        json.dump({'completed': sorted(completed),
                   'n_clusters': len(all_clusters),
                   'subregions': subregions,
                   'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')}, f, indent=2)
    with open(cache_file, 'w') as f:
        json.dump(all_clusters, f)

    tel = time.time() - t0
    tu = sum(c['total_units'] for c in all_clusters)
    print(f"\n[{tag}] COMPLETE: {len(completed)} WCs, {len(all_clusters):,} clusters, "
          f"{tu:,} units, {tel/60:.1f} min", flush=True)
    if failed:
        print(f"[{tag}] Failed: {[c for c, e in failed]}", flush=True)


def merge_partitions():
    """Merge all partition caches + original checkpoint into final v6_clusters_cache.json."""
    print("Merging partition caches...", flush=True)

    all_clusters = []
    all_completed = set()

    # Load original checkpoint clusters (first 150 WCs)
    main_cache = os.path.join(OUT, 'v6_clusters_cache.json')
    if os.path.exists(main_cache):
        with open(main_cache) as f:
            orig = json.load(f)
        # Get CLLIs from original clusters
        orig_cllis = set(c['clli'] for c in orig)
        all_clusters.extend(orig)
        all_completed.update(orig_cllis)
        print(f"  Original: {len(orig):,} clusters from {len(orig_cllis)} WCs", flush=True)

    # Load each partition
    for pid in sorted(PARTITIONS.keys()):
        cache_file = os.path.join(OUT, f'v6_clusters_P{pid}.json')
        cp_file = os.path.join(OUT, f'v6_checkpoint_P{pid}.json')
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                pclusters = json.load(f)
            # Don't double-count CLLIs already in original
            new_clusters = [c for c in pclusters if c['clli'] not in all_completed]
            new_cllis = set(c['clli'] for c in new_clusters)
            all_clusters.extend(new_clusters)
            all_completed.update(new_cllis)
            print(f"  Partition {pid}: {len(new_clusters):,} new clusters from "
                  f"{len(new_cllis)} WCs", flush=True)

    # Write merged output
    with open(main_cache, 'w') as f:
        json.dump(all_clusters, f)

    # Write merged checkpoint
    main_cp = os.path.join(OUT, 'v6_checkpoint.json')
    with open(main_cp, 'w') as f:
        json.dump({
            'completed': sorted(all_completed),
            'n_clusters': len(all_clusters),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        }, f, indent=2)

    tu = sum(c['total_units'] for c in all_clusters)
    ta = sum(c['total_addrs'] for c in all_clusters)
    print(f"\nMerged: {len(all_completed)} WCs, {len(all_clusters):,} clusters, "
          f"{tu:,} units, {ta:,} addresses", flush=True)

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
            w.writerow([c['cluster_id'], c['clli'], c.get('region', ''),
                         c.get('sub_region', ''), c.get('market_density', ''),
                         f"{c['lat']:.6f}", f"{c['lon']:.6f}",
                         c['total_units'], c['total_addrs'],
                         f"{c.get('total_capex', 0):.0f}", f"{c.get('avg_cpp', 0):.0f}",
                         f"{c.get('median_irr', 0):.2f}", c.get('copper_circuits', 0),
                         f"{c.get('avg_formation_score', 0):.1f}",
                         aui.get('SFU', 0), aui.get('SBU', 0),
                         aui.get('MDU', 0), aui.get('MTU', 0)])
    print(f"  Summary CSV: {sp}", flush=True)


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--merge' in args:
        merge_partitions()
    elif '--partition' in args:
        idx = args.index('--partition')
        pid = int(args[idx + 1])
        subregions = PARTITIONS[pid]
        run_partition(pid, subregions)
    elif '--subregions' in args:
        idx = args.index('--subregions')
        srs = args[idx + 1:]
        run_partition(0, srs)
    else:
        print("Usage:")
        print("  python cluster_parallel.py --partition 1  (run partition 1)")
        print("  python cluster_parallel.py --merge        (merge all partitions)")
        print()
        print("Partitions:")
        for pid, srs in PARTITIONS.items():
            total = sum(1 for c, sr in json.load(open(SR_MAP)).items() if sr in srs)
            print(f"  {pid}: {srs} ({total} CLLIs)")

    print("\nDone.", flush=True)
