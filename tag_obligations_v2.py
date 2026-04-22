"""
Post-Formation Obligation Tagging — V2

Tags each address with an obligation bucket (9 types, priority-ordered).
Then aggregates to cluster level as obligation_fill.

Runs AFTER clustering — does not influence cluster boundaries.

Obligation buckets (priority order):
  1. COP_2026_OBLIG  — copper recycling obligation, start date in 2026
  2. COP_2026_PT     — copper recycling pull-through, start date in 2026
  3. COP_2027_OBLIG  — copper recycling obligation, start date in 2027
  4. COP_2027_PT     — copper recycling pull-through, start date in 2027
  5. SBB_OBLIG       — state broadband obligation
  6. SBB_PT          — state broadband pull-through
  7. NSI_OBLIG       — NSI (National Security Interest) obligation
  8. LFA_OBLIG       — Local Franchise Authority obligation
  9. DISCRETIONARY   — no obligation

Usage:
    python tag_obligations_v2.py
"""
import csv, json, os, sys, time
from collections import Counter, defaultdict

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m.csv')
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')

# NSI bridge table: maps NSI ADDRESS_ID -> LOCUS_ADDRESS_ID
# Via GPSAA.NTAS_LOCUS_MAP (42K NON_OFS matches)


def load_nsi_bridge():
    """Query Oracle for NSI -> LOCUS_ADDRESS_ID bridge via NTAS_LOCUS_MAP."""
    print("Loading NSI bridge from Oracle...", flush=True)
    import oracledb
    conn = oracledb.connect(user="tableau_user", password="Verizon1#",
                            dsn="f1btpap-scan.verizon.com:1521/NARPROD")
    cur = conn.cursor()

    # Get NSI addresses that bridge to NON_OFS via NTAS_LOCUS_MAP
    cur.execute("""
        SELECT DISTINCT m.LOCUS_ADDRESS_ID
        FROM NSEEPRD.NSI_INQUIRIES n
        INNER JOIN GPSAA.NTAS_LOCUS_MAP m
          ON n.ADDRESS_ID = TO_CHAR(m.ADDRESS_ID)
        INNER JOIN GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
          ON m.LOCUS_ADDRESS_ID = ak.LOCUS_ADDRESS_ID
        WHERE ak.CATEGORY = 'NON_OFS'
          AND n.ADDRESS_ID != '0'
    """)
    nsi_laids = set()
    for row in cur.fetchall():
        nsi_laids.add(str(row[0]))
    print(f"  NSI bridge: {len(nsi_laids):,} NON_OFS addresses", flush=True)

    # Get EWO pipeline addresses (direct ADDRESS_ID match)
    cur.execute("""
        SELECT DISTINCT p.ADDRESS_ID
        FROM GPSAA.EWO_W_CMTDATE_NODUP_PIPELINE_2026 p
        INNER JOIN GPSAA.NTAS_LOCUS_MAP m
          ON p.ADDRESS_ID = TO_CHAR(m.ADDRESS_ID)
        INNER JOIN GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
          ON m.LOCUS_ADDRESS_ID = ak.LOCUS_ADDRESS_ID
        WHERE ak.CATEGORY = 'NON_OFS'
    """)
    ewo_laids = set()
    for row in cur.fetchall():
        ewo_laids.add(str(row[0]))
    print(f"  EWO pipeline bridge: {len(ewo_laids):,} NON_OFS addresses", flush=True)

    # Get LFA addresses
    cur.execute("""
        SELECT DISTINCT b.LOCUS_ADDRESS_ID
        FROM GPSAA.S_BEYOND_INFINITY_RANKING_SCORING b
        INNER JOIN GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
          ON b.LOCUS_ADDRESS_ID = ak.LOCUS_ADDRESS_ID
        WHERE ak.CATEGORY = 'NON_OFS'
          AND ak.COUNTY_TYPE = 'LFA'
    """)
    lfa_laids = set()
    for row in cur.fetchall():
        lfa_laids.add(str(row[0]))
    print(f"  LFA addresses: {len(lfa_laids):,}", flush=True)

    cur.close()
    conn.close()

    return nsi_laids, ewo_laids, lfa_laids


def tag_address(addr, nsi_laids, lfa_laids):
    """Assign obligation bucket to a single address. Returns bucket name."""
    laid = addr.get('laid', '')
    copper_recycling = addr.get('planned_copper_recycling', '')
    copper_start = addr.get('copper_recycling_start', '')
    wc_sbb = addr.get('wc_sbb_flag', '')
    addr_sbb = addr.get('addr_sbb_flag', '')

    # Parse copper recycling year
    cop_year = None
    if copper_start:
        try:
            # Format: YYYY-MM-DD or DD-MON-YY etc.
            if '-' in copper_start:
                cop_year = int(copper_start[:4])
            elif '/' in copper_start:
                parts = copper_start.split('/')
                cop_year = int(parts[-1])
                if cop_year < 100:
                    cop_year += 2000
        except (ValueError, IndexError):
            pass

    is_copper = copper_recycling in ('Y', 'Yes', '1') or cop_year is not None
    is_sbb = wc_sbb in ('Y', 'Yes', '1') or addr_sbb in ('Y', 'Yes', '1')
    is_nsi = laid in nsi_laids
    is_lfa = laid in lfa_laids

    # Priority ordering (first match wins)
    if is_copper and cop_year and cop_year <= 2026:
        return 'COP_2026_OBLIG'
    if is_copper and cop_year and cop_year == 2027:
        return 'COP_2027_OBLIG'
    if is_copper and (cop_year is None or cop_year > 2027):
        return 'COP_FUTURE_OBLIG'
    if is_sbb:
        return 'SBB_OBLIG'
    if is_nsi:
        return 'NSI_OBLIG'
    if is_lfa:
        return 'LFA_OBLIG'

    return 'DISCRETIONARY'


def tag_all_addresses():
    """Tag all 12.3M addresses with obligation bucket."""
    nsi_laids, ewo_laids, lfa_laids = load_nsi_bridge()

    print(f"\nTagging addresses from {ADDR_CSV}...", flush=True)
    t0 = time.time()

    bucket_counts = Counter()
    addr_buckets = {}  # laid -> bucket
    n = 0

    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            laid = row['LOCUS_ADDRESS_ID']
            addr = {
                'laid': laid,
                'planned_copper_recycling': row['PLANNED_COPPER_RECYCLING'] or '',
                'copper_recycling_start': row['COPPER_RECYCLING_START_DATE'] or '',
                'wc_sbb_flag': row['WC_SBB_FLAG'] or '',
                'addr_sbb_flag': row['ADDR_SBB_FLAG'] or '',
            }
            bucket = tag_address(addr, nsi_laids, lfa_laids)
            bucket_counts[bucket] += 1
            addr_buckets[laid] = bucket
            n += 1
            if n % 3000000 == 0:
                print(f"  ...{n:,} addresses tagged", flush=True)

    elapsed = time.time() - t0
    print(f"\n  Tagged {n:,} addresses in {elapsed:.0f}s", flush=True)
    print(f"\n  Obligation distribution:", flush=True)
    for bucket in ['COP_2026_OBLIG', 'COP_2027_OBLIG', 'COP_FUTURE_OBLIG',
                    'SBB_OBLIG', 'NSI_OBLIG', 'LFA_OBLIG', 'DISCRETIONARY']:
        cnt = bucket_counts.get(bucket, 0)
        pct = cnt / n * 100 if n > 0 else 0
        print(f"    {bucket:<20} {cnt:>10,} ({pct:.1f}%)", flush=True)

    return addr_buckets


def apply_to_clusters(addr_buckets):
    """Apply obligation tags to cluster cache. Adds obligation_fill per cluster."""
    print(f"\nApplying tags to clusters...", flush=True)

    if not os.path.exists(CLUSTER_CACHE):
        print(f"  ERROR: {CLUSTER_CACHE} not found. Run clustering first.", flush=True)
        return None

    with open(CLUSTER_CACHE) as f:
        clusters = json.load(f)
    print(f"  Loaded {len(clusters):,} clusters", flush=True)

    tagged = 0
    for c in clusters:
        oblig_fill = Counter()
        for laid in c.get('addresses', []):
            bucket = addr_buckets.get(str(laid), 'DISCRETIONARY')
            oblig_fill[bucket] += 1
        c['obligation_fill'] = dict(oblig_fill)

        # Highest obligation tier in this cluster
        priority = ['COP_2026_OBLIG', 'COP_2027_OBLIG', 'COP_FUTURE_OBLIG',
                     'SBB_OBLIG', 'NSI_OBLIG', 'LFA_OBLIG', 'DISCRETIONARY']
        c['top_obligation'] = 'DISCRETIONARY'
        for p in priority:
            if oblig_fill.get(p, 0) > 0:
                c['top_obligation'] = p
                break

        # Obligation fraction (non-discretionary / total)
        total = sum(oblig_fill.values())
        non_disc = total - oblig_fill.get('DISCRETIONARY', 0)
        c['obligation_fraction'] = round(non_disc / max(total, 1), 3)

        tagged += 1

    # Save updated clusters
    with open(CLUSTER_CACHE, 'w') as f:
        json.dump(clusters, f)
    print(f"  Updated {tagged:,} clusters in {CLUSTER_CACHE}", flush=True)

    # Summary
    top_counts = Counter(c['top_obligation'] for c in clusters)
    print(f"\n  Cluster top-obligation distribution:", flush=True)
    for bucket in ['COP_2026_OBLIG', 'COP_2027_OBLIG', 'COP_FUTURE_OBLIG',
                    'SBB_OBLIG', 'NSI_OBLIG', 'LFA_OBLIG', 'DISCRETIONARY']:
        cnt = top_counts.get(bucket, 0)
        print(f"    {bucket:<20} {cnt:>6,} clusters", flush=True)

    return clusters


def write_addr_obligations_csv(addr_buckets):
    """Write address-level obligation tags to CSV for downstream joins."""
    out_path = os.path.join(OUT, 'addr_obligation_tags.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['LOCUS_ADDRESS_ID', 'OBLIGATION_BUCKET'])
        for laid, bucket in addr_buckets.items():
            w.writerow([laid, bucket])
    print(f"  Written: {out_path} ({len(addr_buckets):,} rows)", flush=True)


if __name__ == '__main__':
    addr_buckets = tag_all_addresses()
    write_addr_obligations_csv(addr_buckets)
    apply_to_clusters(addr_buckets)
    print("\nDone.", flush=True)
