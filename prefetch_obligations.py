"""
Pre-fetch Obligation Data from Oracle + Tag Addresses
======================================================
Standalone script that runs BEFORE clustering.
Queries Oracle for NSI / EWO / LFA address sets, saves intermediate CSVs,
then tags all 12.3M addresses in all_nonofs_12m.csv with obligation buckets.

Output:
  - nsi_bridge_laids.csv        (LOCUS_ADDRESS_ID for NSI-matched NON_OFS)
  - ewo_pipeline_laids.csv      (LOCUS_ADDRESS_ID for EWO copper recycling pipeline)
  - lfa_laids.csv               (LOCUS_ADDRESS_ID for LFA county type)
  - addr_obligation_tags.csv    (LOCUS_ADDRESS_ID, OBLIGATION_BUCKET)

Run with system Python:
  C:/Users/v267429/AppData/Local/Programs/Python/Python311/python.exe prefetch_obligations.py
"""
import csv, os, sys, time
from collections import Counter

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m.csv')

# ---------------------------------------------------------------------------
# Oracle queries
# ---------------------------------------------------------------------------

def fetch_oracle_data():
    """Connect to Oracle and fetch NSI, EWO, LFA address sets."""
    import oracledb
    print("Connecting to Oracle...", flush=True)
    conn = oracledb.connect(
        user="tableau_user",
        password="Verizon1#",
        dsn="f1btpap-scan.verizon.com:1521/NARPROD"
    )
    cur = conn.cursor()

    # --- 1. NSI bridge: NSI_INQUIRIES -> NTAS_LOCUS_MAP -> G_ALL_LOCUS_ADDRESS_KEY ---
    print("\n[1/3] Fetching NSI bridge LAIDs...", flush=True)
    t0 = time.time()
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
    elapsed = time.time() - t0
    print(f"  NSI bridge: {len(nsi_laids):,} NON_OFS addresses ({elapsed:.0f}s)", flush=True)

    # Save intermediate
    nsi_path = os.path.join(OUT, 'nsi_bridge_laids.csv')
    with open(nsi_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['LOCUS_ADDRESS_ID'])
        for laid in sorted(nsi_laids):
            w.writerow([laid])
    print(f"  Saved: {nsi_path}", flush=True)

    # --- 2. EWO pipeline: copper recycling committed addresses ---
    print("\n[2/3] Fetching EWO pipeline LAIDs...", flush=True)
    t0 = time.time()
    cur.execute("""
        SELECT DISTINCT m.LOCUS_ADDRESS_ID
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
    elapsed = time.time() - t0
    print(f"  EWO pipeline: {len(ewo_laids):,} NON_OFS addresses ({elapsed:.0f}s)", flush=True)

    # Save intermediate
    ewo_path = os.path.join(OUT, 'ewo_pipeline_laids.csv')
    with open(ewo_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['LOCUS_ADDRESS_ID'])
        for laid in sorted(ewo_laids):
            w.writerow([laid])
    print(f"  Saved: {ewo_path}", flush=True)

    # --- 3. LFA addresses: COUNTY_TYPE = 'LFA' in G_ALL_LOCUS_ADDRESS_KEY ---
    print("\n[3/3] Fetching LFA LAIDs...", flush=True)
    t0 = time.time()
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
    elapsed = time.time() - t0
    print(f"  LFA addresses: {len(lfa_laids):,} ({elapsed:.0f}s)", flush=True)

    # Save intermediate
    lfa_path = os.path.join(OUT, 'lfa_laids.csv')
    with open(lfa_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['LOCUS_ADDRESS_ID'])
        for laid in sorted(lfa_laids):
            w.writerow([laid])
    print(f"  Saved: {lfa_path}", flush=True)

    cur.close()
    conn.close()
    print("\nOracle connection closed.", flush=True)

    return nsi_laids, ewo_laids, lfa_laids


# ---------------------------------------------------------------------------
# Address tagging (same logic as tag_obligations_v2.py)
# ---------------------------------------------------------------------------

def tag_address(laid, planned_copper_recycling, copper_recycling_start,
                wc_sbb_flag, addr_sbb_flag, nsi_laids, lfa_laids):
    """Assign obligation bucket to a single address. Returns bucket name."""

    # Parse copper recycling year
    cop_year = None
    copper_start = copper_recycling_start or ''
    if copper_start:
        try:
            if '-' in copper_start:
                cop_year = int(copper_start[:4])
            elif '/' in copper_start:
                parts = copper_start.split('/')
                cop_year = int(parts[-1])
                if cop_year < 100:
                    cop_year += 2000
        except (ValueError, IndexError):
            pass

    is_copper = planned_copper_recycling in ('Y', 'Yes', '1') or cop_year is not None
    is_sbb = wc_sbb_flag in ('Y', 'Yes', '1') or addr_sbb_flag in ('Y', 'Yes', '1')
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


def tag_all_addresses(nsi_laids, ewo_laids, lfa_laids):
    """Read all_nonofs_12m.csv and tag every address with obligation bucket."""
    print(f"\nTagging addresses from {ADDR_CSV}...", flush=True)
    t0 = time.time()

    bucket_counts = Counter()
    addr_buckets = {}  # laid -> bucket
    n = 0

    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            laid = row['LOCUS_ADDRESS_ID']
            bucket = tag_address(
                laid,
                row.get('PLANNED_COPPER_RECYCLING', '') or '',
                row.get('COPPER_RECYCLING_START_DATE', '') or '',
                row.get('WC_SBB_FLAG', '') or '',
                row.get('ADDR_SBB_FLAG', '') or '',
                nsi_laids,
                lfa_laids,
            )
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


def write_addr_obligations_csv(addr_buckets):
    """Write address-level obligation tags to CSV."""
    out_path = os.path.join(OUT, 'addr_obligation_tags.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['LOCUS_ADDRESS_ID', 'OBLIGATION_BUCKET'])
        for laid, bucket in addr_buckets.items():
            w.writerow([laid, bucket])
    sz = os.path.getsize(out_path) / (1024 * 1024)
    print(f"\n  Written: {out_path} ({len(addr_buckets):,} rows, {sz:.1f} MB)", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    overall_t0 = time.time()

    # Step 1: Oracle queries
    nsi_laids, ewo_laids, lfa_laids = fetch_oracle_data()

    # Step 2: Tag all addresses
    addr_buckets = tag_all_addresses(nsi_laids, ewo_laids, lfa_laids)

    # Step 3: Write output CSV
    write_addr_obligations_csv(addr_buckets)

    total = time.time() - overall_t0
    print(f"\nDone in {total:.0f}s.", flush=True)
