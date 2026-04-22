"""
Extract ALL 12.3M NON-OFS addresses from Oracle with full engineering metadata.

Sources:
  - GPSAA.S_BEYOND_INFINITY_RANKING_SCORING (BI scoring + cost)
  - GPSAA.G_ALL_LOCUS_ADDRESS_KEY (lat/lon, category, ODN flag)
  - GPSAA.WIRECENTER (region, sub_region, NT type, copper recycling, SBB)
  - irr_all_base_addresses.csv (V2 penetration ramp IRR — join locally)

Output:
  all_nonofs_12m.csv — one row per base address with all fields needed for clustering
"""
import oracledb
import csv
import os
import time

ORACLE_USER = "tableau_user"
ORACLE_PASS = "Verizon1#"
ORACLE_DSN = "f1btpap-scan.verizon.com:1521/NARPROD"

OUTPUT_DIR = r"C:\Users\v267429\Downloads\AI_Sessions"
IRR_FILE = os.path.join(OUTPUT_DIR, "irr_all_base_addresses.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "all_nonofs_12m.csv")

# Columns to extract from Oracle
ORACLE_SQL = """
SELECT
    b.LOCUS_ADDRESS_ID,
    b.CLLI,
    ak.LATITUDE,
    ak.LONGITUDE,
    b.AUI,
    b.NO_OF_UNITS,
    b.CPO_NTAS,
    b.CPO_PRED,
    b.PRIORITY_RANK,
    b.COPPER_CIR_COUNT,
    b.COPPER_CUST_COUNT,
    b.COPPER_IND_NEW,
    b.DSL_IND,
    b.FIOS_IND,
    b.MARKET_DENSITY,
    b.STATE,
    b.REGION,
    b.SUB_REGION,
    ak.ODN_FLAG,
    w.NT1,
    w.NT2,
    w.PLANNED_COPPER_RECYCLING,
    w.COPPER_RECYCLING_START_DATE,
    w.SBB_FLAG as WC_SBB_FLAG,
    ak.SBB_FLAG as ADDR_SBB_FLAG,
    b.DISPATCH_1YR,
    b.DISPATCH_2YR,
    b.DISPATCH_3YR,
    b.BUILD_CNT_SCORE,
    b.COFS_PRED_SCORE
FROM GPSAA.S_BEYOND_INFINITY_RANKING_SCORING b
INNER JOIN GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak
    ON b.LOCUS_ADDRESS_ID = ak.LOCUS_ADDRESS_ID
LEFT JOIN GPSAA.WIRECENTER w
    ON b.CLLI = w.CLLI
WHERE ak.CATEGORY = 'NON_OFS'
"""

OUTPUT_COLS = [
    'LOCUS_ADDRESS_ID', 'CLLI', 'LATITUDE', 'LONGITUDE', 'AUI', 'NO_OF_UNITS',
    'CPO_NTAS', 'CPO_PRED', 'PRIORITY_RANK',
    'COPPER_CIR_COUNT', 'COPPER_CUST_COUNT', 'COPPER_IND_NEW', 'DSL_IND', 'FIOS_IND',
    'MARKET_DENSITY', 'STATE', 'REGION', 'SUB_REGION',
    'ODN_FLAG', 'NT1', 'NT2',
    'PLANNED_COPPER_RECYCLING', 'COPPER_RECYCLING_START_DATE',
    'WC_SBB_FLAG', 'ADDR_SBB_FLAG',
    'DISPATCH_1YR', 'DISPATCH_2YR', 'DISPATCH_3YR',
    'BUILD_CNT_SCORE', 'COFS_PRED_SCORE',
    # Added from IRR join
    'COMPUTED_IRR', 'PEN_TERMINAL', 'PEN_YR1', 'PEN_YR5', 'PEN_YR10',
    'EBITDA_YR1', 'EBITDA_YR5', 'EBITDA_YR10', 'AVG_ANNUAL_EBITDA',
    'IRR_BUCKET', 'TOTAL_CAPEX', 'CO_OVERHEAD_ALLOC',
    'NT_TYPE'
]


IRR_FIELDS = ['COMPUTED_IRR', 'PEN_TERMINAL', 'PEN_YR1', 'PEN_YR5', 'PEN_YR10',
               'EBITDA_YR1', 'EBITDA_YR5', 'EBITDA_YR10', 'AVG_ANNUAL_EBITDA',
               'IRR_BUCKET', 'TOTAL_CAPEX', 'CO_OVERHEAD_ALLOC', 'NT_TYPE']


def load_irr_lookup():
    """Load IRR V2 ramp data keyed by LOCUS_ADDRESS_ID.
    Uses tuple storage to minimize memory (~4 GB for 12.3M rows)."""
    print(f"Loading IRR data from {IRR_FILE}...")
    if not os.path.exists(IRR_FILE):
        print(f"  WARNING: {IRR_FILE} not found — IRR fields will be empty")
        return {}

    irr = {}
    with open(IRR_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            laid = row.get('LOCUS_ADDRESS_ID', '').strip()
            if laid:
                # Store as tuple (compact) instead of dict
                irr[laid] = tuple(row.get(f, '') for f in IRR_FIELDS)
            if i % 2_000_000 == 0 and i > 0:
                print(f"  ...loaded {i:,} IRR rows")

    print(f"  Loaded {len(irr):,} IRR records")
    return irr


def get_irr_dict(irr_tuple):
    """Convert compact tuple back to field dict."""
    if not irr_tuple:
        return {f: '' for f in IRR_FIELDS}
    return dict(zip(IRR_FIELDS, irr_tuple))


def extract_addresses(irr_lookup):
    """Stream 12.3M addresses from Oracle, join IRR locally, write CSV."""
    print(f"\nConnecting to Oracle...")
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
    cur = conn.cursor()
    cur.arraysize = 10000  # fetch in large batches

    print(f"Executing extraction query...")
    t0 = time.time()
    cur.execute(ORACLE_SQL)

    oracle_cols = [desc[0] for desc in cur.description]
    print(f"  Oracle columns: {len(oracle_cols)}")

    written = 0
    matched_irr = 0
    bad_coords = 0

    # ILEC bounding box filter
    LAT_MIN, LAT_MAX = 36.3, 47.5
    LON_MIN, LON_MAX = -83.0, -66.5

    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLS)

        while True:
            rows = cur.fetchmany(10000)
            if not rows:
                break

            for row in rows:
                rec = dict(zip(oracle_cols, row))

                # Coordinate filter
                lat = rec.get('LATITUDE')
                lon = rec.get('LONGITUDE')
                if lat is None or lon is None:
                    bad_coords += 1
                    continue
                try:
                    lat = float(lat)
                    lon = float(lon)
                except (ValueError, TypeError):
                    bad_coords += 1
                    continue

                if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                    bad_coords += 1
                    continue

                # Join IRR data
                laid_str = str(rec['LOCUS_ADDRESS_ID'])
                irr_tuple = irr_lookup.get(laid_str)
                irr_rec = get_irr_dict(irr_tuple)
                if irr_tuple:
                    matched_irr += 1

                # Convert date to string
                cop_date = rec.get('COPPER_RECYCLING_START_DATE')
                if cop_date:
                    try:
                        cop_date = cop_date.strftime('%Y-%m-%d')
                    except:
                        cop_date = str(cop_date)

                # Build output row
                out_row = [
                    rec.get('LOCUS_ADDRESS_ID'),
                    rec.get('CLLI'),
                    lat, lon,
                    rec.get('AUI'),
                    rec.get('NO_OF_UNITS'),
                    rec.get('CPO_NTAS'),
                    rec.get('CPO_PRED'),
                    rec.get('PRIORITY_RANK'),
                    rec.get('COPPER_CIR_COUNT'),
                    rec.get('COPPER_CUST_COUNT'),
                    rec.get('COPPER_IND_NEW'),
                    rec.get('DSL_IND'),
                    rec.get('FIOS_IND'),
                    rec.get('MARKET_DENSITY'),
                    rec.get('STATE'),
                    rec.get('REGION'),
                    rec.get('SUB_REGION'),
                    rec.get('ODN_FLAG'),
                    rec.get('NT1'),
                    rec.get('NT2'),
                    rec.get('PLANNED_COPPER_RECYCLING'),
                    cop_date,
                    rec.get('WC_SBB_FLAG'),
                    rec.get('ADDR_SBB_FLAG'),
                    rec.get('DISPATCH_1YR'),
                    rec.get('DISPATCH_2YR'),
                    rec.get('DISPATCH_3YR'),
                    rec.get('BUILD_CNT_SCORE'),
                    rec.get('COFS_PRED_SCORE'),
                    # IRR fields
                    irr_rec.get('COMPUTED_IRR', ''),
                    irr_rec.get('PEN_TERMINAL', ''),
                    irr_rec.get('PEN_YR1', ''),
                    irr_rec.get('PEN_YR5', ''),
                    irr_rec.get('PEN_YR10', ''),
                    irr_rec.get('EBITDA_YR1', ''),
                    irr_rec.get('EBITDA_YR5', ''),
                    irr_rec.get('EBITDA_YR10', ''),
                    irr_rec.get('AVG_ANNUAL_EBITDA', ''),
                    irr_rec.get('IRR_BUCKET', ''),
                    irr_rec.get('TOTAL_CAPEX', ''),
                    irr_rec.get('CO_OVERHEAD_ALLOC', ''),
                    irr_rec.get('NT_TYPE', ''),
                ]

                writer.writerow(out_row)
                written += 1

            elapsed = time.time() - t0
            print(f"  {written:>10,} rows | {matched_irr:,} IRR matched | "
                  f"{bad_coords:,} filtered | {elapsed:.0f}s", end='\r')

    elapsed = time.time() - t0
    cur.close()
    conn.close()

    print(f"\n\nExtraction complete:")
    print(f"  Total written: {written:,}")
    print(f"  IRR matched: {matched_irr:,} ({100*matched_irr/max(written,1):.1f}%)")
    print(f"  Bad coordinates filtered: {bad_coords:,}")
    print(f"  Elapsed: {elapsed:.1f}s")
    print(f"  Output: {OUTPUT_FILE}")

    return written


def verify(n_written):
    """Quick verification of output file."""
    print(f"\nVerifying {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'r') as f:
        reader = csv.DictReader(f)
        sample = []
        cllis = set()
        states = set()
        for i, row in enumerate(reader):
            cllis.add(row['CLLI'])
            states.add(row['STATE'])
            if i < 5:
                sample.append(row)

    print(f"  Rows: {i+1:,} (expected ~12.3M)")
    print(f"  CLLIs: {len(cllis):,}")
    print(f"  States: {sorted(states)}")
    print(f"\n  Sample row 1:")
    for k, v in sample[0].items():
        if v:
            print(f"    {k}: {v}")


if __name__ == "__main__":
    irr_lookup = load_irr_lookup()
    n = extract_addresses(irr_lookup)
    if n > 0:
        verify(n)
