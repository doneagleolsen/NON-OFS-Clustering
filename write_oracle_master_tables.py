"""
Write Cluster Results to Oracle Master Tables

Creates and populates:
  1. CLUSTER_ASSIGNMENT_MASTER — address-level (12.3M rows)
  2. CLUSTER_SUMMARY_MASTER   — cluster-level (50-80K rows)

Usage:
    python write_oracle_master_tables.py
    python write_oracle_master_tables.py --drop   # Drop and recreate tables
    python write_oracle_master_tables.py --summary-only  # Only write cluster summary
"""
import csv, json, os, sys, time
from datetime import date

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m.csv')
OBLIGATION_CSV = os.path.join(OUT, 'addr_obligation_tags.csv')

RUN_DATE = date.today().isoformat()

# ── DDL Statements ────────────────────────────────────────────────────────

SCHEMA = 'TABLEAU_USER'  # GPSAA requires DBA; use own schema

DDL_ASSIGNMENT = """
CREATE TABLE CLUSTER_ASSIGNMENT_MASTER (
    LOCUS_ADDRESS_ID    VARCHAR2(50),
    CLUSTER_ID          VARCHAR2(30),
    CLLI                VARCHAR2(10),
    REGION              VARCHAR2(50),
    SUB_REGION          VARCHAR2(50),
    STATE               VARCHAR2(5),
    MARKET_DENSITY      VARCHAR2(20),
    AUI                 VARCHAR2(10),
    NO_OF_UNITS         NUMBER,
    LATITUDE            NUMBER,
    LONGITUDE           NUMBER,
    CPO_NTAS            NUMBER,
    CPO_PRED            NUMBER,
    TOTAL_CAPEX         NUMBER,
    COMPUTED_IRR         NUMBER,
    PEN_TERMINAL        NUMBER,
    AVG_ANNUAL_EBITDA   NUMBER,
    IRR_BUCKET          VARCHAR2(10),
    NT_TYPE             VARCHAR2(5),
    ODN_FLAG            VARCHAR2(5),
    COPPER_CIR_COUNT    NUMBER,
    COPPER_CUST_COUNT   NUMBER,
    DISPATCH_1YR        NUMBER,
    OBLIGATION_BUCKET   VARCHAR2(30),
    PRIORITY_RANK       NUMBER,
    FORMATION_SCORE     NUMBER,
    RUN_DATE            DATE,
    CONSTRAINT PK_CLUSTER_ASSIGN PRIMARY KEY (LOCUS_ADDRESS_ID, RUN_DATE)
)
"""

DDL_SUMMARY = """
CREATE TABLE CLUSTER_SUMMARY_MASTER (
    CLUSTER_ID          VARCHAR2(30),
    CLLI                VARCHAR2(10),
    REGION              VARCHAR2(50),
    SUB_REGION          VARCHAR2(50),
    MARKET_DENSITY      VARCHAR2(20),
    CENTROID_LAT        NUMBER,
    CENTROID_LON        NUMBER,
    TOTAL_UNITS         NUMBER,
    TOTAL_ADDRS         NUMBER,
    TOTAL_CAPEX         NUMBER,
    AVG_CPP             NUMBER,
    MEDIAN_IRR          NUMBER,
    COPPER_CIRCUITS     NUMBER,
    URGENCY_SCORE       NUMBER,
    VALUE_SCORE         NUMBER,
    BUILD_PRIORITY_TIER VARCHAR2(20),
    TOP_OBLIGATION      VARCHAR2(30),
    OBLIGATION_FRACTION NUMBER,
    AVG_FORMATION_SCORE NUMBER,
    AUI_SFU             NUMBER,
    AUI_SBU             NUMBER,
    AUI_MDU             NUMBER,
    AUI_MTU             NUMBER,
    OBLIGATION_FILL     VARCHAR2(500),
    RUN_DATE            DATE,
    CONSTRAINT PK_CLUSTER_SUMMARY PRIMARY KEY (CLUSTER_ID, RUN_DATE)
)
"""


def get_connection():
    """Get Oracle connection."""
    import oracledb
    return oracledb.connect(user="tableau_user", password="Verizon1#",
                            dsn="f1btpap-scan.verizon.com:1521/NARPROD")


def create_tables(drop_first=False):
    """Create master tables if they don't exist."""
    conn = get_connection()
    cur = conn.cursor()

    for table_name, ddl in [('CLUSTER_ASSIGNMENT_MASTER', DDL_ASSIGNMENT),
                             ('CLUSTER_SUMMARY_MASTER', DDL_SUMMARY)]:
        if drop_first:
            try:
                cur.execute(f"DROP TABLE {table_name}")
                print(f"  Dropped {table_name}", flush=True)
            except Exception:
                pass

        try:
            cur.execute(ddl)
            conn.commit()
            print(f"  Created {table_name}", flush=True)
        except Exception as e:
            if 'ORA-00955' in str(e):  # table already exists
                print(f"  {table_name} already exists", flush=True)
            else:
                print(f"  ERROR creating {table_name}: {e}", flush=True)

    cur.close()
    conn.close()


def load_cluster_assignments():
    """Build LAID -> CLUSTER_ID mapping from cluster cache."""
    print("Loading cluster assignments...", flush=True)
    with open(CLUSTER_CACHE) as f:
        clusters = json.load(f)

    laid_to_cluster = {}
    for c in clusters:
        cid = c['cluster_id']
        for laid in c.get('addresses', []):
            laid_to_cluster[str(laid)] = cid

    print(f"  {len(laid_to_cluster):,} address->cluster mappings", flush=True)
    return laid_to_cluster, clusters


def write_assignment_table(laid_to_cluster):
    """Write address-level assignments to Oracle."""
    print(f"\nWriting CLUSTER_ASSIGNMENT_MASTER...", flush=True)

    # Load obligation tags
    obligations = {}
    if os.path.exists(OBLIGATION_CSV):
        with open(OBLIGATION_CSV, encoding='utf-8') as f:
            for row in csv.DictReader(f):
                obligations[row['LOCUS_ADDRESS_ID']] = row['OBLIGATION_BUCKET']

    conn = get_connection()
    cur = conn.cursor()

    # Delete existing rows for this run date
    cur.execute(f"DELETE FROM CLUSTER_ASSIGNMENT_MASTER WHERE RUN_DATE = TO_DATE(:1, 'YYYY-MM-DD')",
                [RUN_DATE])
    conn.commit()

    insert_sql = """
        INSERT INTO CLUSTER_ASSIGNMENT_MASTER (
            LOCUS_ADDRESS_ID, CLUSTER_ID, CLLI, REGION, SUB_REGION, STATE,
            MARKET_DENSITY, AUI, NO_OF_UNITS, LATITUDE, LONGITUDE,
            CPO_NTAS, CPO_PRED, TOTAL_CAPEX, COMPUTED_IRR, PEN_TERMINAL,
            AVG_ANNUAL_EBITDA, IRR_BUCKET, NT_TYPE, ODN_FLAG,
            COPPER_CIR_COUNT, COPPER_CUST_COUNT, DISPATCH_1YR,
            OBLIGATION_BUCKET, PRIORITY_RANK, FORMATION_SCORE, RUN_DATE
        ) VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,
            :17,:18,:19,:20,:21,:22,:23,:24,:25,:26,TO_DATE(:27,'YYYY-MM-DD')
        )
    """

    batch = []
    n = 0
    t0 = time.time()

    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            laid = row['LOCUS_ADDRESS_ID']
            cluster_id = laid_to_cluster.get(laid, '')
            oblig = obligations.get(laid, 'DISCRETIONARY')

            values = (
                laid, cluster_id, row['CLLI'], row['REGION'], row['SUB_REGION'],
                row['STATE'], row['MARKET_DENSITY'], row['AUI'],
                int(row['NO_OF_UNITS']) if row['NO_OF_UNITS'] else 1,
                float(row['LATITUDE']), float(row['LONGITUDE']),
                float(row['CPO_NTAS']) if row['CPO_NTAS'] else None,
                float(row['CPO_PRED']) if row['CPO_PRED'] else None,
                float(row['TOTAL_CAPEX']) if row['TOTAL_CAPEX'] else None,
                float(row['COMPUTED_IRR']) if row['COMPUTED_IRR'] else None,
                float(row['PEN_TERMINAL']) if row['PEN_TERMINAL'] else None,
                float(row['AVG_ANNUAL_EBITDA']) if row['AVG_ANNUAL_EBITDA'] else None,
                row['IRR_BUCKET'] or None,
                row['NT_TYPE'] or None,
                row['ODN_FLAG'] or None,
                int(row['COPPER_CIR_COUNT']) if row['COPPER_CIR_COUNT'] else 0,
                int(row['COPPER_CUST_COUNT']) if row['COPPER_CUST_COUNT'] else 0,
                float(row['DISPATCH_1YR']) if row['DISPATCH_1YR'] else 0,
                oblig,
                float(row['PRIORITY_RANK']) if row['PRIORITY_RANK'] else None,
                None,  # FORMATION_SCORE — filled from cluster data later
                RUN_DATE,
            )
            batch.append(values)
            n += 1

            if len(batch) >= 5000:
                cur.executemany(insert_sql, batch)
                conn.commit()
                batch = []
                if n % 500000 == 0:
                    elapsed = time.time() - t0
                    rate = n / elapsed
                    print(f"  ...{n:,} rows ({rate:.0f} rows/s)", flush=True)

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()

    elapsed = time.time() - t0
    print(f"  Written {n:,} rows in {elapsed:.0f}s ({n/elapsed:.0f} rows/s)", flush=True)

    cur.close()
    conn.close()


def write_summary_table(clusters):
    """Write cluster-level summary to Oracle."""
    print(f"\nWriting CLUSTER_SUMMARY_MASTER...", flush=True)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f"DELETE FROM CLUSTER_SUMMARY_MASTER WHERE RUN_DATE = TO_DATE(:1, 'YYYY-MM-DD')",
                [RUN_DATE])
    conn.commit()

    insert_sql = """
        INSERT INTO CLUSTER_SUMMARY_MASTER (
            CLUSTER_ID, CLLI, REGION, SUB_REGION, MARKET_DENSITY,
            CENTROID_LAT, CENTROID_LON, TOTAL_UNITS, TOTAL_ADDRS, TOTAL_CAPEX,
            AVG_CPP, MEDIAN_IRR, COPPER_CIRCUITS,
            URGENCY_SCORE, VALUE_SCORE, BUILD_PRIORITY_TIER,
            TOP_OBLIGATION, OBLIGATION_FRACTION, AVG_FORMATION_SCORE,
            AUI_SFU, AUI_SBU, AUI_MDU, AUI_MTU,
            OBLIGATION_FILL, RUN_DATE
        ) VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,
            :17,:18,:19,:20,:21,:22,:23,:24,TO_DATE(:25,'YYYY-MM-DD')
        )
    """

    batch = []
    for c in clusters:
        aui = c.get('aui_units', {})
        oblig_fill = c.get('obligation_fill', {})
        oblig_str = '|'.join(f"{k}:{v}" for k, v in sorted(oblig_fill.items()))

        values = (
            c['cluster_id'], c['clli'], c.get('region', ''),
            c.get('sub_region', ''), c.get('market_density', ''),
            c['lat'], c['lon'], c['total_units'], c['total_addrs'],
            round(c.get('total_capex', 0)),
            round(c.get('avg_cpp', 0)),
            round(c.get('median_irr', 0), 4),
            c.get('copper_circuits', 0),
            c.get('urgency_score', 0), c.get('value_score', 0),
            c.get('build_priority_tier', ''),
            c.get('top_obligation', ''),
            round(c.get('obligation_fraction', 0), 3),
            round(c.get('avg_formation_score', 0), 1),
            aui.get('SFU', 0), aui.get('SBU', 0),
            aui.get('MDU', 0), aui.get('MTU', 0),
            oblig_str[:500],  # truncate to fit VARCHAR2(500)
            RUN_DATE,
        )
        batch.append(values)

    cur.executemany(insert_sql, batch)
    conn.commit()

    print(f"  Written {len(batch):,} cluster rows", flush=True)
    cur.close()
    conn.close()


if __name__ == '__main__':
    args = sys.argv[1:]
    drop = '--drop' in args
    summary_only = '--summary-only' in args

    print("Oracle Master Tables Writer", flush=True)
    print(f"  Run date: {RUN_DATE}", flush=True)

    create_tables(drop_first=drop)

    laid_to_cluster, clusters = load_cluster_assignments()

    if not summary_only:
        write_assignment_table(laid_to_cluster)

    write_summary_table(clusters)

    print("\nDone.", flush=True)
