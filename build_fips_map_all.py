"""Build CLLI -> FIPS mapping for ALL 2,078 NON-OFS CLLIs.

Uses CENSUSBLOCKID (first 5 digits = county FIPS) from G_ALL_LOCUS_ADDRESS_KEY.
"""
import csv, os
from collections import defaultdict

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'

# ── Step 1: Load existing FIPS map (verified multi-county) ────────────────
existing = {}
existing_path = os.path.join(OUT, 'clli_county_fips.csv')
if os.path.exists(existing_path):
    with open(existing_path) as f:
        for row in csv.DictReader(f):
            existing[row['CLLI']] = {
                'primary': row['PRIMARY_FIPS'],
                'county': row['PRIMARY_COUNTY'],
                'all': row['ALL_FIPS'].split(';') if row['ALL_FIPS'] else [],
            }
    print(f"  {len(existing)} existing FIPS mappings loaded", flush=True)

# ── Step 2: Query Oracle for CLLI -> county FIPS via CENSUSBLOCKID ────────
print("Querying Oracle for CLLI -> county FIPS via CENSUSBLOCKID...", flush=True)
import oracledb
conn = oracledb.connect(user="tableau_user", password="Verizon1#",
                        dsn="f1btpap-scan.verizon.com:1521/NARPROD")
cur = conn.cursor()

# Get county FIPS distribution per CLLI
cur.execute("""
    SELECT b.CLLI, SUBSTR(ak.CENSUSBLOCKID, 1, 5) as COUNTY_FIPS, COUNT(*) as CNT
    FROM GPSAA.S_BEYOND_INFINITY_RANKING_SCORING b
    INNER JOIN GPSAA.G_ALL_LOCUS_ADDRESS_KEY ak ON b.LOCUS_ADDRESS_ID = ak.LOCUS_ADDRESS_ID
    WHERE ak.CATEGORY = 'NON_OFS'
      AND ak.CENSUSBLOCKID IS NOT NULL
      AND LENGTH(ak.CENSUSBLOCKID) >= 5
    GROUP BY b.CLLI, SUBSTR(ak.CENSUSBLOCKID, 1, 5)
    ORDER BY b.CLLI, COUNT(*) DESC
""")

clli_fips = defaultdict(list)  # clli -> [(fips, count), ...]
for row in cur.fetchall():
    if row[1] and len(row[1]) == 5:
        clli_fips[row[0]].append((row[1], row[2]))

print(f"  {len(clli_fips)} CLLIs with census-derived FIPS", flush=True)

# Also get county names from WIRECENTER
cur.execute("SELECT CLLI, COUNTY FROM GPSAA.WIRECENTER")
wc_county = {r[0]: r[1] for r in cur.fetchall() if r[1]}
print(f"  {len(wc_county)} WCs with county names", flush=True)

cur.close()
conn.close()

# ── Step 3: Build final FIPS map ──────────────────────────────────────────
print("\nBuilding final FIPS map...", flush=True)

results = {}

for clli in sorted(clli_fips.keys()):
    # If we have existing verified mapping, keep it
    if clli in existing:
        results[clli] = existing[clli]
        continue

    fips_list = clli_fips[clli]
    primary = fips_list[0][0]  # most common FIPS
    total = sum(c for _, c in fips_list)

    # Include counties with at least 1% of addresses or 10+ addresses
    threshold = max(10, total * 0.01)
    all_fips = [f for f, c in fips_list if c >= threshold]
    if not all_fips:
        all_fips = [primary]

    county = wc_county.get(clli, '')

    results[clli] = {
        'primary': primary,
        'county': county,
        'all': all_fips,
    }

# Check for CLLIs with addresses but no FIPS
all_cllis = set()
with open(os.path.join(OUT, 'all_nonofs_12m.csv'), encoding='utf-8') as f:
    for row in csv.DictReader(f):
        all_cllis.add(row['CLLI'])

missing = all_cllis - set(results.keys())
print(f"  Mapped: {len(results)}")
print(f"  Missing: {len(missing)}")
if missing:
    print(f"  Missing: {sorted(missing)[:20]}")

# ── Step 4: Write extended FIPS CSV ───────────────────────────────────────
out_path = os.path.join(OUT, 'clli_county_fips_all.csv')
with open(out_path, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['CLLI', 'PRIMARY_FIPS', 'PRIMARY_COUNTY', 'ALL_FIPS', 'N_COUNTIES'])
    for clli in sorted(results.keys()):
        r = results[clli]
        w.writerow([clli, r['primary'], r['county'],
                     ';'.join(r['all']), len(r['all'])])

unique_counties = set(f for r in results.values() for f in r['all'])
print(f"\nWritten: {out_path}")
print(f"  {len(results)} CLLIs, {len(unique_counties)} unique counties")
print("Done.")
