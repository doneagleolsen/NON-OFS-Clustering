"""Extract copper cable data from Oracle FIM for test wire centers."""
import csv
import oracledb

# Use thin mode (no Oracle client needed)
CONN_STR = "tableau_user/Verizon1#@f1btpap-scan.verizon.com:1521/NARPROD"

# WC code is 4-char; CLLI is 8-char
WCS = {
    'CLFD': 'CLFDPACL',
    'ARTN': 'ARTNVAAR',
    'SPFD': 'SPFDVASP',
    # LGNR already extracted
}

SQL = """
SELECT FROM_LATITUDE, FROM_LONGITUDE, TO_LATITUDE, TO_LONGITUDE,
       QUANTITY, CONSTRUCTION_TYPE
FROM VNADSPRD.FIM_VZT_CABLE
WHERE WC = :wc
  AND (ITEM IS NULL OR ITEM != 'FIBER')
  AND FROM_LATITUDE IS NOT NULL
  AND TO_LATITUDE IS NOT NULL
"""

conn = oracledb.connect(CONN_STR)
cur = conn.cursor()

for wc4, clli in WCS.items():
    print(f"Extracting {clli} (WC={wc4})...")
    cur.execute(SQL, {'wc': wc4})
    rows = cur.fetchall()
    outpath = f"{clli}_copper_cable.csv"
    with open(outpath, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['FROM_LATITUDE', 'FROM_LONGITUDE', 'TO_LATITUDE', 'TO_LONGITUDE',
                     'QUANTITY', 'CONSTRUCTION_TYPE'])
        for r in rows:
            w.writerow(r)
    print(f"  {len(rows)} segments -> {outpath}")

cur.close()
conn.close()
print("Done.")
