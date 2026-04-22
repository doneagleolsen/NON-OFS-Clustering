"""
Export Cluster Results to ArcGIS File GDB

Converts GeoJSON polygons + address points to file GDB feature classes.
Adds WC boundary + fiber cable basemap layers.

MUST RUN WITH: arcgispro-py3 Python environment
  "C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe"

Usage:
    python export_arcgis_gdb.py
"""
import arcpy
import csv, json, os, sys, time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
GDB_PATH = os.path.join(OUT, 'NONOFS_Master.gdb')
POLYGON_GEOJSON = os.path.join(OUT, 'all_nonofs_cluster_polygons.geojson')
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m_sorted.csv')
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')

# Basemap layers
WC_SHAPEFILE = r'C:\Users\v267429\Documents\ILEC_WC_Shapefile\ILEC_WC_Shapefile\VZ_ILEC_WCs.shp'
FIBER_GDB = r'C:\Users\v267429\Downloads\AI_Sessions\_select_from_bbp_gfim_ilec_fiber_all_wgs84_20260206_as_t1_left__20260227.gdb'

# Bounding box filter
BBOX = {'lat_min': 36.3, 'lat_max': 47.5, 'lon_min': -83.0, 'lon_max': -66.5}

arcpy.env.overwriteOutput = True


def create_gdb():
    """Create fresh file GDB."""
    if arcpy.Exists(GDB_PATH):
        arcpy.management.Delete(GDB_PATH)
    folder, name = os.path.split(GDB_PATH)
    arcpy.management.CreateFileGDB(folder, name)
    print(f"  Created: {GDB_PATH}", flush=True)


def import_polygons():
    """Import cluster polygons from GeoJSON to GDB feature class."""
    print("Importing cluster polygons...", flush=True)
    if not os.path.exists(POLYGON_GEOJSON):
        print(f"  ERROR: {POLYGON_GEOJSON} not found", flush=True)
        return

    fc = os.path.join(GDB_PATH, 'Cluster_Polygons')
    arcpy.conversion.JSONToFeatures(POLYGON_GEOJSON, fc, geometry_type='POLYGON')

    count = int(arcpy.management.GetCount(fc)[0])
    print(f"  Imported {count:,} polygon features -> Cluster_Polygons", flush=True)


def import_address_points():
    """Import address points from CSV to GDB feature class (XY Table)."""
    print("Importing address points (this may take a while)...", flush=True)

    fc = os.path.join(GDB_PATH, 'Address_Points')

    # Create point feature class
    sr = arcpy.SpatialReference(4326)  # WGS84
    arcpy.management.CreateFeatureclass(
        GDB_PATH, 'Address_Points', 'POINT', spatial_reference=sr
    )

    # Add fields
    fields = [
        ('LAID', 'TEXT', 50),
        ('CLUSTER_ID', 'TEXT', 30),
        ('CLLI', 'TEXT', 10),
        ('REGION', 'TEXT', 50),
        ('SUB_REGION', 'TEXT', 50),
        ('STATE_CD', 'TEXT', 5),
        ('MARKET_DENSITY', 'TEXT', 20),
        ('AUI', 'TEXT', 10),
        ('NO_OF_UNITS', 'LONG', None),
        ('CPO_NTAS', 'DOUBLE', None),
        ('COMPUTED_IRR', 'DOUBLE', None),
        ('OBLIGATION_BUCKET', 'TEXT', 30),
        ('BUILD_PRIORITY', 'TEXT', 20),
    ]
    for fname, ftype, flen in fields:
        if ftype == 'TEXT':
            arcpy.management.AddField(fc, fname, ftype, field_length=flen)
        else:
            arcpy.management.AddField(fc, fname, ftype)

    # Load cluster assignments
    with open(CLUSTER_CACHE) as f:
        clusters = json.load(f)
    laid_to_cluster = {}
    cluster_priority = {}
    for c in clusters:
        cid = c['cluster_id']
        cluster_priority[cid] = c.get('build_priority_tier', '')
        for laid in c.get('addresses', []):
            laid_to_cluster[str(laid)] = cid
    print(f"  {len(laid_to_cluster):,} address->cluster mappings", flush=True)

    # Load obligation tags
    obligations = {}
    ob_path = os.path.join(OUT, 'addr_obligation_tags.csv')
    if os.path.exists(ob_path):
        with open(ob_path, encoding='utf-8') as f:
            for row in csv.DictReader(f):
                obligations[row['LOCUS_ADDRESS_ID']] = row['OBLIGATION_BUCKET']

    # Insert points
    insert_fields = ['SHAPE@XY'] + [f[0] for f in fields]
    n = 0
    with arcpy.da.InsertCursor(fc, insert_fields) as cursor:
        with open(ADDR_CSV, encoding='utf-8') as f:
            for row in csv.DictReader(f):
                lat = float(row['LATITUDE'])
                lon = float(row['LONGITUDE'])
                if not (BBOX['lat_min'] <= lat <= BBOX['lat_max'] and
                        BBOX['lon_min'] <= lon <= BBOX['lon_max']):
                    continue

                laid = row['LOCUS_ADDRESS_ID']
                cid = laid_to_cluster.get(laid, '')
                oblig = obligations.get(laid, 'DISCRETIONARY')
                priority = cluster_priority.get(cid, '')
                irr = float(row['COMPUTED_IRR']) if row['COMPUTED_IRR'] else None
                cpo = float(row['CPO_NTAS']) if row['CPO_NTAS'] else None
                units = int(row['NO_OF_UNITS']) if row['NO_OF_UNITS'] else 1

                cursor.insertRow([
                    (lon, lat), laid, cid, row['CLLI'],
                    row.get('REGION', ''), row.get('SUB_REGION', ''),
                    row.get('STATE', ''), row.get('MARKET_DENSITY', ''),
                    row.get('AUI', 'SFU'), units, cpo, irr, oblig, priority
                ])
                n += 1
                if n % 1000000 == 0:
                    print(f"  ...{n:,} points", flush=True)

    print(f"  Imported {n:,} address points -> Address_Points", flush=True)


def import_basemap():
    """Import WC boundaries and fiber cable layers."""
    print("Importing basemap layers...", flush=True)

    # WC boundaries
    if os.path.exists(WC_SHAPEFILE):
        wc_fc = os.path.join(GDB_PATH, 'WC_Boundaries')
        arcpy.conversion.FeatureClassToFeatureClass(WC_SHAPEFILE, GDB_PATH, 'WC_Boundaries')
        count = int(arcpy.management.GetCount(wc_fc)[0])
        print(f"  WC Boundaries: {count:,} features", flush=True)
    else:
        print(f"  WARNING: WC shapefile not found: {WC_SHAPEFILE}", flush=True)

    # Fiber cable
    if arcpy.Exists(FIBER_GDB):
        prev_ws = arcpy.env.workspace
        arcpy.env.workspace = FIBER_GDB
        fiber_fcs = arcpy.ListFeatureClasses()
        arcpy.env.workspace = prev_ws

        if fiber_fcs:
            src_fc = os.path.join(FIBER_GDB, fiber_fcs[0])
            arcpy.conversion.FeatureClassToFeatureClass(src_fc, GDB_PATH, 'Fiber_FTTP')
            count = int(arcpy.management.GetCount(os.path.join(GDB_PATH, 'Fiber_FTTP'))[0])
            print(f"  Fiber FTTP: {count:,} features", flush=True)
        else:
            print(f"  WARNING: No feature classes found in fiber GDB", flush=True)
    else:
        print(f"  WARNING: Fiber GDB not found: {FIBER_GDB}", flush=True)


if __name__ == '__main__':
    t0 = time.time()
    print("ArcGIS GDB Export — NON-OFS Master", flush=True)
    print(f"  Output: {GDB_PATH}", flush=True)

    create_gdb()
    import_polygons()
    import_address_points()
    import_basemap()

    elapsed = time.time() - t0
    print(f"\nExport complete in {elapsed/60:.1f} min", flush=True)
    print(f"  GDB: {GDB_PATH}", flush=True)

    # List all feature classes
    arcpy.env.workspace = GDB_PATH
    fcs = arcpy.ListFeatureClasses()
    for fc in fcs:
        count = int(arcpy.management.GetCount(fc)[0])
        print(f"    {fc}: {count:,} features", flush=True)

    print("\nDone.", flush=True)
