"""
TIGER Road Download & GeoJSON Conversion Pipeline.

Downloads 2024 TIGER/Line road shapefiles by county FIPS, converts to GeoJSON
using a pure-Python shapefile reader (no ogr2ogr dependency).

Designed for the 590K plan: 647 WCs across 172 unique counties.

Usage:
    python tiger_pipeline.py                   # Download all 172 counties
    python tiger_pipeline.py --fips 42079      # Download specific county
    python tiger_pipeline.py --clli KGTNPAES   # Download county for a WC
    python tiger_pipeline.py --status          # Show download status
"""
import csv, io, json, os, struct, sys, time, zipfile
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from collections import defaultdict

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
TIGER_DIR = os.path.join(OUT, 'TIGER')
FIPS_CSV = os.path.join(OUT, 'clli_county_fips.csv')

# Census TIGER/Line 2024 roads URL pattern
TIGER_URL = 'https://www2.census.gov/geo/tiger/TIGER2024/ROADS/tl_2024_{fips}_roads.zip'

# Connecticut replaced 8 counties with 9 planning regions in 2022.
# Old FIPS codes (09001-09015) no longer exist in TIGER 2024.
# Map old county -> new planning region(s) that cover the same geography.
CT_OLD_TO_NEW = {
    '09001': ['09110', '09120'],  # Fairfield -> Western CT, Naugatuck Valley
    '09003': ['09150', '09170'],  # Hartford -> Capitol, South Central CT
    '09005': ['09120', '09160'],  # Litchfield -> Naugatuck Valley, NW Hills
    '09007': ['09150', '09170'],  # Middlesex -> Capitol, South Central CT
    '09009': ['09120', '09170'],  # New Haven -> Naugatuck Valley, South Central CT
    '09011': ['09140', '09180'],  # New London -> Southeastern CT, Windham
    '09013': ['09140', '09180'],  # Tolland -> Southeastern CT, Windham
    '09015': ['09140', '09180'],  # Windham -> Southeastern CT, Windham
}

def _resolve_fips(fips):
    """Resolve a FIPS code, handling CT remap. Returns list of FIPS codes."""
    fips = str(fips).zfill(5)
    if fips in CT_OLD_TO_NEW:
        return CT_OLD_TO_NEW[fips]
    return [fips]


def read_shapefile_to_geojson(shp_path):
    """Read a shapefile and return GeoJSON FeatureCollection (PolyLine only)."""
    dbf_path = shp_path.replace('.shp', '.dbf')

    # ---- Read DBF attributes ----
    dbf_records = []
    dbf_fields = []
    with open(dbf_path, 'rb') as f:
        f.read(4)  # version + date
        num_records = struct.unpack('<I', f.read(4))[0]
        header_size = struct.unpack('<H', f.read(2))[0]
        record_size = struct.unpack('<H', f.read(2))[0]
        f.read(20)  # reserved

        while True:
            field_header = f.read(32)
            if field_header[0:1] == b'\r':
                break
            name = field_header[:11].split(b'\x00')[0].decode('ascii')
            ftype = chr(field_header[11])
            flen = field_header[16]
            dbf_fields.append((name, ftype, flen))

        for _ in range(num_records):
            rec = f.read(record_size)
            if not rec or len(rec) < record_size:
                break
            offset = 1  # skip deletion flag
            values = {}
            for name, ftype, flen in dbf_fields:
                val = rec[offset:offset+flen].decode('latin-1', errors='replace').strip()
                values[name] = val
                offset += flen
            dbf_records.append(values)

    # ---- Read SHP geometry ----
    features = []
    with open(shp_path, 'rb') as f:
        file_code = struct.unpack('>I', f.read(4))[0]
        f.read(20)
        file_length = struct.unpack('>I', f.read(4))[0] * 2
        version = struct.unpack('<I', f.read(4))[0]
        shape_type = struct.unpack('<I', f.read(4))[0]
        bbox = struct.unpack('<8d', f.read(64))

        rec_idx = 0
        while f.tell() < file_length:
            try:
                rec_num = struct.unpack('>I', f.read(4))[0]
                content_length = struct.unpack('>I', f.read(4))[0] * 2
            except struct.error:
                break

            rec_start = f.tell()
            st = struct.unpack('<I', f.read(4))[0]

            if st == 0:  # Null
                f.seek(rec_start + content_length)
                rec_idx += 1
                continue

            if st == 3:  # PolyLine
                f.read(32)  # bbox
                num_parts = struct.unpack('<I', f.read(4))[0]
                num_points = struct.unpack('<I', f.read(4))[0]
                parts = [struct.unpack('<I', f.read(4))[0] for _ in range(num_parts)]
                points = [(struct.unpack('<d', f.read(8))[0],
                           struct.unpack('<d', f.read(8))[0])
                          for _ in range(num_points)]

                coords = []
                for i, start in enumerate(parts):
                    end = parts[i+1] if i+1 < len(parts) else num_points
                    coords.append([list(points[j]) for j in range(start, end)])

                props = dbf_records[rec_idx] if rec_idx < len(dbf_records) else {}

                if len(coords) == 1:
                    geom = {'type': 'LineString', 'coordinates': coords[0]}
                else:
                    geom = {'type': 'MultiLineString', 'coordinates': coords}

                features.append({
                    'type': 'Feature',
                    'properties': props,
                    'geometry': geom
                })
            else:
                f.seek(rec_start + content_length)

            rec_idx += 1

    return {'type': 'FeatureCollection', 'features': features}


def _download_single_fips(fips):
    """Download and convert a single FIPS to GeoJSON. Returns (path, n_features) or (None, 0)."""
    fips = str(fips).zfill(5)
    county_dir = os.path.join(TIGER_DIR, f'roads_{fips}')
    geojson_path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')

    if os.path.exists(geojson_path):
        return geojson_path, -1  # already exists

    url = TIGER_URL.format(fips=fips)
    os.makedirs(county_dir, exist_ok=True)

    try:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urlopen(req, timeout=60)
        zip_data = resp.read()
    except (URLError, HTTPError) as e:
        return None, 0

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        zf.extractall(county_dir)

    shp_path = os.path.join(county_dir, f'tl_2024_{fips}_roads.shp')
    if not os.path.exists(shp_path):
        return None, 0

    gj = read_shapefile_to_geojson(shp_path)
    with open(geojson_path, 'w') as f:
        json.dump(gj, f)

    return geojson_path, len(gj['features'])


def download_tiger_roads(fips, force=False):
    """Download and convert TIGER roads for a county FIPS code.

    Handles CT FIPS remapping: if fips is an old CT code, downloads the
    replacement planning region files and merges them.

    Returns: path to GeoJSON file, or None on failure.
    """
    fips = str(fips).zfill(5)
    resolved = _resolve_fips(fips)

    # If single FIPS (no remap needed), download directly
    if len(resolved) == 1 and resolved[0] == fips:
        geojson_path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        if os.path.exists(geojson_path) and not force:
            return geojson_path
        path, n = _download_single_fips(fips)
        return path

    # CT remap: download multiple regions, merge into one file under old FIPS name
    geojson_path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
    if os.path.exists(geojson_path) and not force:
        return geojson_path

    all_features = []
    for new_fips in resolved:
        path, n = _download_single_fips(new_fips)
        if path and os.path.exists(path):
            with open(path) as f:
                gj = json.load(f)
            all_features.extend(gj['features'])

    if not all_features:
        return None

    merged = {'type': 'FeatureCollection', 'features': all_features}
    with open(geojson_path, 'w') as f:
        json.dump(merged, f)

    return geojson_path


def load_clli_fips_map():
    """Load the CLLI -> FIPS mapping."""
    clli_to_fips = {}
    fips_to_cllis = defaultdict(list)
    with open(FIPS_CSV) as f:
        for row in csv.DictReader(f):
            clli = row['CLLI']
            primary = row['PRIMARY_FIPS']
            all_fips = row['ALL_FIPS'].split(';') if row['ALL_FIPS'] else []
            clli_to_fips[clli] = {
                'primary': primary,
                'all': all_fips,
                'county': row['PRIMARY_COUNTY'],
            }
            for fp in all_fips:
                fips_to_cllis[fp].append(clli)
    return clli_to_fips, fips_to_cllis


def get_roads_geojson_path(clli):
    """Get the GeoJSON path for a CLLI's primary county. Downloads if needed."""
    clli_to_fips, _ = load_clli_fips_map()
    if clli not in clli_to_fips:
        raise ValueError(f"CLLI {clli} not in FIPS mapping")
    fips = clli_to_fips[clli]['primary']
    path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
    if not os.path.exists(path):
        path = download_tiger_roads(fips)
    return path


def get_all_roads_paths(clli):
    """Get GeoJSON paths for ALL counties a CLLI spans. Downloads if needed."""
    clli_to_fips, _ = load_clli_fips_map()
    if clli not in clli_to_fips:
        raise ValueError(f"CLLI {clli} not in FIPS mapping")
    paths = []
    for fips in clli_to_fips[clli]['all']:
        path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        if not os.path.exists(path):
            path = download_tiger_roads(fips)
        if path:
            paths.append(path)
    return paths


def download_all(max_concurrent=1, skip_existing=True):
    """Download TIGER roads for all unique counties in the 590K plan."""
    _, fips_to_cllis = load_clli_fips_map()
    all_fips = sorted(fips_to_cllis.keys())

    print(f"TIGER Road Download Pipeline")
    print(f"  Counties to download: {len(all_fips)}")
    print(f"  WCs served: {sum(len(v) for v in fips_to_cllis.values())}")

    done = 0
    skipped = 0
    failed = 0
    t0 = time.time()

    for i, fips in enumerate(all_fips):
        geojson_path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        n_wcs = len(fips_to_cllis[fips])

        if os.path.exists(geojson_path) and skip_existing:
            skipped += 1
            continue

        resolved = _resolve_fips(fips)
        tag = f" [CT remap -> {resolved}]" if fips in CT_OLD_TO_NEW else ""
        print(f"  [{i+1}/{len(all_fips)}] FIPS {fips} ({n_wcs} WCs){tag}...", end=' ', flush=True)
        t1 = time.time()
        result = download_tiger_roads(fips)
        elapsed = time.time() - t1

        if result:
            size_mb = os.path.getsize(result) / 1024 / 1024
            print(f"{size_mb:.1f} MB, {elapsed:.1f}s")
            done += 1
        else:
            print("FAILED")
            failed += 1

    total_time = time.time() - t0
    print(f"\nDone in {total_time:.0f}s")
    print(f"  Downloaded: {done}")
    print(f"  Skipped (existing): {skipped}")
    print(f"  Failed: {failed}")
    print(f"  Total GeoJSON files: {done + skipped}")

    return done, skipped, failed


def status():
    """Show download status."""
    clli_to_fips, fips_to_cllis = load_clli_fips_map()
    all_fips = sorted(fips_to_cllis.keys())

    done = 0
    total_size = 0
    total_roads = 0
    missing = []

    for fips in all_fips:
        path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        if os.path.exists(path):
            done += 1
            total_size += os.path.getsize(path)
        else:
            missing.append(fips)

    # Count WCs covered
    covered_wcs = set()
    for fips in all_fips:
        path = os.path.join(TIGER_DIR, f'roads_{fips}.geojson')
        if os.path.exists(path):
            for clli in fips_to_cllis[fips]:
                # WC is covered if primary county is downloaded
                if clli_to_fips[clli]['primary'] == fips:
                    covered_wcs.add(clli)

    print(f"TIGER Download Status")
    print(f"  Counties: {done}/{len(all_fips)} downloaded")
    print(f"  WCs covered: {len(covered_wcs)}/{len(clli_to_fips)}")
    print(f"  Total GeoJSON size: {total_size / 1024 / 1024:.0f} MB")
    if missing:
        print(f"  Missing counties: {missing[:20]}{'...' if len(missing) > 20 else ''}")

    return done, len(all_fips), len(covered_wcs), len(clli_to_fips)


if __name__ == '__main__':
    args = sys.argv[1:]

    if '--status' in args:
        status()
    elif '--fips' in args:
        idx = args.index('--fips')
        fips = args[idx + 1]
        print(f"Downloading TIGER roads for FIPS {fips}...")
        result = download_tiger_roads(fips, force='--force' in args)
        if result:
            with open(result) as f:
                gj = json.load(f)
            print(f"  Done: {len(gj['features']):,} roads -> {result}")
        else:
            print("  Failed!")
    elif '--clli' in args:
        idx = args.index('--clli')
        clli = args[idx + 1]
        paths = get_all_roads_paths(clli)
        print(f"Roads for {clli}: {paths}")
    else:
        download_all()
