"""
Push NON-OFS Explorer to GitHub via Git Trees/Blobs API

Batch-pushes all files in a single commit using the low-level Git Data API.
Uses nested tree construction (bottom-up) to avoid 502 errors on large flat trees.
Caches blob SHAs locally so re-runs skip already-uploaded blobs.

Usage:
    python push_explorer.py [-m "commit message"] [--dry-run]
"""
import argparse, base64, hashlib, json, os, sys, time
import urllib.request, urllib.error
from collections import defaultdict

# Strip proxy for GitHub API calls
for v in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(v, None)

CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.claude', 'github-config.json')
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

TOKEN = cfg['token']
OWNER = cfg['username']
REPO = 'NON-OFS-Clustering'
BASE_URL = f'https://api.github.com/repos/{OWNER}/{REPO}'
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))
BLOB_CACHE = os.path.join(LOCAL_DIR, '.blob_cache.json')

# Files/dirs to skip
SKIP = {'.git', '__pycache__', '.DS_Store', 'Thumbs.db', '.blob_cache.json'}
SKIP_EXT = {'.pyc', '.pyo'}


def api(method, url, data=None, accept='application/vnd.github+json', timeout=120):
    headers = {
        'Authorization': f'token {TOKEN}',
        'Content-Type': 'application/json',
        'Accept': accept,
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode()), resp.status
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()[:500]
        return {'error': err_body, 'status': e.code}, e.code
    except Exception as e:
        return {'error': str(e)}, 0


def collect_files():
    """Collect all files to push."""
    files = []
    for root, dirs, filenames in os.walk(LOCAL_DIR):
        dirs[:] = [d for d in dirs if d not in SKIP]
        for fn in filenames:
            if fn in SKIP or os.path.splitext(fn)[1] in SKIP_EXT:
                continue
            fp = os.path.join(root, fn)
            rel = os.path.relpath(fp, LOCAL_DIR).replace('\\', '/')
            files.append((fp, rel))
    return files


def file_sha256(fp):
    """Compute SHA-256 of a file for cache keying."""
    h = hashlib.sha256()
    with open(fp, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def load_blob_cache():
    if os.path.exists(BLOB_CACHE):
        with open(BLOB_CACHE) as f:
            return json.load(f)
    return {}


def save_blob_cache(cache):
    with open(BLOB_CACHE, 'w') as f:
        json.dump(cache, f)


def push_via_batched_trees(files, commit_msg, branch='main'):
    """Push all files using batched tree creation to avoid 502 on large trees.

    Strategy: Build tree incrementally by adding files in batches of BATCH_SIZE,
    using the previous tree as base_tree for the next batch.
    """
    BATCH_SIZE = 100  # Max entries per tree API call (GitHub 502s at 500+)

    # Step 1: Get current commit SHA
    ref_data, status = api('GET', f'{BASE_URL}/git/ref/heads/{branch}')
    if status != 200:
        print(f"ERROR getting ref: {ref_data}")
        return False
    parent_sha = ref_data['object']['sha']

    # Get the tree SHA of the parent commit
    commit_info, _ = api('GET', f'{BASE_URL}/git/commits/{parent_sha}')
    base_tree_sha = commit_info['tree']['sha']

    # Step 2: Create blobs (with cache)
    cache = load_blob_cache()
    tree_items = []  # (rel_path, blob_sha) for all files
    created = 0
    cached = 0

    print(f"Creating blobs for {len(files)} files (cached blobs will be skipped)...", flush=True)
    for i, (fp, rel) in enumerate(files):
        fhash = file_sha256(fp)
        cache_key = f"{rel}:{fhash}"

        if cache_key in cache:
            blob_sha = cache[cache_key]
            cached += 1
        else:
            with open(fp, 'rb') as f:
                content_b64 = base64.b64encode(f.read()).decode('ascii')

            blob_data, status = api('POST', f'{BASE_URL}/git/blobs', {
                'content': content_b64,
                'encoding': 'base64'
            })

            if status not in (200, 201):
                print(f"  ERROR creating blob for {rel}: {blob_data}")
                continue

            blob_sha = blob_data['sha']
            cache[cache_key] = blob_sha
            created += 1

        tree_items.append({
            'path': rel,
            'mode': '100644',
            'type': 'blob',
            'sha': blob_sha
        })

        if (i + 1) % 100 == 0:
            print(f"  ...{i+1}/{len(files)} processed ({created} new, {cached} cached)", flush=True)
            save_blob_cache(cache)  # Periodic save

    save_blob_cache(cache)
    print(f"  {len(tree_items)} blobs ready ({created} new, {cached} cached)", flush=True)

    # Step 3: Build tree in batches
    # Each batch adds BATCH_SIZE entries, using previous tree as base
    current_tree_sha = base_tree_sha
    n_batches = (len(tree_items) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(n_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(tree_items))
        batch = tree_items[start:end]

        print(f"  Creating tree batch {batch_idx+1}/{n_batches} ({len(batch)} entries, total {end}/{len(tree_items)})...", flush=True)

        tree_data, status = api('POST', f'{BASE_URL}/git/trees', {
            'base_tree': current_tree_sha,
            'tree': batch
        }, timeout=180)

        if status not in (200, 201):
            print(f"  ERROR creating tree batch {batch_idx+1}: {tree_data}")
            # Retry once after 5s
            print(f"  Retrying in 5s...", flush=True)
            time.sleep(5)
            tree_data, status = api('POST', f'{BASE_URL}/git/trees', {
                'base_tree': current_tree_sha,
                'tree': batch
            }, timeout=300)
            if status not in (200, 201):
                print(f"  ERROR on retry: {tree_data}")
                return False

        current_tree_sha = tree_data['sha']

    print(f"  Final tree SHA: {current_tree_sha[:8]}", flush=True)

    # Step 4: Create commit
    print("Creating commit...", flush=True)
    commit_data, status = api('POST', f'{BASE_URL}/git/commits', {
        'message': commit_msg,
        'tree': current_tree_sha,
        'parents': [parent_sha]
    })
    if status not in (200, 201):
        print(f"ERROR creating commit: {commit_data}")
        return False

    # Step 5: Update branch ref
    ref_data, status = api('PATCH', f'{BASE_URL}/git/refs/heads/{branch}', {
        'sha': commit_data['sha']
    })
    if status != 200:
        print(f"ERROR updating ref: {ref_data}")
        return False

    print(f"Committed! SHA: {commit_data['sha'][:8]}", flush=True)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--message', '-m', default='Update NON-OFS Cluster Explorer')
    args = parser.parse_args()

    files = collect_files()
    print(f"Files to push: {len(files)}")
    total_size = sum(os.path.getsize(fp) for fp, _ in files)
    print(f"Total size: {total_size/1e6:.1f} MB")

    if args.dry_run:
        for fp, rel in files:
            size = os.path.getsize(fp)
            print(f"  {rel}  ({size:,} bytes)")
        print("\n(dry run)")
        return

    # Check if repo exists
    _, status = api('GET', BASE_URL)
    if status == 404:
        print("Creating repository...", flush=True)
        result, status = api('POST', 'https://api.github.com/user/repos', {
            'name': REPO,
            'description': 'NON-OFS ILEC Cluster Explorer — Interactive 3-tier drill-down map for 12.3M addresses, 68K clusters',
            'private': True,
            'auto_init': True
        })
        if status not in (200, 201):
            print(f"ERROR creating repo: {result}")
            return
        print(f"Created: {result['html_url']}")
        time.sleep(2)  # Wait for initialization

    t0 = time.time()
    success = push_via_batched_trees(files, args.message)
    elapsed = time.time() - t0

    if success:
        print(f"\nDone in {elapsed/60:.1f} min")
        print(f"View: https://github.com/{OWNER}/{REPO}")
        # Enable GitHub Pages
        pages_data, status = api('POST', f'{BASE_URL}/pages', {
            'source': {'branch': 'main', 'path': '/'}
        })
        if status in (200, 201):
            print(f"Pages: https://{OWNER}.github.io/{REPO}/")
        elif status == 409:
            print(f"Pages already enabled: https://{OWNER}.github.io/{REPO}/")
    else:
        print(f"\nFailed after {elapsed:.0f}s")


if __name__ == '__main__':
    main()
