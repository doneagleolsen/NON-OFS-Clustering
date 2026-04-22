"""
Monthly Score Refresh Pipeline

Re-queries Oracle for updated BI scoring, obligation flags, EWO status.
Joins to existing cluster assignments (stable boundaries).
Recomputes: CPP, IRR, obligation tags, Eisenhower scores.
Writes updated Oracle tables with new RUN_DATE partition.

Does NOT re-cluster — boundaries are stable.

Usage:
    python monthly_refresh.py
    python monthly_refresh.py --dry-run   # Score only, don't write Oracle
"""
import csv, json, os, sys, time

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'


def run_refresh(dry_run=False):
    """Execute monthly refresh pipeline."""
    t0 = time.time()
    print("=" * 70, flush=True)
    print("NON-OFS ILEC MASTER — MONTHLY SCORE REFRESH", flush=True)
    print(f"  Working directory: {OUT}", flush=True)
    print("=" * 70, flush=True)

    # Step 1: Re-extract address data (updated BI scores)
    print("\n[1/5] Re-extracting address data from Oracle...", flush=True)
    print("  Running extract_all_nonofs_addresses.py...", flush=True)
    rc = os.system(f'python "{os.path.join(OUT, "extract_all_nonofs_addresses.py")}"')
    if rc != 0:
        print("  ERROR: Extraction failed", flush=True)
        return

    # Step 2: Re-tag obligations (updated flags)
    print("\n[2/5] Re-tagging obligations...", flush=True)
    print("  Running tag_obligations_v2.py...", flush=True)
    rc = os.system(f'python "{os.path.join(OUT, "tag_obligations_v2.py")}"')
    if rc != 0:
        print("  ERROR: Obligation tagging failed", flush=True)
        return

    # Step 3: Re-score Eisenhower (updated urgency/value)
    print("\n[3/5] Re-scoring Eisenhower matrix...", flush=True)
    print("  Running eisenhower_scoring.py...", flush=True)
    rc = os.system(f'python "{os.path.join(OUT, "eisenhower_scoring.py")}"')
    if rc != 0:
        print("  ERROR: Eisenhower scoring failed", flush=True)
        return

    # Step 4: Regenerate polygons (metadata updated, geometry unchanged)
    print("\n[4/5] Regenerating polygon metadata...", flush=True)
    print("  Running generate_cluster_polygons.py...", flush=True)
    rc = os.system(f'python "{os.path.join(OUT, "generate_cluster_polygons.py")}"')
    if rc != 0:
        print("  ERROR: Polygon generation failed", flush=True)
        return

    # Step 5: Write to Oracle (new RUN_DATE partition)
    if not dry_run:
        print("\n[5/5] Writing to Oracle master tables...", flush=True)
        print("  Running write_oracle_master_tables.py...", flush=True)
        rc = os.system(f'python "{os.path.join(OUT, "write_oracle_master_tables.py")}"')
        if rc != 0:
            print("  ERROR: Oracle write failed", flush=True)
            return
    else:
        print("\n[5/5] DRY RUN — skipping Oracle write", flush=True)

    elapsed = time.time() - t0
    print(f"\n{'='*70}", flush=True)
    print(f"MONTHLY REFRESH COMPLETE — {elapsed/60:.1f} min", flush=True)
    print(f"{'='*70}", flush=True)


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    run_refresh(dry_run=dry_run)
    print("\nDone.", flush=True)
