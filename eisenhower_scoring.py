"""
Eisenhower Scoring — Post-Formation Urgency x Value Matrix

Each cluster gets:
  URGENCY_SCORE (0-100): obligation tier + copper recycling imminence + dispatch trend
  VALUE_SCORE (0-100): IRR V2 + penetration terminal + revenue potential
  BUILD_PRIORITY_TIER: Q1 Do First | Q2 Schedule | Q3 Must Do | Q4 Deprioritize

Usage:
    python eisenhower_scoring.py
"""
import csv, json, os, sys
from collections import Counter

sys.path.insert(0, r'C:\Users\v267429\Downloads\AI_Sessions')

OUT = r'C:\Users\v267429\Downloads\AI_Sessions'
CLUSTER_CACHE = os.path.join(OUT, 'v6_clusters_cache.json')
ADDR_CSV = os.path.join(OUT, 'all_nonofs_12m.csv')
OBLIGATION_CSV = os.path.join(OUT, 'addr_obligation_tags.csv')
CONFIG_PATH = os.path.join(OUT, 'clustering_config.json')


def load_config():
    """Load scoring configuration."""
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_addr_data():
    """Load address-level data for scoring (IRR, pen, dispatch, copper).
    Returns dict: laid -> {irr, pen_terminal, dispatch_1yr, copper_cir, ...}
    """
    print("Loading address data for scoring...", flush=True)
    data = {}
    n = 0
    with open(ADDR_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            laid = row['LOCUS_ADDRESS_ID']
            data[laid] = {
                'irr': float(row['COMPUTED_IRR']) if row['COMPUTED_IRR'] else 0,
                'pen_terminal': float(row['PEN_TERMINAL']) if row['PEN_TERMINAL'] else 0,
                'avg_ebitda': float(row['AVG_ANNUAL_EBITDA']) if row['AVG_ANNUAL_EBITDA'] else 0,
                'dispatch_1yr': float(row['DISPATCH_1YR']) if row['DISPATCH_1YR'] else 0,
                'dispatch_3yr': float(row['DISPATCH_3YR']) if row['DISPATCH_3YR'] else 0,
                'copper_cir': int(row['COPPER_CIR_COUNT']) if row['COPPER_CIR_COUNT'] else 0,
                'copper_cust': int(row['COPPER_CUST_COUNT']) if row['COPPER_CUST_COUNT'] else 0,
                'copper_start': row['COPPER_RECYCLING_START_DATE'] or '',
                'units': max(int(row['NO_OF_UNITS']) if row['NO_OF_UNITS'] else 1, 1),
                'cpo_ntas': float(row['CPO_NTAS']) if row['CPO_NTAS'] else 0,
            }
            n += 1
            if n % 3000000 == 0:
                print(f"  ...{n:,}", flush=True)
    print(f"  Loaded {n:,} address records", flush=True)
    return data


def load_obligations():
    """Load obligation tags per address."""
    tags = {}
    if not os.path.exists(OBLIGATION_CSV):
        print(f"  WARNING: {OBLIGATION_CSV} not found", flush=True)
        return tags
    with open(OBLIGATION_CSV, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tags[row['LOCUS_ADDRESS_ID']] = row['OBLIGATION_BUCKET']
    print(f"  Loaded {len(tags):,} obligation tags", flush=True)
    return tags


def compute_urgency_score(cluster, addr_data, obligations, config):
    """
    URGENCY_SCORE (0-100): How soon must this cluster be built?

    Components:
      - Obligation tier (50%): highest obligation in cluster
      - Copper recycling imminence (30%): copper circuit density + timeline
      - Dispatch frequency (20%): maintenance burden trend
    """
    tier_scores = config.get('obligation_tier_scores', {})
    weights = config.get('eisenhower_urgency_weights', {})
    w_oblig = weights.get('obligation_tier', 0.50)
    w_copper = weights.get('copper_recycling_imminence', 0.30)
    w_dispatch = weights.get('dispatch_frequency', 0.20)

    laids = cluster.get('addresses', [])
    if not laids:
        return 0

    # ── Obligation tier component ─────────────────────────────────
    oblig_fill = cluster.get('obligation_fill', {})
    top_oblig = cluster.get('top_obligation', 'DISCRETIONARY')
    oblig_score = tier_scores.get(top_oblig, 0) / 100.0

    # Boost if many addresses have obligations (not just 1)
    total_addrs = sum(oblig_fill.values()) or 1
    non_disc = total_addrs - oblig_fill.get('DISCRETIONARY', 0)
    oblig_density = min(non_disc / total_addrs, 1.0)
    oblig_component = oblig_score * 0.7 + oblig_density * 0.3

    # ── Copper recycling imminence ────────────────────────────────
    copper_total = 0
    copper_imminence = 0
    for laid in laids:
        d = addr_data.get(str(laid), {})
        cir = d.get('copper_cir', 0)
        copper_total += cir
        start = d.get('copper_start', '')
        if start:
            try:
                yr = int(start[:4])
                if yr <= 2026:
                    copper_imminence += cir * 1.0
                elif yr == 2027:
                    copper_imminence += cir * 0.7
                elif yr <= 2029:
                    copper_imminence += cir * 0.3
            except (ValueError, IndexError):
                pass

    # Normalize: high circuit count = higher urgency
    copper_component = 0
    if copper_total > 0:
        imminence_ratio = copper_imminence / copper_total if copper_total else 0
        density = min(copper_total / (len(laids) * 2), 1.0)  # ~2 circuits/addr = max
        copper_component = imminence_ratio * 0.6 + density * 0.4

    # ── Dispatch frequency ────────────────────────────────────────
    dispatch_total = 0
    for laid in laids:
        d = addr_data.get(str(laid), {})
        dispatch_total += d.get('dispatch_1yr', 0)
    avg_dispatch = dispatch_total / max(len(laids), 1)
    dispatch_component = min(avg_dispatch / 5.0, 1.0)  # 5+ dispatches/addr/yr = max

    urgency = (w_oblig * oblig_component +
               w_copper * copper_component +
               w_dispatch * dispatch_component) * 100

    return round(min(urgency, 100), 1)


def compute_value_score(cluster, addr_data, config):
    """
    VALUE_SCORE (0-100): How valuable is this cluster to build?

    Components:
      - IRR V2 (50%): median address-level IRR
      - Penetration terminal (25%): long-term revenue capture
      - Revenue potential (25%): EBITDA × units
    """
    weights = config.get('eisenhower_value_weights', {})
    w_irr = weights.get('irr_v2', 0.50)
    w_pen = weights.get('penetration_terminal', 0.25)
    w_rev = weights.get('revenue_potential', 0.25)

    laids = cluster.get('addresses', [])
    if not laids:
        return 0

    irr_vals = []
    pen_vals = []
    rev_total = 0

    for laid in laids:
        d = addr_data.get(str(laid), {})
        irr = d.get('irr', 0)
        if irr > 0:
            irr_vals.append(irr)
        pen = d.get('pen_terminal', 0)
        if pen > 0:
            pen_vals.append(pen)
        rev_total += d.get('avg_ebitda', 0) * d.get('units', 1)

    # ── IRR component (0-100 scale, where 15% IRR = 50 score) ────
    if irr_vals:
        median_irr = sorted(irr_vals)[len(irr_vals) // 2]
        # IRR is in percentage format (15 = 15%)
        # Map: 0%->0, 15%->50, 30%+->100
        irr_component = min(median_irr / 30.0, 1.0)
    else:
        irr_component = 0

    # ── Penetration terminal ──────────────────────────────────────
    if pen_vals:
        avg_pen = sum(pen_vals) / len(pen_vals)
        # Penetration is 0-1 scale; typical range 0.30-0.60
        pen_component = min(avg_pen / 0.60, 1.0)
    else:
        pen_component = 0

    # ── Revenue potential (EBITDA per unit) ────────────────────────
    total_units = cluster.get('total_units', 1)
    ebitda_per_unit = rev_total / max(total_units, 1)
    # Map: $0->0, $500/unit->50, $1000+/unit->100
    rev_component = min(ebitda_per_unit / 1000.0, 1.0)

    value = (w_irr * irr_component +
             w_pen * pen_component +
             w_rev * rev_component) * 100

    return round(min(value, 100), 1)


def assign_quadrant(urgency, value, thresholds):
    """Assign Eisenhower quadrant based on scores."""
    if urgency >= 50 and value >= 50:
        return 'Q1_Do_First'
    elif urgency < 50 and value >= 50:
        return 'Q2_Schedule'
    elif urgency >= 50 and value < 50:
        return 'Q3_Must_Do'
    else:
        return 'Q4_Deprioritize'


def score_all_clusters():
    """Score all clusters with Eisenhower urgency x value matrix."""
    config = load_config()
    addr_data = load_addr_data()
    obligations = load_obligations()

    print(f"\nLoading clusters from {CLUSTER_CACHE}...", flush=True)
    with open(CLUSTER_CACHE) as f:
        clusters = json.load(f)
    print(f"  {len(clusters):,} clusters", flush=True)

    thresholds = config.get('eisenhower_thresholds', {})
    quadrant_counts = Counter()

    print("Scoring clusters...", flush=True)
    for i, c in enumerate(clusters):
        urgency = compute_urgency_score(c, addr_data, obligations, config)
        value = compute_value_score(c, addr_data, config)
        quadrant = assign_quadrant(urgency, value, thresholds)

        c['urgency_score'] = urgency
        c['value_score'] = value
        c['build_priority_tier'] = quadrant
        quadrant_counts[quadrant] += 1

        if (i + 1) % 10000 == 0:
            print(f"  ...{i+1:,} scored", flush=True)

    # Save updated clusters
    with open(CLUSTER_CACHE, 'w') as f:
        json.dump(clusters, f)
    print(f"\n  Saved to {CLUSTER_CACHE}", flush=True)

    # Distribution
    print(f"\n  Eisenhower quadrant distribution:", flush=True)
    total = len(clusters)
    for q in ['Q1_Do_First', 'Q2_Schedule', 'Q3_Must_Do', 'Q4_Deprioritize']:
        cnt = quadrant_counts.get(q, 0)
        pct = cnt / total * 100
        units = sum(c['total_units'] for c in clusters if c.get('build_priority_tier') == q)
        print(f"    {q:<18} {cnt:>6,} clusters ({pct:.1f}%), {units:>10,} units", flush=True)

    # Score distribution
    urgencies = [c['urgency_score'] for c in clusters]
    values = [c['value_score'] for c in clusters]
    print(f"\n  Urgency score: min={min(urgencies):.1f}, "
          f"median={sorted(urgencies)[len(urgencies)//2]:.1f}, "
          f"max={max(urgencies):.1f}", flush=True)
    print(f"  Value score:   min={min(values):.1f}, "
          f"median={sorted(values)[len(values)//2]:.1f}, "
          f"max={max(values):.1f}", flush=True)

    # Write scored summary CSV
    out_path = os.path.join(OUT, 'v6_cluster_eisenhower.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['cluster_id', 'clli', 'region', 'sub_region', 'market_density',
                     'lat', 'lon', 'total_units', 'total_addrs', 'total_capex',
                     'avg_cpp', 'median_irr', 'copper_circuits',
                     'urgency_score', 'value_score', 'build_priority_tier',
                     'top_obligation', 'obligation_fraction',
                     'aui_SFU', 'aui_SBU', 'aui_MDU', 'aui_MTU'])
        for c in clusters:
            aui = c.get('aui_units', {})
            w.writerow([
                c['cluster_id'], c['clli'], c.get('region', ''),
                c.get('sub_region', ''), c.get('market_density', ''),
                f"{c['lat']:.6f}", f"{c['lon']:.6f}",
                c['total_units'], c['total_addrs'],
                f"{c.get('total_capex', 0):.0f}",
                f"{c.get('avg_cpp', 0):.0f}",
                f"{c.get('median_irr', 0):.2f}",
                c.get('copper_circuits', 0),
                c.get('urgency_score', 0), c.get('value_score', 0),
                c.get('build_priority_tier', ''),
                c.get('top_obligation', ''),
                f"{c.get('obligation_fraction', 0):.3f}",
                aui.get('SFU', 0), aui.get('SBU', 0),
                aui.get('MDU', 0), aui.get('MTU', 0),
            ])
    print(f"\n  Written: {out_path}", flush=True)

    return clusters


if __name__ == '__main__':
    clusters = score_all_clusters()
    print("\nDone.", flush=True)
