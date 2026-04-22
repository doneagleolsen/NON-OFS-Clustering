"""
Telecom Hub Clustering v6 — Geospatial-First, Financial-Quality-Driven

Key changes from v5:
  1. OBLIGATIONS REMOVED from cluster formation (they are a post-formation tag)
  2. New formation weights: IRR_V2 40%, COPPER_SALVAGE 25%, BI_RANK 20%, DENSITY 15%
  3. Seed ordering by financial quality only (not obligation priority)
  4. Growth scoring: IRR gravity replaces obligation gravity

The geospatial rules (road-graph, morphology, radius caps) are unchanged from v5.

Usage:
    from telecom_clustering_v6 import cluster_addresses_v6, get_morphology_params

    params = get_morphology_params('SUBURBAN')
    result = cluster_addresses_v6(addresses, road_graph, params)
"""
import math
import json
from collections import defaultdict

# Import shared utilities from v5 (unchanged)
from telecom_clustering_v5 import (
    MORPHOLOGY_PARAMS, DEFAULT_PARAMS,
    get_morphology_params, haversine_ft,
)

# ═══════════════════════════════════════════════════════════════════════════════
# V6 Formation Weights — Geospatial + Financial Quality ONLY
# Obligations are a post-formation tag, NOT a clustering input.
# ═══════════════════════════════════════════════════════════════════════════════

W_IRR_V2 = 0.40            # Our address-level IRR (high trust, bottom-up)
W_COPPER_SALVAGE = 0.25    # Copper recycling salvage value
W_BI_RANK = 0.20           # BI PRIORITY_RANK (dispatch/sales/geo synergy)
W_DENSITY = 0.15           # MDU/MTU unit density bonus

# Growth scoring weights (how candidates are added to a hub)
GW_FINANCIAL = 0.35         # avg financial quality of candidate group
GW_PROXIMITY = 0.25         # distance from hub centroid
GW_STREET = 0.15            # shared road names (cable routing affinity)
GW_IRR_GRAVITY = 0.25       # IRR-based pull (high-IRR groups attract growth)


def compute_formation_scores(addresses):
    """
    Score each address 0-100 for cluster formation.
    Purely financial/engineering — NO obligation input.

    Expected address dict keys:
        - irr_v2: float (COMPUTED_IRR from V2 ramp model, 0-1 scale)
        - copper_salvage: float (estimated salvage $ of removable copper, 0+)
        - priority_rank: float (BI rank — lower is better)
        - units: int (NTAS unit count, >1 for MDU/MTU)
    """
    # Normalize IRR (0-1 already, but may be 0-100% in some datasets)
    irr_vals = [a.get('irr_v2', 0) for a in addresses if a.get('irr_v2', 0) > 0]
    rank_vals = [a.get('priority_rank', 1.0) for a in addresses
                 if a.get('priority_rank') is not None]
    salvage_vals = [a.get('copper_salvage', 0) for a in addresses
                    if a.get('copper_salvage', 0) > 0]

    # IRR normalization
    if irr_vals:
        irr_min = min(irr_vals)
        irr_range = max(max(irr_vals) - irr_min, 0.001)
    else:
        irr_min, irr_range = 0, 1

    # BI rank normalization (inverted — lower rank = higher score)
    if rank_vals:
        rank_min = min(rank_vals)
        rank_range = max(max(rank_vals) - rank_min, 0.01)
    else:
        rank_min, rank_range = 0, 1

    # Copper salvage normalization
    if salvage_vals:
        salvage_max = max(salvage_vals)
    else:
        salvage_max = 1

    for a in addresses:
        # IRR V2 component (higher IRR = higher score)
        irr = a.get('irr_v2', 0)
        if irr > 0:
            irr_score = (irr - irr_min) / irr_range
        else:
            irr_score = 0.0

        # Copper salvage component (higher salvage = higher score)
        salvage = a.get('copper_salvage', 0)
        if salvage > 0:
            salvage_score = min(salvage / salvage_max, 1.0)
        else:
            salvage_score = 0.0

        # BI rank component (inverted — lower rank = higher score)
        rank = a.get('priority_rank', 1.0)
        if rank and rank > 0:
            rank_score = 1.0 - ((rank - rank_min) / rank_range)
        else:
            rank_score = 0.0

        # Unit density component (MDU/MTU bonus)
        units = a.get('units', 1)
        density_score = min(units / 10.0, 1.0)  # caps at 10+ units

        composite = (W_IRR_V2 * irr_score +
                     W_COPPER_SALVAGE * salvage_score +
                     W_BI_RANK * rank_score +
                     W_DENSITY * density_score)

        a['fin_score'] = round(composite * 100, 1)
        a['irr_score'] = irr_score  # saved for growth gravity


def cluster_addresses_v6(addresses, road_graph, params,
                         barrier_water_path=None, core_fiber_path=None):
    """
    V6 clustering: geospatial-first, financial-quality-driven.

    Same 6-phase architecture as v5 but:
      - Phase 1 uses formation scores (IRR/copper/BI/density — no obligations)
      - Phase 4 seed ordering by financial score only (no obligation priority)
      - Phase 4 growth uses IRR gravity instead of obligation gravity

    Args:
        addresses: list of dicts with lat, lon, units, irr_v2, copper_salvage,
                   priority_rank, road_group (from road_graph.snap_addresses)
        road_graph: RoadGraph instance
        params: from get_morphology_params()
        barrier_water_path: optional GeoJSON of water barriers
        core_fiber_path: optional GeoJSON of existing fiber

    Returns:
        (hub_centroids, rg_units, hub_obligation_summary)
    """
    TARGET_HUB = params['target_units']
    MIN_HUB = params['min_units']
    MAX_HUB = params['max_units']
    MAX_RADIUS = params['max_radius_ft']
    PROX_FALLBACK = params['proximity_fallback_ft']

    if not addresses:
        return {}, {}, {}

    print(f"  Clustering {len(addresses)} addresses (v6: geospatial+financial)")
    print(f"    Target={TARGET_HUB}, Min={MIN_HUB}, Max={MAX_HUB}, Radius={MAX_RADIUS:,}ft")

    # ── Phase 1: Formation scoring (NO obligations) ──────────────────────
    print("  Phase 1: Computing formation scores (IRR/copper/BI/density)...")
    compute_formation_scores(addresses)
    scores = [a.get('fin_score', 0) for a in addresses if a.get('fin_score', 0) > 0]
    if scores:
        print(f"    Score range: {min(scores):.1f} - {max(scores):.1f}, "
              f"median: {sorted(scores)[len(scores)//2]:.1f}")

    # ── Phase 2: Road-group units ────────────────────────────────────────
    print("  Phase 2: Building road-group units...")
    rg_units = {}

    by_group = defaultdict(list)
    for a in addresses:
        gid = a.get('road_group', -1)
        by_group[gid].append(a)

    unsnapped = by_group.pop(-1, [])
    if unsnapped:
        print(f"    {len(unsnapped)} unsnapped addresses (assigning to nearest group)")

    for gid, addrs_in_group in by_group.items():
        units = sum(a.get('units', 1) for a in addrs_in_group)
        lat = sum(a['lat'] for a in addrs_in_group) / len(addrs_in_group)
        lon = sum(a['lon'] for a in addrs_in_group) / len(addrs_in_group)

        group_scores = [a.get('fin_score', 0) for a in addrs_in_group]
        cofs_vals = [a.get('cofs', 0) for a in addrs_in_group if a.get('cofs', 0) > 0]
        copper = sum(a.get('copper_cir', 0) for a in addrs_in_group)
        road_names = set()
        for a in addrs_in_group:
            rn = a.get('road_name')
            if rn:
                road_names.add(rn)

        # Max IRR score in group (for growth gravity)
        max_irr_score = max(
            (a.get('irr_score', 0) for a in addrs_in_group), default=0
        )

        rg_units[gid] = {
            'gid': gid,
            'addrs': addrs_in_group,
            'n_addrs': len(addrs_in_group),
            'units': units,
            'lat': lat,
            'lon': lon,
            'avg_score': sum(group_scores) / len(group_scores) if group_scores else 0,
            'avg_cofs': sum(cofs_vals) / len(cofs_vals) if cofs_vals else 0,
            'copper': copper,
            'road_names': road_names,
            'max_irr_score': max_irr_score,
            'hub_id': -1,
        }

    # Assign unsnapped addresses to nearest road group
    if unsnapped and rg_units:
        for ua in unsnapped:
            best_gid = None
            best_dist = float('inf')
            for gid, rgu in rg_units.items():
                d = haversine_ft(ua['lat'], ua['lon'], rgu['lat'], rgu['lon'])
                if d < best_dist:
                    best_dist = d
                    best_gid = gid
            if best_gid is not None:
                rgu = rg_units[best_gid]
                rgu['addrs'].append(ua)
                rgu['n_addrs'] += 1
                rgu['units'] += ua.get('units', 1)
                ua['road_group'] = best_gid

    total_units = sum(u['units'] for u in rg_units.values())
    print(f"    {len(rg_units)} road-group units, {total_units:,} total units")

    # ── Phase 3: Build group adjacency (same as v5) ─────────────────────
    print("  Phase 3: Building group adjacency...")
    group_adj = defaultdict(set)
    for nid, road_ids in road_graph.node_roads.items():
        groups_at_node = set()
        for rid in road_ids:
            r = road_graph.roads.get(rid)
            if r:
                groups_at_node.add(r.get('group_id', -1))
        group_list = list(groups_at_node)
        for i in range(len(group_list)):
            for j in range(i + 1, len(group_list)):
                if group_list[i] in rg_units and group_list[j] in rg_units:
                    group_adj[group_list[i]].add(group_list[j])
                    group_adj[group_list[j]].add(group_list[i])

    road_adj_count = sum(1 for g in rg_units if g in group_adj and group_adj[g])

    # Proximity fallback
    prox_added = 0
    gid_list = list(rg_units.keys())
    for i in range(len(gid_list)):
        for j in range(i + 1, len(gid_list)):
            g1, g2 = gid_list[i], gid_list[j]
            if g2 in group_adj.get(g1, set()):
                continue
            d = haversine_ft(rg_units[g1]['lat'], rg_units[g1]['lon'],
                             rg_units[g2]['lat'], rg_units[g2]['lon'])
            if d <= PROX_FALLBACK:
                group_adj[g1].add(g2)
                group_adj[g2].add(g1)
                prox_added += 1

    connected = sum(1 for g in rg_units if g in group_adj and group_adj[g])
    print(f"    {road_adj_count} road-adjacent + {prox_added} proximity ({PROX_FALLBACK:,} ft)")

    # ── Phase 4: Seed-and-grow (FINANCIAL-DRIVEN, radius-capped) ─────────
    print("  Phase 4: Seed-and-grow (financial-quality-driven, radius-capped)...")

    # V6 CHANGE: Sort by financial score only — no obligation priority
    sorted_groups = sorted(
        rg_units.values(),
        key=lambda g: g['avg_score'],
        reverse=True
    )

    hub_id = 0
    assigned = set()

    for seed_group in sorted_groups:
        gid = seed_group['gid']
        if gid in assigned:
            continue

        hub_members = [gid]
        assigned.add(gid)
        hub_units = seed_group['units']
        seed_lat = seed_group['lat']
        seed_lon = seed_group['lon']

        frontier = set(group_adj.get(gid, set()))

        while hub_units < TARGET_HUB and frontier:
            hub_lat = sum(rg_units[m]['lat'] * rg_units[m]['units']
                          for m in hub_members) / hub_units
            hub_lon = sum(rg_units[m]['lon'] * rg_units[m]['units']
                          for m in hub_members) / hub_units

            candidates = []
            for cand_gid in frontier:
                if cand_gid in assigned:
                    continue
                cand = rg_units.get(cand_gid)
                if not cand:
                    continue
                if hub_units + cand['units'] > MAX_HUB:
                    continue

                dist_to_seed = haversine_ft(seed_lat, seed_lon,
                                            cand['lat'], cand['lon'])
                if dist_to_seed > MAX_RADIUS:
                    continue

                if road_graph.crosses_water(hub_lat, hub_lon,
                                            cand['lat'], cand['lon']):
                    continue

                # V6 CHANGE: Combined score with IRR gravity instead of obligation gravity
                dist_to_hub = haversine_ft(hub_lat, hub_lon,
                                           cand['lat'], cand['lon'])
                proximity = max(0, 1.0 - dist_to_hub / MAX_RADIUS)

                hub_streets = set()
                for m in hub_members:
                    hub_streets.update(rg_units[m].get('road_names', set()))
                shared = cand.get('road_names', set()) & hub_streets
                street_bonus = GW_STREET if shared else 0.0

                # IRR gravity replaces obligation gravity
                irr_gravity = cand.get('max_irr_score', 0) * GW_IRR_GRAVITY

                combined = (cand['avg_score'] / 100.0 * GW_FINANCIAL +
                            proximity * GW_PROXIMITY +
                            street_bonus +
                            irr_gravity)
                candidates.append((cand_gid, combined))

            if not candidates:
                break

            candidates.sort(key=lambda x: x[1], reverse=True)
            best_gid = candidates[0][0]
            best_group = rg_units[best_gid]

            best_group['hub_id'] = hub_id
            hub_members.append(best_gid)
            assigned.add(best_gid)
            hub_units += best_group['units']

            for neighbor_gid in group_adj.get(best_gid, set()):
                if neighbor_gid not in assigned:
                    frontier.add(neighbor_gid)
            frontier.discard(best_gid)

        for m_gid in hub_members:
            rg_units[m_gid]['hub_id'] = hub_id
        hub_id += 1

    # Assign remaining unassigned to nearest hub
    for gid, unit in rg_units.items():
        if unit['hub_id'] == -1:
            best_hub = -1
            best_dist = float('inf')
            for other_gid, other_unit in rg_units.items():
                if other_unit['hub_id'] >= 0:
                    d = haversine_ft(unit['lat'], unit['lon'],
                                     other_unit['lat'], other_unit['lon'])
                    if d < best_dist:
                        best_dist = d
                        best_hub = other_unit['hub_id']
            unit['hub_id'] = best_hub if best_hub >= 0 else 0

    print(f"    {hub_id} initial hubs")

    # ── Phase 5: Size enforcement (same as v5) ───────────────────────────
    print("  Phase 5: Size enforcement...")

    def hub_total_units(hid):
        return sum(u['units'] for u in rg_units.values() if u['hub_id'] == hid)

    def hub_groups(hid):
        return [u for u in rg_units.values() if u['hub_id'] == hid]

    def hub_centroid(hid):
        grps = hub_groups(hid)
        tu = sum(g['units'] for g in grps) or 1
        return (sum(g['lat'] * g['units'] for g in grps) / tu,
                sum(g['lon'] * g['units'] for g in grps) / tu)

    active_hubs = set(u['hub_id'] for u in rg_units.values())
    merge_count = 0
    split_count = 0

    MERGE_RADIUS = MAX_RADIUS * 1.5
    stalled = set()
    for _ in range(1000):
        small = [(h, hub_total_units(h)) for h in active_hubs
                 if hub_total_units(h) < MIN_HUB and h not in stalled]
        if not small:
            break
        small.sort(key=lambda x: x[1])
        target_hid, target_u = small[0]
        tlat, tlon = hub_centroid(target_hid)

        neighbors = []
        for h in active_hubs:
            if h == target_hid:
                continue
            hlat, hlon = hub_centroid(h)
            d = haversine_ft(tlat, tlon, hlat, hlon)
            h_u = hub_total_units(h)
            crosses = road_graph.crosses_water(tlat, tlon, hlat, hlon)
            neighbors.append((h, d, h_u, crosses))
        neighbors.sort(key=lambda x: (x[3], x[1]))

        merged = False
        for h, d, h_u, crosses in neighbors:
            if target_u + h_u <= MAX_HUB and d <= MERGE_RADIUS:
                for u in rg_units.values():
                    if u['hub_id'] == target_hid:
                        u['hub_id'] = h
                active_hubs.discard(target_hid)
                merge_count += 1
                merged = True
                break
        if not merged:
            stalled.add(target_hid)

    # Split oversized hubs
    import random
    random.seed(42)
    for split_round in range(20):
        oversized = [(h, hub_total_units(h)) for h in list(active_hubs)
                     if hub_total_units(h) > MAX_HUB]
        if not oversized:
            break
        for h, h_u in oversized:
            h_grps = hub_groups(h)
            if len(h_grps) <= 1:
                continue
            n_pieces = max(2, int(h_u / TARGET_HUB) + 1)
            h_grps.sort(key=lambda g: g['lat'] if split_round % 2 == 0 else g['lon'])
            buckets = [[] for _ in range(n_pieces)]
            bucket_units = [0] * n_pieces
            for g in h_grps:
                min_bucket = min(range(n_pieces), key=lambda b: bucket_units[b])
                buckets[min_bucket].append(g)
                bucket_units[min_bucket] += g['units']
            first = True
            for bucket in buckets:
                if not bucket:
                    continue
                if first:
                    first = False
                    continue
                new_hid = max(active_hubs) + 1
                for g in bucket:
                    g['hub_id'] = new_hid
                active_hubs.add(new_hid)
                split_count += 1

    # Second-pass merge
    stalled2 = set()
    for _ in range(500):
        small = [(h, hub_total_units(h)) for h in active_hubs
                 if hub_total_units(h) < MIN_HUB and h not in stalled2]
        if not small:
            break
        target_hid, target_u = min(small, key=lambda x: x[1])
        tg = hub_groups(target_hid)
        if not tg:
            active_hubs.discard(target_hid)
            continue
        tlat, tlon = hub_centroid(target_hid)
        best_h = None
        best_d = float('inf')
        for hh in active_hubs:
            if hh == target_hid:
                continue
            hg = hub_groups(hh)
            if not hg:
                continue
            hlat, hlon = hub_centroid(hh)
            d = haversine_ft(tlat, tlon, hlat, hlon)
            if d < best_d and target_u + hub_total_units(hh) <= MAX_HUB and d <= MERGE_RADIUS:
                best_d = d
                best_h = hh
        if best_h is not None:
            for u in rg_units.values():
                if u['hub_id'] == target_hid:
                    u['hub_id'] = best_h
            active_hubs.discard(target_hid)
            merge_count += 1
        else:
            stalled2.add(target_hid)

    print(f"    {merge_count} merges, {split_count} splits")

    # ── Phase 6: Renumber + centroids ────────────────────────────────────
    old_ids = sorted(set(u['hub_id'] for u in rg_units.values()))
    remap = {old: new for new, old in enumerate(old_ids)}
    for u in rg_units.values():
        u['hub_id'] = remap.get(u['hub_id'], u['hub_id'])

    for u in rg_units.values():
        for a in u['addrs']:
            a['hub_id'] = u['hub_id']
            a['fin_score'] = a.get('fin_score', 0)

    hub_centroids = {}
    for hid in set(u['hub_id'] for u in rg_units.values()):
        h_grps = [u for u in rg_units.values() if u['hub_id'] == hid]
        total_w = sum(g['avg_score'] * g['units'] for g in h_grps) or 1
        lat = sum(g['lat'] * g['avg_score'] * g['units'] for g in h_grps) / total_w
        lon = sum(g['lon'] * g['avg_score'] * g['units'] for g in h_grps) / total_w
        hub_centroids[hid] = (lat, lon)

    # Core fiber bias (same as v5)
    core_points = []
    if core_fiber_path:
        try:
            with open(core_fiber_path) as f:
                cgj = json.load(f)
            for feat in cgj.get("features", []):
                coords = feat["geometry"].get("coordinates", [])
                if coords and isinstance(coords[0], (int, float)):
                    pass
                elif coords:
                    flat = (coords[0] if isinstance(coords[0][0], (int, float))
                            else coords[0][0] if coords[0] else [])
                    if flat:
                        core_points.append((flat[1], flat[0]))
                    continue
                if len(coords) > 1:
                    mid_c = coords[len(coords) // 2]
                    core_points.append((mid_c[1], mid_c[0]))
        except Exception:
            pass

    if core_points:
        for hid in hub_centroids:
            lat, lon = hub_centroids[hid]
            nearest = min(core_points, key=lambda p: (p[0]-lat)**2 + (p[1]-lon)**2)
            hub_centroids[hid] = (lat * 0.85 + nearest[0] * 0.15,
                                  lon * 0.85 + nearest[1] * 0.15)

    # Hub summary (same as v5)
    n_final = len(set(u['hub_id'] for u in rg_units.values()))
    hub_sizes = {}
    for u in rg_units.values():
        hid = u['hub_id']
        hub_sizes[hid] = hub_sizes.get(hid, 0) + u['units']

    print(f"    {n_final} final hubs, {total_units:,} units")
    if hub_sizes:
        sizes = sorted(hub_sizes.values())
        print(f"    Hub sizes: min={sizes[0]}, median={sizes[len(sizes)//2]}, max={sizes[-1]}")

    # Build empty obligation summary (will be populated post-formation)
    hub_obligation_summary = {}

    return hub_centroids, rg_units, hub_obligation_summary
