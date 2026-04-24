"""
Telecom Hub Clustering v7 — OFS-Aware, Barrier-Ready, Financial-Quality-Driven

Key changes from v6:
  1. OFS EXCLUSION ZONES: Addresses inside existing FiOS hub service areas are
     pre-filtered before clustering. NON-OFS clusters build AROUND OFS clusters.
  2. OFS ZONE BARRIER: During growth, crossing into an OFS exclusion zone is
     treated as a barrier (like crossing water).
  3. INFILL TAGGING: "Donut hole" addresses near OFS zones get an infill bonus.
  4. Prepared for Phase 0 (multi-barrier) — accepts barrier_paths list.

Usage:
    from telecom_clustering_v7 import cluster_addresses_v7, get_morphology_params
    from ofs_integration import get_ofs_data_for_wc

    ofs_data = get_ofs_data_for_wc('LGNRPALI')
    params = get_morphology_params('SUBURBAN')
    result = cluster_addresses_v7(addresses, road_graph, params,
                                  ofs_exclusion_zones=ofs_data['exclusion_zones'])
"""
import math
import json
from collections import defaultdict

# Import shared utilities from v5 (unchanged)
from telecom_clustering_v5 import (
    MORPHOLOGY_PARAMS, DEFAULT_PARAMS,
    get_morphology_params, haversine_ft,
)

# Import OFS integration utilities
from ofs_integration import _point_in_polygon, haversine_ft as ofs_haversine_ft

# ═══════════════════════════════════════════════════════════════════════════════
# V7 = V6 weights (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

W_IRR_V2 = 0.40
W_COPPER_SALVAGE = 0.25
W_BI_RANK = 0.20
W_DENSITY = 0.15

GW_FINANCIAL = 0.35
GW_PROXIMITY = 0.25
GW_STREET = 0.15
GW_IRR_GRAVITY = 0.25

# Infill bonus: addresses flagged as donut holes get a formation score boost
INFILL_BONUS = 15.0  # points added to fin_score (0-100 scale)


def compute_formation_scores(addresses):
    """
    Score each address 0-100 for cluster formation.
    Purely financial/engineering — NO obligation input.
    Infill-tagged addresses get a bonus.
    """
    irr_vals = [a.get('irr_v2', 0) for a in addresses if a.get('irr_v2', 0) > 0]
    rank_vals = [a.get('priority_rank', 1.0) for a in addresses
                 if a.get('priority_rank') is not None]
    salvage_vals = [a.get('copper_salvage', 0) for a in addresses
                    if a.get('copper_salvage', 0) > 0]

    if irr_vals:
        irr_min = min(irr_vals)
        irr_range = max(max(irr_vals) - irr_min, 0.001)
    else:
        irr_min, irr_range = 0, 1

    if rank_vals:
        rank_min = min(rank_vals)
        rank_range = max(max(rank_vals) - rank_min, 0.01)
    else:
        rank_min, rank_range = 0, 1

    if salvage_vals:
        salvage_max = max(salvage_vals)
    else:
        salvage_max = 1

    for a in addresses:
        irr = a.get('irr_v2', 0)
        irr_score = (irr - irr_min) / irr_range if irr > 0 else 0.0

        salvage = a.get('copper_salvage', 0)
        salvage_score = min(salvage / salvage_max, 1.0) if salvage > 0 else 0.0

        rank = a.get('priority_rank', 1.0)
        rank_score = 1.0 - ((rank - rank_min) / rank_range) if rank and rank > 0 else 0.0

        units = a.get('units', 1)
        density_score = min(units / 10.0, 1.0)

        composite = (W_IRR_V2 * irr_score +
                     W_COPPER_SALVAGE * salvage_score +
                     W_BI_RANK * rank_score +
                     W_DENSITY * density_score)

        score = round(composite * 100, 1)

        # V7: Infill bonus for donut hole addresses
        if a.get('is_infill'):
            score = min(score + INFILL_BONUS, 100.0)

        a['fin_score'] = score
        a['irr_score'] = irr_score


# ═══════════════════════════════════════════════════════════════════════════════
# OFS zone helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _in_any_ofs_zone(lat, lon, ofs_zones):
    """Check if a point falls inside any OFS exclusion zone."""
    for z in ofs_zones:
        if _point_in_polygon(lon, lat, z['hull']):
            return z['hub_name']
    return None


def _growth_crosses_ofs_zone(hub_lat, hub_lon, cand_lat, cand_lon, ofs_zones):
    """
    Check if the line from hub centroid to candidate crosses into an OFS zone.
    Uses midpoint sampling (3 sample points along the line).
    """
    for t in [0.25, 0.5, 0.75]:
        mid_lat = hub_lat + t * (cand_lat - hub_lat)
        mid_lon = hub_lon + t * (cand_lon - hub_lon)
        if _in_any_ofs_zone(mid_lat, mid_lon, ofs_zones):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# V7 Main clustering function
# ═══════════════════════════════════════════════════════════════════════════════

def cluster_addresses_v7(addresses, road_graph, params,
                         barrier_water_path=None, core_fiber_path=None,
                         ofs_exclusion_zones=None, infill_ids=None,
                         morphology=None, use_network_distance=True):
    """
    V7 clustering: OFS-aware, geospatial-first, financial-quality-driven.

    Changes from v6:
      - Pre-filters addresses inside OFS exclusion zones
      - Growth avoids crossing OFS zone boundaries
      - Infill-tagged addresses get formation score boost

    Args:
        addresses: list of dicts with lat, lon, units, irr_v2, copper_salvage,
                   priority_rank, road_group (from road_graph.snap_addresses)
        road_graph: RoadGraph instance
        params: from get_morphology_params()
        barrier_water_path: optional GeoJSON of water barriers
        core_fiber_path: optional GeoJSON of existing fiber
        ofs_exclusion_zones: list of zone dicts from ofs_integration.build_ofs_exclusion_zones()
        infill_ids: set of LOCUS_ADDRESS_IDs flagged as infill opportunities
        morphology: 'RURAL', 'URBAN', or 'SUBURBAN' (for barrier classification)
        use_network_distance: if True, use road-network distance instead of Euclidean

    Returns:
        (hub_centroids, rg_units, clustering_stats)
    """
    TARGET_HUB = params['target_units']
    MIN_HUB = params['min_units']
    MAX_HUB = params['max_units']
    MAX_RADIUS = params['max_radius_ft']
    PROX_FALLBACK = params['proximity_fallback_ft']

    ofs_zones = ofs_exclusion_zones or []
    infill_set = infill_ids or set()
    excluded_count = 0

    if not addresses:
        return {}, {}, {}

    # V7: Classify highway barriers if not already done
    if not hasattr(road_graph, 'highway_segments') or road_graph.highway_segments is None:
        morph = morphology or 'SUBURBAN'
        road_graph.classify_barriers(morphology=morph)

    # V7: Compute connected components (addresses in different components cannot share a hub)
    if not hasattr(road_graph, 'node_component') or not road_graph.node_component:
        road_graph.compute_barrier_components()

    # Helper: crosses any barrier (highway + rail)
    def crosses_barrier(lat1, lon1, lat2, lon2):
        if road_graph.crosses_highway(lat1, lon1, lat2, lon2):
            return True
        if hasattr(road_graph, 'crosses_rail') and road_graph.crosses_rail(lat1, lon1, lat2, lon2):
            return True
        return False

    # V7: Pre-compute edge weights for network-distance mode
    if use_network_distance:
        if not hasattr(road_graph, 'edge_weight'):
            road_graph.compute_edge_weights()
        print(f"  Network-distance mode: ON (walking salesman)")

    # ── V7 Pre-filter: Remove addresses inside OFS exclusion zones ────────
    if ofs_zones:
        original_count = len(addresses)
        filtered = []
        excluded_count = 0
        for a in addresses:
            zone = _in_any_ofs_zone(a['lat'], a['lon'], ofs_zones)
            if zone:
                a['excluded_by_ofs'] = zone
                excluded_count += 1
            else:
                # Tag infill addresses
                if a.get('address_id') in infill_set:
                    a['is_infill'] = True
                filtered.append(a)

        addresses = filtered
        print(f"  V7 OFS filter: {excluded_count} addresses excluded "
              f"({excluded_count}/{original_count} = {excluded_count*100/max(original_count,1):.1f}% inside OFS zones)")
        print(f"    {len(addresses)} addresses remain for clustering")
    else:
        print(f"  No OFS exclusion zones provided (running in V6-compatible mode)")

    print(f"  Clustering {len(addresses)} addresses (v7: OFS-aware + financial)")
    print(f"    Target={TARGET_HUB}, Min={MIN_HUB}, Max={MAX_HUB}, Radius={MAX_RADIUS:,}ft")

    if not addresses:
        return {}, {}, {'ofs_excluded': excluded_count if ofs_zones else 0}

    # ── Phase 1: Formation scoring (with infill bonus) ─────────────────────
    print("  Phase 1: Computing formation scores (IRR/copper/BI/density + infill)...")
    compute_formation_scores(addresses)
    scores = [a.get('fin_score', 0) for a in addresses if a.get('fin_score', 0) > 0]
    if scores:
        infill_count = sum(1 for a in addresses if a.get('is_infill'))
        print(f"    Score range: {min(scores):.1f} - {max(scores):.1f}, "
              f"median: {sorted(scores)[len(scores)//2]:.1f}"
              f"{f', {infill_count} infill-boosted' if infill_count else ''}")

    # ── Phase 2: Road-group units ──────────────────────────────────────────
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

    # Assign unsnapped to nearest road group
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

    # V7: Pre-snap road group centroids to nearest graph nodes for network distance
    if use_network_distance:
        snapped_nodes = 0
        for gid, rgu in rg_units.items():
            nid, snap_d = road_graph.snap_point_to_node(rgu['lat'], rgu['lon'])
            rgu['nearest_node'] = nid
            rgu['snap_to_node_ft'] = snap_d
            rgu['component_id'] = road_graph.node_component.get(nid, -1) if nid is not None else -1
            if nid is not None:
                snapped_nodes += 1
        print(f"    Pre-snapped {snapped_nodes}/{len(rg_units)} groups to graph nodes")

    # ── Phase 3: Build group adjacency ─────────────────────────────────────
    print("  Phase 3: Building group adjacency...")
    group_adj = defaultdict(set)

    # Build set of barrier road IDs for fast lookup
    barrier_rids = getattr(road_graph, 'barrier_road_ids', set())

    for nid, road_ids in road_graph.node_roads.items():
        # Skip adjacency through barrier-only nodes:
        # If ALL roads at this node are barrier roads, don't connect groups through it
        non_barrier_rids = [rid for rid in road_ids if rid not in barrier_rids]
        if not non_barrier_rids and barrier_rids:
            continue  # node only has barrier roads — no cross-barrier adjacency

        groups_at_node = set()
        for rid in non_barrier_rids if barrier_rids else road_ids:
            r = road_graph.roads.get(rid)
            if r:
                groups_at_node.add(r.get('group_id', -1))
        group_list = list(groups_at_node)
        for i in range(len(group_list)):
            for j in range(i + 1, len(group_list)):
                gi, gj = group_list[i], group_list[j]
                if gi in rg_units and gj in rg_units:
                    # Component constraint: don't connect groups across barriers
                    ci = rg_units[gi].get('component_id', -1)
                    cj = rg_units[gj].get('component_id', -1)
                    if ci != -1 and cj != -1 and ci != cj:
                        continue
                    group_adj[gi].add(gj)
                    group_adj[gj].add(gi)

    road_adj_count = sum(1 for g in rg_units if g in group_adj and group_adj[g])

    # Proximity fallback — must also respect highway barriers
    prox_added = 0
    prox_blocked = 0
    gid_list = list(rg_units.keys())
    for i in range(len(gid_list)):
        for j in range(i + 1, len(gid_list)):
            g1, g2 = gid_list[i], gid_list[j]
            if g2 in group_adj.get(g1, set()):
                continue
            d = haversine_ft(rg_units[g1]['lat'], rg_units[g1]['lon'],
                             rg_units[g2]['lat'], rg_units[g2]['lon'])
            if d <= PROX_FALLBACK:
                # Check highway barrier crossing before adding edge
                if crosses_barrier(
                        rg_units[g1]['lat'], rg_units[g1]['lon'],
                        rg_units[g2]['lat'], rg_units[g2]['lon']):
                    prox_blocked += 1
                    continue
                # Component constraint
                c1 = rg_units[g1].get('component_id', -1)
                c2 = rg_units[g2].get('component_id', -1)
                if c1 != -1 and c2 != -1 and c1 != c2:
                    prox_blocked += 1
                    continue
                # Also check OFS zone crossing
                if ofs_zones and _growth_crosses_ofs_zone(
                        rg_units[g1]['lat'], rg_units[g1]['lon'],
                        rg_units[g2]['lat'], rg_units[g2]['lon'], ofs_zones):
                    prox_blocked += 1
                    continue
                group_adj[g1].add(g2)
                group_adj[g2].add(g1)
                prox_added += 1

    # Copper cable adjacency — groups connected by copper plant become adjacent
    copper_added = 0
    copper_adj = getattr(road_graph, 'copper_adj', {})
    if copper_adj:
        for (g1, g2), pairs in copper_adj.items():
            if g1 not in rg_units or g2 not in rg_units:
                continue
            if g2 in group_adj.get(g1, set()):
                continue  # already adjacent
            # Still respect highway barriers
            if crosses_barrier(
                    rg_units[g1]['lat'], rg_units[g1]['lon'],
                    rg_units[g2]['lat'], rg_units[g2]['lon']):
                continue
            group_adj[g1].add(g2)
            group_adj[g2].add(g1)
            copper_added += 1

    connected = sum(1 for g in rg_units if g in group_adj and group_adj[g])
    copper_msg = f", {copper_added} copper-cable" if copper_added else ""
    print(f"    {road_adj_count} road-adjacent + {prox_added} proximity ({PROX_FALLBACK:,} ft)"
          f"{copper_msg}"
          f"{f', {prox_blocked} blocked by barriers' if prox_blocked else ''}")

    # ── Phase 4: Seed-and-grow (financial-driven, OFS-aware) ───────────────
    mode_label = "network-distance" if use_network_distance else "Euclidean"
    print(f"  Phase 4: Seed-and-grow (financial + OFS-aware, {mode_label})...")

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

        # Network distance: compute Dijkstra from seed node
        dijkstra_cache = None
        dijkstra_node = None
        if use_network_distance:
            seed_node = seed_group.get('nearest_node')
            if seed_node is not None:
                dijkstra_cache = road_graph.shortest_path_from(
                    seed_node, max_dist_ft=MAX_RADIUS, respect_barriers=True)
                dijkstra_node = seed_node

        frontier = set(group_adj.get(gid, set()))

        while hub_units < TARGET_HUB and frontier:
            hub_lat = sum(rg_units[m]['lat'] * rg_units[m]['units']
                          for m in hub_members) / hub_units
            hub_lon = sum(rg_units[m]['lon'] * rg_units[m]['units']
                          for m in hub_members) / hub_units

            # Refresh Dijkstra if hub centroid moved to a different node
            if use_network_distance:
                hub_node, _ = road_graph.snap_point_to_node(hub_lat, hub_lon)
                if hub_node is not None and hub_node != dijkstra_node:
                    dijkstra_cache = road_graph.shortest_path_from(
                        hub_node, max_dist_ft=MAX_RADIUS, respect_barriers=True)
                    dijkstra_node = hub_node

            candidates = []
            for cand_gid in frontier:
                if cand_gid in assigned:
                    continue
                cand = rg_units.get(cand_gid)
                if not cand:
                    continue
                if hub_units + cand['units'] > MAX_HUB:
                    continue

                # Hard barrier check: block candidate if ALL hub members are
                # across a highway from it (nearest-member test, not centroid)
                nearest_member_crosses = True
                for m_gid in hub_members:
                    m = rg_units[m_gid]
                    if not crosses_barrier(m['lat'], m['lon'],
                                                     cand['lat'], cand['lon']):
                        nearest_member_crosses = False
                        break
                if nearest_member_crosses and hub_members:
                    continue
                # Water barrier (centroid-based is OK, water bodies are large)
                if road_graph.crosses_water(hub_lat, hub_lon,
                                            cand['lat'], cand['lon']):
                    continue

                # Distance check: network or Euclidean
                if use_network_distance and dijkstra_cache is not None:
                    cand_node = cand.get('nearest_node')
                    if cand_node is not None:
                        net_dist = dijkstra_cache.get(cand_node, float('inf'))
                    else:
                        net_dist = float('inf')

                    if net_dist == float('inf'):
                        # Same component but disconnected locally — 2x Euclidean penalty
                        euc_dist = haversine_ft(seed_lat, seed_lon,
                                                cand['lat'], cand['lon'])
                        net_dist = euc_dist * 2.0

                    if net_dist > MAX_RADIUS:
                        continue
                    dist_to_seed = net_dist
                else:
                    dist_to_seed = haversine_ft(seed_lat, seed_lon,
                                                cand['lat'], cand['lon'])
                    if dist_to_seed > MAX_RADIUS:
                        continue

                    # Water barrier check (only in Euclidean mode; network mode
                    # respects barriers via Dijkstra edge exclusion)
                    if road_graph.crosses_water(hub_lat, hub_lon,
                                                cand['lat'], cand['lon']):
                        continue

                    # Highway barrier check (Euclidean mode only)
                    if crosses_barrier(hub_lat, hub_lon,
                                                  cand['lat'], cand['lon']):
                        continue

                # V7: OFS zone barrier check (always — OFS zones aren't in road graph)
                if ofs_zones and _growth_crosses_ofs_zone(
                        hub_lat, hub_lon, cand['lat'], cand['lon'], ofs_zones):
                    continue

                # Growth scoring — proximity uses same distance mode
                if use_network_distance and dijkstra_cache is not None:
                    cand_node = cand.get('nearest_node')
                    if cand_node is not None:
                        dist_to_hub = dijkstra_cache.get(cand_node, float('inf'))
                    else:
                        dist_to_hub = haversine_ft(hub_lat, hub_lon,
                                                   cand['lat'], cand['lon'])
                    if dist_to_hub == float('inf'):
                        dist_to_hub = haversine_ft(hub_lat, hub_lon,
                                                   cand['lat'], cand['lon']) * 2.0
                else:
                    dist_to_hub = haversine_ft(hub_lat, hub_lon,
                                               cand['lat'], cand['lon'])

                proximity = max(0, 1.0 - dist_to_hub / MAX_RADIUS)

                hub_streets = set()
                for m in hub_members:
                    hub_streets.update(rg_units[m].get('road_names', set()))
                shared = cand.get('road_names', set()) & hub_streets
                street_bonus = GW_STREET if shared else 0.0

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

    # Assign remaining unassigned to nearest hub (respecting barriers)
    for gid, unit in rg_units.items():
        if unit['hub_id'] == -1:
            best_hub = -1
            best_dist = float('inf')
            best_hub_any = -1
            best_dist_any = float('inf')
            for other_gid, other_unit in rg_units.items():
                if other_unit['hub_id'] >= 0:
                    d = haversine_ft(unit['lat'], unit['lon'],
                                     other_unit['lat'], other_unit['lon'])
                    # Prefer non-crossing assignment
                    crosses = crosses_barrier(
                        unit['lat'], unit['lon'],
                        other_unit['lat'], other_unit['lon'])
                    if not crosses and d < best_dist:
                        best_dist = d
                        best_hub = other_unit['hub_id']
                    # Track nearest overall as fallback for isolated dead-ends
                    if d < best_dist_any:
                        best_dist_any = d
                        best_hub_any = other_unit['hub_id']
            if best_hub >= 0:
                unit['hub_id'] = best_hub
            elif best_hub_any >= 0:
                unit['hub_id'] = best_hub_any  # rural dead-end safety valve
            else:
                unit['hub_id'] = 0

    print(f"    {hub_id} initial hubs")

    # ── Phase 5: Size enforcement ──────────────────────────────────────────
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

        tg = hub_groups(target_hid)
        neighbors = []
        for h in active_hubs:
            if h == target_hid:
                continue
            hlat, hlon = hub_centroid(h)
            d = haversine_ft(tlat, tlon, hlat, hlon)
            h_u = hub_total_units(h)
            # Quick distance pre-filter
            if d > MERGE_RADIUS * 2:
                continue
            crosses = road_graph.crosses_water(tlat, tlon, hlat, hlon)
            # V7: Highway barrier — nearest-group-pair test
            # (centroid-to-centroid is too aggressive in rural areas)
            if not crosses:
                hg = hub_groups(h)
                any_clear = False
                for tgrp in tg:
                    for hgrp in hg:
                        if not crosses_barrier(
                                tgrp['lat'], tgrp['lon'],
                                hgrp['lat'], hgrp['lon']):
                            any_clear = True
                            break
                    if any_clear:
                        break
                if not any_clear:
                    crosses = True
            # V7: Also penalize crossing OFS zones during merge
            if not crosses and ofs_zones:
                crosses = _growth_crosses_ofs_zone(tlat, tlon, hlat, hlon, ofs_zones)
            # Copper cable bonus: reduce effective distance for copper-connected hubs
            copper_bonus = False
            if copper_adj:
                for tgrp in tg:
                    tgid = tgrp.get('gid', -1)
                    for hgrp in hub_groups(h):
                        hgid = hgrp.get('gid', -1)
                        key = (min(tgid, hgid), max(tgid, hgid))
                        if key in copper_adj:
                            copper_bonus = True
                            break
                    if copper_bonus:
                        break
            eff_d = d * 0.7 if copper_bonus else d
            neighbors.append((h, eff_d, h_u, crosses))
        neighbors.sort(key=lambda x: (x[3], x[1]))

        merged = False
        for h, d, h_u, crosses in neighbors:
            if crosses:
                continue  # hard constraint: never merge across barriers
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
            if d >= best_d or target_u + hub_total_units(hh) > MAX_HUB or d > MERGE_RADIUS:
                continue
            # Highway barrier — nearest-group-pair test
            any_clear = False
            for tgrp in tg:
                for hgrp in hg:
                    if not crosses_barrier(
                            tgrp['lat'], tgrp['lon'],
                            hgrp['lat'], hgrp['lon']):
                        any_clear = True
                        break
                if any_clear:
                    break
            if not any_clear:
                continue
            # Water barrier (centroid OK — water bodies are large)
            if road_graph and hasattr(road_graph, 'crosses_water') and road_graph.crosses_water(tlat, tlon, hlat, hlon):
                continue
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

    # ── Phase 5b: Barrier violation cleanup ─────────────────────────────────
    # Move road groups that are on the wrong side of a highway from their
    # hub centroid to the nearest hub on the correct side.
    barrier_moves = 0
    for cleanup_pass in range(20):
        moves_this_pass = 0
        current_hubs = set(u['hub_id'] for u in rg_units.values())
        # Cache centroids for this pass
        cent_cache = {}
        for hid in current_hubs:
            grps = [u for u in rg_units.values() if u['hub_id'] == hid]
            tu = sum(g['units'] for g in grps) or 1
            cent_cache[hid] = (
                sum(g['lat'] * g['units'] for g in grps) / tu,
                sum(g['lon'] * g['units'] for g in grps) / tu)

        for gid, unit in rg_units.items():
            hid = unit['hub_id']
            hlat, hlon = cent_cache[hid]
            # Does this group cross a highway from its hub centroid?
            if not crosses_barrier(hlat, hlon, unit['lat'], unit['lon']):
                continue
            # Find nearest hub on same side (no highway crossing)
            best_hub = None
            best_dist = float('inf')
            for other_hid in current_hubs:
                if other_hid == hid:
                    continue
                olat, olon = cent_cache[other_hid]
                if crosses_barrier(olat, olon, unit['lat'], unit['lon']):
                    continue
                d = haversine_ft(olat, olon, unit['lat'], unit['lon'])
                if d < best_dist:
                    best_dist = d
                    best_hub = other_hid
            if best_hub is not None:
                unit['hub_id'] = best_hub
                moves_this_pass += 1
        barrier_moves += moves_this_pass
        if moves_this_pass == 0:
            break

    if barrier_moves > 0:
        # Recompute active hubs (some may now be empty)
        active_hubs = set(u['hub_id'] for u in rg_units.values())
        print(f"    Barrier cleanup: {barrier_moves} groups moved "
              f"({cleanup_pass + 1} passes)")
    else:
        print(f"    Barrier cleanup: 0 violations")

    n_final = len(set(u['hub_id'] for u in rg_units.values()))
    hub_sizes = [sum(u['units'] for u in rg_units.values() if u['hub_id'] == h)
                 for h in set(u['hub_id'] for u in rg_units.values())]
    hub_sizes.sort()
    print(f"    {n_final} final hubs, {sum(hub_sizes)} units")
    print(f"    Hub sizes: min={hub_sizes[0]}, "
          f"median={hub_sizes[len(hub_sizes)//2]}, max={hub_sizes[-1]}")

    # ── Phase 6: Renumber + centroids ──────────────────────────────────────
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

    # Core fiber bias
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

    # Final stats
    n_final = len(set(u['hub_id'] for u in rg_units.values()))
    hub_sizes = {}
    for u in rg_units.values():
        hid = u['hub_id']
        hub_sizes[hid] = hub_sizes.get(hid, 0) + u['units']

    print(f"    {n_final} final hubs, {total_units:,} units")
    if hub_sizes:
        sizes = sorted(hub_sizes.values())
        print(f"    Hub sizes: min={sizes[0]}, median={sizes[len(sizes)//2]}, max={sizes[-1]}")

    # V7 clustering stats
    clustering_stats = {
        'version': 'v7',
        'total_addresses': len(addresses),
        'total_units': total_units,
        'ofs_excluded': excluded_count if ofs_zones else 0,
        'ofs_zones_count': len(ofs_zones),
        'infill_boosted': sum(1 for a in addresses if a.get('is_infill')),
        'hub_count': n_final,
        'hub_sizes': hub_sizes,
    }

    return hub_centroids, rg_units, clustering_stats
