"""
Road graph builder for telecom hub clustering.
Builds a graph from TIGER road segments, detects dead-ends,
traces feeder hierarchies, and snaps addresses to roads.

Usage:
    from road_graph import RoadGraph
    rg = RoadGraph(roads_geojson_path)
    rg.snap_addresses(addresses)  # adds 'road_id', 'road_name', 'road_group' to each address
"""
import json
import math
from collections import defaultdict

# Snap tolerance: max distance (ft) from address to road to be considered "on" that road
SNAP_TOLERANCE_FT = 500

# Highway MTFCC codes that act as cluster barriers
# S1100=Interstate, S1200=US/State Highway, S1630=Ramp
HIGHWAY_BARRIER_MTFCC = {'S1100', 'S1200', 'S1630'}
# Rural uses same set — state highways ARE major barriers in rural areas
HIGHWAY_BARRIER_MTFCC_RURAL = {'S1100', 'S1200'}

def haversine_ft(lat1, lon1, lat2, lon2):
    R = 20902231
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def point_to_segment_dist(plat, plon, s1lat, s1lon, s2lat, s2lon):
    """Approximate distance from point to line segment in ft."""
    # Project point onto segment
    dx = s2lon - s1lon
    dy = s2lat - s1lat
    if dx == 0 and dy == 0:
        return haversine_ft(plat, plon, s1lat, s1lon)
    t = max(0, min(1, ((plon - s1lon)*dx + (plat - s1lat)*dy) / (dx*dx + dy*dy)))
    proj_lon = s1lon + t * dx
    proj_lat = s1lat + t * dy
    return haversine_ft(plat, plon, proj_lat, proj_lon)


class RoadGraph:
    def __init__(self, roads_geojson_path, barrier_water_path=None, bbox=None):
        """Build road graph from TIGER GeoJSON.

        Args:
            roads_geojson_path: path to GeoJSON with road features
            barrier_water_path: optional water barrier GeoJSON
            bbox: optional (min_lat, min_lon, max_lat, max_lon) to filter roads.
                  Roads outside this bbox + 0.01° buffer are skipped.
                  Use this to clip county TIGER roads to WC boundary.
        """
        with open(roads_geojson_path) as f:
            gj = json.load(f)

        # Bbox filter setup
        if bbox:
            bbox_min_lat = bbox[0] - 0.01
            bbox_min_lon = bbox[1] - 0.01
            bbox_max_lat = bbox[2] + 0.01
            bbox_max_lon = bbox[3] + 0.01
        else:
            bbox_min_lat = bbox_min_lon = -999
            bbox_max_lat = bbox_max_lon = 999

        self.roads = {}  # dict of road_id -> road dict
        self.nodes = {}  # (rounded_lon, rounded_lat) -> node_id
        self.edges = defaultdict(set)  # node_id -> set of connected node_ids
        self.node_roads = defaultdict(set)  # node_id -> set of road_ids
        self.road_nodes = {}  # road_id -> (start_node, end_node)

        # Load water barriers for zone checking
        self.water_segments = []
        if barrier_water_path:
            try:
                with open(barrier_water_path) as f:
                    wgj = json.load(f)
                for feat in wgj.get("features", []):
                    geom = feat.get("geometry", {})
                    coords = geom.get("coordinates", [])
                    gtype = geom.get("type", "")
                    if gtype == "Polygon":
                        for ring in coords:
                            for i in range(len(ring) - 1):
                                self.water_segments.append((tuple(ring[i]), tuple(ring[i+1])))
            except Exception:
                pass

        # Parse roads
        PREC = 5  # decimal places for node snapping (roughly 3 ft precision)
        next_node = 0
        skipped_bbox = 0

        for idx, feat in enumerate(gj.get("features", [])):
            fullname_raw = feat["properties"].get("FULLNAME", "")
            # TIGER DBF field-shift bug: MTFCC is embedded in FULLNAME
            # Two patterns: "MS1400 ..." (offset 1) or "S1400 ..." (offset 0)
            mtfcc = ""
            if len(fullname_raw) >= 6 and fullname_raw[1:3] == 'S1':
                mtfcc = fullname_raw[1:6]  # e.g., "MS1400..." -> "S1400"
            elif len(fullname_raw) >= 5 and fullname_raw[0:2] == 'S1':
                mtfcc = fullname_raw[0:5]  # e.g., "S1400..." -> "S1400"
            if not mtfcc:
                mtfcc = feat["properties"].get("MTFCC", "")
            name = fullname_raw  # keep original for road grouping (consistent per road)
            coords = feat["geometry"].get("coordinates", [])

            if not coords or len(coords) < 2:
                continue

            # Handle MultiLineString (nested arrays)
            if coords and isinstance(coords[0], list) and isinstance(coords[0][0], list):
                # Flatten: take first line segment
                coords = coords[0]
            if not coords or len(coords) < 2:
                continue

            # Bbox filter: skip roads entirely outside WC area
            if bbox:
                road_lats = [c[1] for c in coords]
                road_lons = [c[0] for c in coords]
                if max(road_lats) < bbox_min_lat or min(road_lats) > bbox_max_lat:
                    skipped_bbox += 1
                    continue
                if max(road_lons) < bbox_min_lon or min(road_lons) > bbox_max_lon:
                    skipped_bbox += 1
                    continue

            # Start and end nodes (rounded for snapping at intersections)
            start = (round(coords[0][0], PREC), round(coords[0][1], PREC))
            end = (round(coords[-1][0], PREC), round(coords[-1][1], PREC))

            # Check if same point (loop or very short segment)
            if start == end and len(coords) <= 3:
                continue

            # Get or create node IDs
            if start not in self.nodes:
                self.nodes[start] = next_node
                next_node += 1
            if end not in self.nodes:
                self.nodes[end] = next_node
                next_node += 1

            start_id = self.nodes[start]
            end_id = self.nodes[end]

            # Store road
            self.roads[idx] = {
                'id': idx,
                'name': name,
                'mtfcc': mtfcc,
                'coords': coords,
                'start_node': start_id,
                'end_node': end_id,
                'lat': sum(c[1] for c in coords) / len(coords),
                'lon': sum(c[0] for c in coords) / len(coords),
            }

            # Build graph edges
            self.edges[start_id].add(end_id)
            self.edges[end_id].add(start_id)
            self.node_roads[start_id].add(idx)
            self.node_roads[end_id].add(idx)
            self.road_nodes[idx] = (start_id, end_id)

        # Node positions (for later use)
        self.node_pos = {}
        for (lon, lat), nid in self.nodes.items():
            self.node_pos[nid] = (lat, lon)

        # Precompute road bounding boxes for fast filtering
        for rid, r in self.roads.items():
            coords = r['coords']
            lats = [c[1] for c in coords]
            lons = [c[0] for c in coords]
            r['min_lat'] = min(lats)
            r['max_lat'] = max(lats)
            r['min_lon'] = min(lons)
            r['max_lon'] = max(lons)

        # Spatial grid index (built lazily on first snap)
        self._grid = None
        self._grid_res = None

        if skipped_bbox > 0:
            print(f"    Road graph: {len(self.roads)} roads, {len(self.nodes)} nodes "
                  f"(filtered {skipped_bbox:,} outside bbox)")
        else:
            print(f"    Road graph: {len(self.roads)} roads, {len(self.nodes)} nodes")

    def find_dead_ends(self):
        """Find dead-end nodes (degree 1) and trace to their feeder road."""
        dead_ends = {}  # node_id -> feeder_road_id

        for nid, neighbors in self.edges.items():
            if len(neighbors) == 1:
                # This is a dead-end node
                # Find which road(s) touch this node
                road_ids = self.node_roads.get(nid, set())
                if road_ids:
                    dead_ends[nid] = list(road_ids)

        # For each dead-end road, trace to its feeder
        dead_end_roads = set()
        road_feeder = {}  # road_id -> feeder_road_id

        for nid, road_ids in dead_ends.items():
            for rid in road_ids:
                road = self.roads[rid]
                # The other end of this road
                other_node = road['end_node'] if road['start_node'] == nid else road['start_node']
                # Find the feeder road at the intersection (other_node)
                feeder_roads = self.node_roads.get(other_node, set()) - {rid}
                if feeder_roads:
                    # Pick the feeder with most connections (likely the main road)
                    best_feeder = max(feeder_roads,
                                      key=lambda fr: len(self.edges.get(self.roads[fr]['start_node'], set())) +
                                                     len(self.edges.get(self.roads[fr]['end_node'], set())))
                    road_feeder[rid] = best_feeder
                    dead_end_roads.add(rid)

        print(f"    Dead-end nodes: {len(dead_ends)}, dead-end roads: {len(dead_end_roads)}")
        return dead_end_roads, road_feeder

    def build_road_groups(self):
        """
        Build road groups: connected road clusters that should be in the same hub.
        Dead-end roads are locked to their feeder road's group.
        """
        dead_end_roads, road_feeder = self.find_dead_ends()

        # Build road adjacency: two roads are adjacent if they share a node
        road_adj = defaultdict(set)
        for nid, road_ids in self.node_roads.items():
            road_list = list(road_ids)
            for i in range(len(road_list)):
                for j in range(i+1, len(road_list)):
                    road_adj[road_list[i]].add(road_list[j])
                    road_adj[road_list[j]].add(road_list[i])

        # Build road groups using union-find
        parent = {rid: rid for rid in self.roads}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        # Lock dead-end roads to their feeders
        for dead_rid, feeder_rid in road_feeder.items():
            union(dead_rid, feeder_rid)

        # Also group roads that share the same name and are adjacent
        name_roads = defaultdict(list)
        for rid, r in self.roads.items():
            if r['name']:
                name_roads[r['name']].append(rid)

        for name, rids in name_roads.items():
            if len(rids) > 1:
                for i in range(len(rids) - 1):
                    if rids[i+1] in road_adj.get(rids[i], set()):
                        union(rids[i], rids[i+1])

        # Extract initial groups
        groups = defaultdict(set)
        for rid in self.roads:
            groups[find(rid)].add(rid)

        # ── Small-group merging ──────────────────────────────────────────────
        # Groups with few roads that share a node with a larger group get absorbed.
        # This fixes cases like Shady Ln (separate name) connecting to Deerfield Ln.
        SMALL_GROUP_THRESHOLD = 3  # groups with <= this many roads are candidates
        merge_count = 0

        # Build group adjacency via shared nodes
        group_adj = defaultdict(set)  # group_root -> set of adjacent group_roots
        for nid, road_ids in self.node_roads.items():
            groups_at_node = set()
            for rid in road_ids:
                groups_at_node.add(find(rid))
            group_list = list(groups_at_node)
            for i in range(len(group_list)):
                for j in range(i+1, len(group_list)):
                    group_adj[group_list[i]].add(group_list[j])
                    group_adj[group_list[j]].add(group_list[i])

        # Iteratively merge small groups into adjacent larger groups
        for _ in range(500):
            merged = False
            for root, members in list(groups.items()):
                if len(members) <= SMALL_GROUP_THRESHOLD and len(members) > 0:
                    # Find adjacent groups, pick largest
                    adj_groups = group_adj.get(root, set())
                    best_target = None
                    best_size = 0
                    for adj_root in adj_groups:
                        adj_members = groups.get(adj_root, set())
                        if len(adj_members) > best_size and adj_root != root:
                            best_size = len(adj_members)
                            best_target = adj_root
                    if best_target is not None:
                        # Merge: union all members of small group into target
                        for rid in members:
                            union(rid, best_target)
                        # Rebuild this group
                        groups[best_target] = groups[best_target] | members
                        del groups[root]
                        # Update adjacency
                        for adj in group_adj.get(root, set()):
                            group_adj[adj].discard(root)
                            if adj != best_target:
                                group_adj[adj].add(best_target)
                                group_adj[best_target].add(adj)
                        if root in group_adj:
                            del group_adj[root]
                        merge_count += 1
                        merged = True
                        break
            if not merged:
                break

        print(f"    Small-group merges: {merge_count}")

        # Re-extract groups after merging
        groups = defaultdict(set)
        for rid in self.roads:
            groups[find(rid)].add(rid)

        # ── Highway barrier split ─────────────────────────────────────────────
        # If classify_barriers() has been called, split road groups that span
        # both sides of a highway.  A "barrier node" is any node incident to a
        # barrier road (highway intersection).  Within each group we BFS only
        # through non-barrier nodes; disconnected components become new groups.
        split_count = 0
        if hasattr(self, 'barrier_road_ids') and self.barrier_road_ids:
            barrier_nodes = set()
            for rid in self.barrier_road_ids:
                r = self.roads.get(rid)
                if r:
                    barrier_nodes.add(r['start_node'])
                    barrier_nodes.add(r['end_node'])

            # Build per-road node list (non-barrier nodes only) for fast lookup
            road_nonbarrier_nodes = {}
            for rid, r in self.roads.items():
                nodes = set()
                if r['start_node'] not in barrier_nodes:
                    nodes.add(r['start_node'])
                if r['end_node'] not in barrier_nodes:
                    nodes.add(r['end_node'])
                road_nonbarrier_nodes[rid] = nodes

            new_groups = {}
            next_gid_split = 0
            for root, members in groups.items():
                if len(members) <= 1:
                    new_groups[next_gid_split] = members
                    next_gid_split += 1
                    continue

                # Build within-group adjacency via shared non-barrier nodes
                node_to_roads = defaultdict(set)
                for rid in members:
                    for nid in road_nonbarrier_nodes.get(rid, set()):
                        node_to_roads[nid].add(rid)

                local_adj = defaultdict(set)
                for nid, rids in node_to_roads.items():
                    rids_list = list(rids)
                    for i in range(len(rids_list)):
                        for j in range(i + 1, len(rids_list)):
                            local_adj[rids_list[i]].add(rids_list[j])
                            local_adj[rids_list[j]].add(rids_list[i])

                # BFS connected components within group
                visited = set()
                components = []
                for rid in members:
                    if rid in visited:
                        continue
                    comp = set()
                    queue = [rid]
                    visited.add(rid)
                    while queue:
                        cur = queue.pop()
                        comp.add(cur)
                        for nb in local_adj.get(cur, set()):
                            if nb not in visited and nb in members:
                                visited.add(nb)
                                queue.append(nb)
                    components.append(comp)

                if len(components) > 1:
                    split_count += 1
                for comp in components:
                    new_groups[next_gid_split] = comp
                    next_gid_split += 1

            groups = new_groups
            if split_count > 0:
                print(f"    Highway barrier splits: {split_count} groups split "
                      f"into {len(groups)} total")

        # Assign group_id to each road
        group_map = {}
        for gid, members in sorted(groups.items()):
            for rid in members:
                group_map[rid] = gid

        for rid, r in self.roads.items():
            r['group_id'] = group_map.get(rid, rid)

        n_groups = len(groups)
        largest = max(len(v) for v in groups.values()) if groups else 0
        print(f"    Road groups: {n_groups} (largest: {largest} roads)")
        return groups, group_map

    def _build_grid(self, addresses):
        """Build spatial grid index for fast road lookup.

        Grid cell size is ~0.002° ≈ 600 ft at mid-latitudes.
        Each cell stores the set of road IDs whose bounding box overlaps it.

        Also precomputes flattened segment arrays for fast distance computation.
        """
        # Determine grid resolution from address spread
        lats = [a['lat'] for a in addresses]
        lons = [a['lon'] for a in addresses]
        lat_range = max(lats) - min(lats) if lats else 0.1
        lon_range = max(lons) - min(lons) if lons else 0.1

        # Cell size: ~600 ft ≈ 0.002° lat.  Slightly larger for sparse areas.
        cell_size = 0.002
        if lat_range > 0.5:  # very spread out WC
            cell_size = 0.004

        self._grid_res = cell_size
        self._grid = defaultdict(set)
        pad = cell_size  # one-cell padding for road bbox overlap

        for rid, r in self.roads.items():
            # Grid cells this road touches (bbox + padding)
            min_row = int((r['min_lat'] - pad) / cell_size)
            max_row = int((r['max_lat'] + pad) / cell_size) + 1
            min_col = int((r['min_lon'] - pad) / cell_size)
            max_col = int((r['max_lon'] + pad) / cell_size) + 1

            for row in range(min_row, max_row + 1):
                for col in range(min_col, max_col + 1):
                    self._grid[(row, col)].add(rid)

        # Precompute flat segment list per road for faster iteration
        # Each entry: (s1lat, s1lon, s2lat, s2lon)
        self._road_segments = {}
        for rid, r in self.roads.items():
            coords = r['coords']
            segs = []
            for i in range(len(coords) - 1):
                segs.append((coords[i][1], coords[i][0], coords[i+1][1], coords[i+1][0]))
            self._road_segments[rid] = segs

        total_entries = sum(len(v) for v in self._grid.values())
        avg_per_cell = total_entries / max(len(self._grid), 1)
        print(f"    Spatial grid: {len(self._grid):,} cells, "
              f"avg {avg_per_cell:.1f} roads/cell (cell={cell_size}°)")

    def snap_addresses(self, addresses):
        """
        Snap each address to its nearest road segment.
        Uses spatial grid index for O(n * k) performance instead of O(n * m).
        Adds 'road_id', 'road_name', 'road_group' to each address dict.
        """
        # Build groups first
        groups, group_map = self.build_road_groups()

        # Build spatial grid
        self._build_grid(addresses)
        cell_size = self._grid_res
        grid = self._grid

        # Snap tolerance in degrees (approximate, for bbox pre-filter)
        # 500 ft ≈ 0.0024° lat at 40°N
        bbox_pad = 0.003

        snapped = 0
        unsnapped = 0
        EARLY_EXIT_FT = 50  # close enough — stop searching
        road_segments = self._road_segments

        # Pre-fetch road data into arrays for hot loop
        road_min_lat = {rid: r['min_lat'] for rid, r in self.roads.items()}
        road_max_lat = {rid: r['max_lat'] for rid, r in self.roads.items()}
        road_min_lon = {rid: r['min_lon'] for rid, r in self.roads.items()}
        road_max_lon = {rid: r['max_lon'] for rid, r in self.roads.items()}
        road_clat = {rid: r['lat'] for rid, r in self.roads.items()}
        road_clon = {rid: r['lon'] for rid, r in self.roads.items()}

        # Inline haversine constants
        _R = 20902231
        _radians = math.radians
        _sin = math.sin
        _cos = math.cos
        _asin = math.asin
        _sqrt = math.sqrt

        for a in addresses:
            alat, alon = a['lat'], a['lon']
            best_road = None
            best_dist = float('inf')

            # Look up grid cell for this address + immediate neighbors
            row = int(alat / cell_size)
            col = int(alon / cell_size)

            candidate_rids = set()
            for dr in range(-1, 2):
                for dc in range(-1, 2):
                    candidate_rids.update(grid.get((row + dr, col + dc), set()))

            # Sort candidates by centroid distance (cheap — helps early exit)
            sorted_rids = sorted(candidate_rids,
                                  key=lambda rid: (alat - road_clat[rid])**2 +
                                                   (alon - road_clon[rid])**2)

            for rid in sorted_rids:
                # Quick bbox check with precomputed bounds
                if alat < road_min_lat[rid] - bbox_pad or alat > road_max_lat[rid] + bbox_pad:
                    continue
                if alon < road_min_lon[rid] - bbox_pad or alon > road_max_lon[rid] + bbox_pad:
                    continue

                # Check each segment (using precomputed flat list)
                for s1lat, s1lon, s2lat, s2lon in road_segments[rid]:
                    # Inline point_to_segment_dist for speed
                    dx = s2lon - s1lon
                    dy = s2lat - s1lat
                    if dx == 0 and dy == 0:
                        # Degenerate segment — distance to point
                        dlat = _radians(alat - s1lat)
                        dlon = _radians(alon - s1lon)
                        aa = _sin(dlat/2)**2 + _cos(_radians(alat))*_cos(_radians(s1lat))*_sin(dlon/2)**2
                        d = _R * 2 * _asin(_sqrt(aa))
                    else:
                        t = ((alon - s1lon)*dx + (alat - s1lat)*dy) / (dx*dx + dy*dy)
                        if t < 0: t = 0
                        elif t > 1: t = 1
                        plat = s1lat + t * dy
                        plon = s1lon + t * dx
                        dlat = _radians(alat - plat)
                        dlon = _radians(alon - plon)
                        aa = _sin(dlat/2)**2 + _cos(_radians(alat))*_cos(_radians(plat))*_sin(dlon/2)**2
                        d = _R * 2 * _asin(_sqrt(aa))

                    if d < best_dist:
                        best_dist = d
                        best_road = rid
                        if d <= EARLY_EXIT_FT:
                            break
                if best_dist <= EARLY_EXIT_FT:
                    break

            if best_road is not None and best_dist <= SNAP_TOLERANCE_FT:
                r = self.roads[best_road]
                a['road_id'] = best_road
                a['road_name'] = r['name']
                a['road_group'] = r.get('group_id', best_road)
                a['snap_dist_ft'] = round(best_dist)
                snapped += 1
            else:
                a['road_id'] = -1
                a['road_name'] = ''
                a['road_group'] = -1
                a['snap_dist_ft'] = round(best_dist) if best_dist < float('inf') else -1
                unsnapped += 1

        print(f"    Snapped: {snapped:,}, unsnapped: {unsnapped:,}")
        if unsnapped > 0:
            print(f"    (unsnapped addresses are >500ft from any road)")

        return groups

    def load_copper_cable(self, csv_path):
        """Load copper cable segments and build road-group adjacency from them.

        Each cable segment endpoint is snapped to the nearest road group.
        Two road groups connected by copper cable become copper-adjacent.

        Stores self.copper_adj: {(gid_a, gid_b): total_pairs}
        """
        import csv as csv_mod
        self.copper_adj = {}
        self.copper_segments = []

        try:
            with open(csv_path) as f:
                reader = csv_mod.DictReader(f)
                for row in reader:
                    flat = float(row['FROM_LATITUDE'])
                    flon = float(row['FROM_LONGITUDE'])
                    tlat = float(row['TO_LATITUDE'])
                    tlon = float(row['TO_LONGITUDE'])
                    qty = int(float(row.get('QUANTITY', 0) or 0))
                    self.copper_segments.append((flat, flon, tlat, tlon, qty))
        except Exception as e:
            print(f"    Copper cable: failed to load {csv_path}: {e}")
            return

        if not self.copper_segments:
            print(f"    Copper cable: 0 segments")
            return

        # Snap each endpoint to nearest road by centroid distance (fast)
        def snap_to_group(lat, lon):
            best_gid = -1
            best_d = float('inf')
            for rid, r in self.roads.items():
                d = (lat - r['lat'])**2 + (lon - r['lon'])**2
                if d < best_d:
                    best_d = d
                    best_gid = r.get('group_id', -1)
            return best_gid

        # Build a simple spatial lookup for road groups by centroid
        # (reuse road centroids, much faster than per-segment snapping)
        from collections import defaultdict as _dd
        grid_res = 0.005
        group_grid = _dd(list)
        for rid, r in self.roads.items():
            row = int(r['lat'] / grid_res)
            col = int(r['lon'] / grid_res)
            group_grid[(row, col)].append((rid, r['lat'], r['lon'],
                                           r.get('group_id', -1)))

        def snap_to_group_fast(lat, lon):
            row = int(lat / grid_res)
            col = int(lon / grid_res)
            best_gid = -1
            best_d = float('inf')
            for dr in range(-2, 3):
                for dc in range(-2, 3):
                    for rid, rlat, rlon, gid in group_grid.get((row+dr, col+dc), []):
                        d = (lat - rlat)**2 + (lon - rlon)**2
                        if d < best_d:
                            best_d = d
                            best_gid = gid
            return best_gid

        adj = {}
        connections = 0
        for flat, flon, tlat, tlon, qty in self.copper_segments:
            g1 = snap_to_group_fast(flat, flon)
            g2 = snap_to_group_fast(tlat, tlon)
            if g1 < 0 or g2 < 0 or g1 == g2:
                continue
            key = (min(g1, g2), max(g1, g2))
            adj[key] = adj.get(key, 0) + max(qty, 1)
            connections += 1

        self.copper_adj = adj
        n_pairs = len(adj)
        print(f"    Copper cable: {len(self.copper_segments)} segments, "
              f"{connections} cross-group, {n_pairs} group pairs")

    def crosses_water(self, lat1, lon1, lat2, lon2):
        """Check if straight line between two points crosses a waterway."""
        p1 = (lon1, lat1)
        p2 = (lon2, lat2)
        for seg in self.water_segments:
            d1 = (seg[0][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[0][1]-p1[1])
            d2 = (seg[1][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[1][1]-p1[1])
            d3 = (p1[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p1[1]-seg[0][1])
            d4 = (p2[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p2[1]-seg[0][1])
            if ((d1 > 0 and d2 < 0) or (d1 < 0 and d2 > 0)) and \
               ((d3 > 0 and d4 < 0) or (d3 < 0 and d4 > 0)):
                return True
        return False

    # ── Rail barrier infrastructure ──────────────────────────────────────

    def load_rail_barriers(self, rails_shp_path, bbox=None):
        """Load railroad barriers from TIGER shapefile (pure Python, no pyshp).

        Args:
            rails_shp_path: path to tl_2024_us_rails.shp
            bbox: (min_lat, min_lon, max_lat, max_lon) to clip
        """
        import struct
        self.rail_segments = []
        self._rail_grid = defaultdict(list)

        # MTFCC filter: R1051=mainline, R1052=branch (skip R1011 connectors)
        rail_mtfcc = {b'R1051', b'R1052'}

        dbf_path = rails_shp_path.replace('.shp', '.dbf')
        try:
            # Read DBF to get MTFCC per record
            mtfcc_list = []
            with open(dbf_path, 'rb') as f:
                f.seek(4)
                num_records = struct.unpack('<I', f.read(4))[0]
                header_size = struct.unpack('<H', f.read(2))[0]
                record_size = struct.unpack('<H', f.read(2))[0]
                # Parse field descriptors
                f.seek(32)
                fields = []
                while True:
                    b = f.read(1)
                    if b == b'\r':
                        break
                    name = (b + f.read(10)).rstrip(b'\x00').decode('ascii')
                    ftype = f.read(1).decode('ascii')
                    f.read(4)
                    fsize = struct.unpack('B', f.read(1))[0]
                    f.read(15)
                    fields.append((name, fsize))
                # Find MTFCC field offset and size
                mtfcc_offset = 1  # skip deletion flag byte
                mtfcc_size = 5
                found = False
                for fname, fsize in fields:
                    if fname == 'MTFCC':
                        mtfcc_size = fsize
                        found = True
                        break
                    mtfcc_offset += fsize
                if not found:
                    print(f"    Rail barriers: no MTFCC field in DBF")
                    return
                # Read MTFCC for each record
                f.seek(header_size)
                for _ in range(num_records):
                    rec = f.read(record_size)
                    mtfcc_val = rec[mtfcc_offset:mtfcc_offset + mtfcc_size].strip()
                    mtfcc_list.append(mtfcc_val)

            # Read SHP polylines
            seg_count = 0
            with open(rails_shp_path, 'rb') as f:
                # Skip 100-byte header
                f.seek(100)
                rec_idx = 0
                while True:
                    hdr = f.read(8)
                    if len(hdr) < 8:
                        break
                    rec_num, content_len = struct.unpack('>II', hdr)
                    content = f.read(content_len * 2)
                    if len(content) < content_len * 2:
                        break

                    # Check MTFCC filter
                    if rec_idx < len(mtfcc_list) and mtfcc_list[rec_idx] not in rail_mtfcc:
                        rec_idx += 1
                        continue
                    rec_idx += 1

                    # Parse polyline shape (type 3)
                    shape_type = struct.unpack('<I', content[0:4])[0]
                    if shape_type != 3:  # not polyline
                        continue

                    # Bounding box: xmin, ymin, xmax, ymax (doubles)
                    xmin, ymin, xmax, ymax = struct.unpack('<4d', content[4:36])
                    if bbox:
                        if ymax < bbox[0] or ymin > bbox[2]:
                            continue
                        if xmax < bbox[1] or xmin > bbox[3]:
                            continue

                    num_parts = struct.unpack('<I', content[36:40])[0]
                    num_points = struct.unpack('<I', content[40:44])[0]
                    parts = []
                    for p in range(num_parts):
                        parts.append(struct.unpack('<I', content[44 + p*4:48 + p*4])[0])

                    pts_offset = 44 + num_parts * 4
                    points = []
                    for pt in range(num_points):
                        x, y = struct.unpack('<2d', content[pts_offset + pt*16:pts_offset + pt*16 + 16])
                        points.append((x, y))  # (lon, lat)

                    # Extract line segments
                    for i in range(len(points) - 1):
                        seg = (points[i], points[i+1])
                        self.rail_segments.append(seg)
                        seg_count += 1

        except Exception as e:
            print(f"    Rail barriers: failed to load {rails_shp_path}: {e}")
            return

        # Build spatial grid (same resolution as highway grid)
        grid_res = 0.005
        for seg in self.rail_segments:
            min_lon = min(seg[0][0], seg[1][0])
            max_lon = max(seg[0][0], seg[1][0])
            min_lat = min(seg[0][1], seg[1][1])
            max_lat = max(seg[0][1], seg[1][1])
            for row in range(int(min_lat / grid_res) - 1, int(max_lat / grid_res) + 2):
                for col in range(int(min_lon / grid_res) - 1, int(max_lon / grid_res) + 2):
                    self._rail_grid[(row, col)].append(seg)
        self._rail_grid_res = grid_res

        print(f"    Rail barriers: {seg_count} segments")

    def crosses_rail(self, lat1, lon1, lat2, lon2):
        """Check if straight line between two points crosses a railroad."""
        if not hasattr(self, 'rail_segments') or not self.rail_segments:
            return False

        p1 = (lon1, lat1)
        p2 = (lon2, lat2)
        gr = self._rail_grid_res
        min_lon = min(lon1, lon2)
        max_lon = max(lon1, lon2)
        min_lat = min(lat1, lat2)
        max_lat = max(lat1, lat2)

        checked = set()
        for row in range(int(min_lat / gr) - 1, int(max_lat / gr) + 2):
            for col in range(int(min_lon / gr) - 1, int(max_lon / gr) + 2):
                for seg in self._rail_grid.get((row, col), []):
                    seg_id = id(seg)
                    if seg_id in checked:
                        continue
                    checked.add(seg_id)

                    d1 = (seg[0][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[0][1]-p1[1])
                    d2 = (seg[1][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[1][1]-p1[1])
                    d3 = (p1[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p1[1]-seg[0][1])
                    d4 = (p2[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p2[1]-seg[0][1])
                    if ((d1 > 0 and d2 < 0) or (d1 < 0 and d2 > 0)) and \
                       ((d3 > 0 and d4 < 0) or (d3 < 0 and d4 > 0)):
                        return True
        return False

    # ── Highway barrier infrastructure ────────────────────────────────────

    def classify_barriers(self, morphology='SUBURBAN'):
        """Identify highway barrier road segments and mark barrier edges.

        Args:
            morphology: 'RURAL', 'URBAN', or 'SUBURBAN' — controls which
                        MTFCC codes count as barriers.
        """
        barrier_set = HIGHWAY_BARRIER_MTFCC_RURAL if morphology == 'RURAL' else HIGHWAY_BARRIER_MTFCC
        self.highway_segments = []
        self.edge_is_barrier = {}
        self.barrier_road_ids = set()

        for rid, r in self.roads.items():
            if r['mtfcc'] in barrier_set:
                self.barrier_road_ids.add(rid)
                coords = r['coords']
                for i in range(len(coords) - 1):
                    self.highway_segments.append(
                        ((coords[i][0], coords[i][1]),
                         (coords[i+1][0], coords[i+1][1]))
                    )
                sn, en = r['start_node'], r['end_node']
                self.edge_is_barrier[(sn, en)] = True
                self.edge_is_barrier[(en, sn)] = True

        # Build spatial grid for fast barrier crossing checks
        self._barrier_grid = defaultdict(list)
        grid_res = 0.005  # ~1600 ft cells
        for seg in self.highway_segments:
            min_lon = min(seg[0][0], seg[1][0])
            max_lon = max(seg[0][0], seg[1][0])
            min_lat = min(seg[0][1], seg[1][1])
            max_lat = max(seg[0][1], seg[1][1])
            for row in range(int(min_lat / grid_res) - 1, int(max_lat / grid_res) + 2):
                for col in range(int(min_lon / grid_res) - 1, int(max_lon / grid_res) + 2):
                    self._barrier_grid[(row, col)].append(seg)
        self._barrier_grid_res = grid_res

        print(f"    Highway barriers: {len(self.barrier_road_ids)} roads, "
              f"{len(self.highway_segments)} segments ({morphology} mode)")

    def compute_barrier_components(self):
        """Compute connected components where edges that CROSS highway
        barriers are removed.

        Key distinction from edge_is_barrier:
          - edge_is_barrier: edge IS a highway segment (too aggressive —
            fragments rural areas where highways are the backbone)
          - edge_crosses_barrier: the line between two nodes crosses a
            highway line geometrically (correct — separates sides of highway)

        Traveling ALONG a highway is fine; crossing from one side to
        the other is not.  Dead-end roads that branch off a highway stay
        connected to their side.
        """
        from collections import deque, Counter

        if not hasattr(self, 'highway_segments') or not self.highway_segments:
            self.node_component = {}
            self.edge_crosses_barrier = {}
            return

        # Pre-compute which edges cross a highway barrier (once)
        self.edge_crosses_barrier = {}
        crossing_count = 0
        for node, neighbors in self.edges.items():
            lat1, lon1 = self.node_pos[node]
            for neighbor in neighbors:
                key = (node, neighbor)
                if key in self.edge_crosses_barrier:
                    continue
                lat2, lon2 = self.node_pos[neighbor]
                crosses = self.crosses_highway(lat1, lon1, lat2, lon2)
                self.edge_crosses_barrier[key] = crosses
                self.edge_crosses_barrier[(neighbor, node)] = crosses
                if crosses:
                    crossing_count += 1

        print(f"    Edges that cross highways: {crossing_count}")

        # BFS connected components — skip edges that CROSS barriers
        all_nodes = set(self.node_pos.keys())
        visited = set()
        self.node_component = {}
        comp_id = 0

        for start_node in all_nodes:
            if start_node in visited:
                continue
            queue = deque([start_node])
            visited.add(start_node)
            self.node_component[start_node] = comp_id
            while queue:
                node = queue.popleft()
                for neighbor in self.edges.get(node, set()):
                    if neighbor in visited:
                        continue
                    if self.edge_crosses_barrier.get((node, neighbor), False):
                        continue  # crosses highway — do not traverse
                    visited.add(neighbor)
                    self.node_component[neighbor] = comp_id
                    queue.append(neighbor)
            comp_id += 1

        comp_sizes = Counter(self.node_component.values())
        largest = comp_sizes.most_common(1)[0][1] if comp_sizes else 0
        singletons = sum(1 for s in comp_sizes.values() if s == 1)
        print(f"    Barrier components: {comp_id} components "
              f"(largest: {largest} nodes, singletons: {singletons})")

    def crosses_highway(self, lat1, lon1, lat2, lon2):
        """Check if straight line between two points crosses a highway barrier.
        Uses spatial grid for fast lookup."""
        if not hasattr(self, 'highway_segments') or not self.highway_segments:
            return False

        p1 = (lon1, lat1)
        p2 = (lon2, lat2)

        # Determine which grid cells the query line passes through
        gr = self._barrier_grid_res
        min_lon = min(lon1, lon2)
        max_lon = max(lon1, lon2)
        min_lat = min(lat1, lat2)
        max_lat = max(lat1, lat2)

        checked = set()
        for row in range(int(min_lat / gr) - 1, int(max_lat / gr) + 2):
            for col in range(int(min_lon / gr) - 1, int(max_lon / gr) + 2):
                for seg in self._barrier_grid.get((row, col), []):
                    seg_id = id(seg)
                    if seg_id in checked:
                        continue
                    checked.add(seg_id)

                    d1 = (seg[0][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[0][1]-p1[1])
                    d2 = (seg[1][0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(seg[1][1]-p1[1])
                    d3 = (p1[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p1[1]-seg[0][1])
                    d4 = (p2[0]-seg[0][0])*(seg[1][1]-seg[0][1]) - (seg[1][0]-seg[0][0])*(p2[1]-seg[0][1])
                    if ((d1 > 0 and d2 < 0) or (d1 < 0 and d2 > 0)) and \
                       ((d3 > 0 and d4 < 0) or (d3 < 0 and d4 > 0)):
                        return True
        return False

    # ── Edge weights + Dijkstra shortest path ─────────────────────────────

    def compute_edge_weights(self):
        """Pre-compute haversine edge weights (ft) for Dijkstra."""
        self.edge_weight = {}  # (start_node, end_node) -> distance_ft
        for rid, r in self.roads.items():
            length = 0.0
            coords = r['coords']
            for i in range(len(coords) - 1):
                length += haversine_ft(coords[i][1], coords[i][0],
                                       coords[i+1][1], coords[i+1][0])
            r['length_ft'] = length
            sn, en = r['start_node'], r['end_node']
            key = (sn, en)
            # Keep shortest if multiple roads connect same node pair
            if key not in self.edge_weight or length < self.edge_weight[key]:
                self.edge_weight[key] = length
                self.edge_weight[(en, sn)] = length

    def snap_point_to_node(self, lat, lon):
        """Find nearest graph node to a lat/lon point.
        Uses spatial grid for fast lookup. Returns (node_id, snap_dist_ft)."""
        if not hasattr(self, '_node_grid') or self._node_grid is None:
            # Build a spatial grid index over nodes (once)
            self._node_grid = defaultdict(list)
            self._node_grid_res = 0.005  # ~1600 ft cells
            for nid, (nlat, nlon) in self.node_pos.items():
                row = int(nlat / self._node_grid_res)
                col = int(nlon / self._node_grid_res)
                self._node_grid[(row, col)].append(nid)

        gr = self._node_grid_res
        row = int(lat / gr)
        col = int(lon / gr)

        best_nid = None
        best_dist = float('inf')

        # Search expanding rings (1 ring usually enough)
        for ring_r in range(4):
            for dr in range(-ring_r, ring_r + 1):
                for dc in range(-ring_r, ring_r + 1):
                    if abs(dr) != ring_r and abs(dc) != ring_r and ring_r > 0:
                        continue  # only check perimeter of this ring
                    for nid in self._node_grid.get((row + dr, col + dc), []):
                        nlat, nlon = self.node_pos[nid]
                        d = haversine_ft(lat, lon, nlat, nlon)
                        if d < best_dist:
                            best_dist = d
                            best_nid = nid
            if best_nid is not None and ring_r >= 1:
                break  # found nodes in inner ring, no need to expand further

        return best_nid, best_dist

    def shortest_path_from(self, start_node, max_dist_ft=None, respect_barriers=True):
        """Dijkstra shortest path from start_node to all reachable nodes.

        Args:
            start_node: node_id to start from
            max_dist_ft: stop exploring beyond this distance (optimization)
            respect_barriers: if True, highway barrier edges are impassable

        Returns:
            dict of node_id -> distance_ft from start_node
        """
        import heapq

        if not hasattr(self, 'edge_weight'):
            self.compute_edge_weights()

        dist = {start_node: 0.0}
        heap = [(0.0, start_node)]
        visited = set()

        while heap:
            d, u = heapq.heappop(heap)
            if u in visited:
                continue
            visited.add(u)

            if max_dist_ft is not None and d > max_dist_ft:
                break

            for v in self.edges.get(u, set()):
                if respect_barriers and hasattr(self, 'edge_crosses_barrier') and \
                   self.edge_crosses_barrier.get((u, v), False):
                    continue
                w = self.edge_weight.get((u, v), float('inf'))
                nd = d + w
                if nd < dist.get(v, float('inf')):
                    dist[v] = nd
                    heapq.heappush(heap, (nd, v))

        return dist

    def network_distance(self, lat1, lon1, lat2, lon2, max_dist_ft=None):
        """Compute road-network distance between two points.
        Returns float('inf') if no path (e.g., separated by highway barrier)."""
        nid1, _ = self.snap_point_to_node(lat1, lon1)
        nid2, _ = self.snap_point_to_node(lat2, lon2)
        if nid1 is None or nid2 is None:
            return float('inf')
        dist_map = self.shortest_path_from(nid1, max_dist_ft=max_dist_ft)
        return dist_map.get(nid2, float('inf'))

    def get_barrier_geojson(self):
        """Export highway barrier segments as GeoJSON for map visualization."""
        if not hasattr(self, 'barrier_road_ids'):
            return {'type': 'FeatureCollection', 'features': []}
        features = []
        for rid in self.barrier_road_ids:
            r = self.roads[rid]
            features.append({
                'type': 'Feature',
                'geometry': {
                    'type': 'LineString',
                    'coordinates': r['coords'],
                },
                'properties': {
                    'mtfcc': r['mtfcc'],
                    'name': r['name'],
                },
            })
        return {'type': 'FeatureCollection', 'features': features}
