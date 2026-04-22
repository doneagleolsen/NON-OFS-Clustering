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
            name = feat["properties"].get("FULLNAME", "")
            mtfcc = feat["properties"].get("MTFCC", "")
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

        # Assign group_id to each road
        group_map = {}
        for gid, (root, members) in enumerate(sorted(groups.items())):
            for rid in members:
                group_map[rid] = gid

        for rid, r in self.roads.items():
            r['group_id'] = group_map.get(rid, rid)

        n_groups = len(groups)
        largest = max(len(v) for v in groups.values())
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
