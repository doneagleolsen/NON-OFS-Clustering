# NON-OFS ILEC Master Clustering Pipeline

**Version 6.0** | Updated 2026-04-21

Clusters ~12.3M NON-OFS ILEC base addresses into 50-80K build-priority clusters using road-graph-constrained, morphology-aware, financial-quality-driven formation. Obligations are tagged and scored **post-formation only** -- they never influence cluster boundaries.

---

## Pipeline Steps

| Step | Script | Python Env | Description |
|------|--------|-----------|-------------|
| 1 | `extract_all_nonofs_addresses.py` | system 3.11 | Query Oracle for 12.3M NON-OFS addresses; join V2 IRR ramp locally; write `all_nonofs_12m.csv` |
| 2 | (manual) sort by CLLI | system 3.11 | Sort CSV by CLLI column to produce `all_nonofs_12m_sorted.csv` for streaming |
| 3 | `tiger_pipeline.py` | system 3.11 | Download TIGER/Line 2024 county road shapefiles; convert to GeoJSON in `TIGER/` |
| 4 | `cluster_all_nonofs.py` | system 3.11 | Stream sorted CSV one WC at a time; build road graph; run V6 clustering; write `v6_clusters_cache.json` + `v6_cluster_summary.csv` |
| 5 | `tag_obligations_v2.py` | system 3.11 | Query Oracle for NSI/EWO/LFA bridge tables; tag each address with obligation bucket; write `addr_obligation_tags.csv`; update cluster cache |
| 6 | `eisenhower_scoring.py` | system 3.11 | Compute Urgency x Value scores per cluster; assign Eisenhower quadrant (Q1-Q4); write `v6_cluster_eisenhower.csv` |
| 7 | `generate_cluster_polygons.py` | system 3.11 | Build convex-hull polygons per cluster from address coordinates; write `all_nonofs_cluster_polygons.geojson` |
| 8 | `write_oracle_master_tables.py` | system 3.11 | Create/populate `GPSAA.CLUSTER_ASSIGNMENT_MASTER` (12.3M rows) + `GPSAA.CLUSTER_SUMMARY_MASTER` (50-80K rows) |
| 9 | `export_arcgis_gdb.py` | arcgispro-py3 | Convert polygons + address points to `NONOFS_Master.gdb` with WC boundary + fiber basemap layers |
| 10 | `monthly_refresh.py` | system 3.11 | Orchestrator: re-extract scores, re-tag obligations, re-score Eisenhower, regenerate polygons, write Oracle. Does NOT re-cluster. |

---

## V6 Formation Scoring

Cluster formation uses **financial and engineering quality only**. Obligations are deliberately excluded from formation to avoid gerrymandering cluster boundaries around regulatory deadlines.

### Formation Weights (per address, 0-100 composite)

| Weight | Component | Source | Logic |
|--------|-----------|--------|-------|
| **40%** | IRR V2 | `irr_all_base_addresses.csv` | 15yr DCF from V2 penetration ramp model. Higher IRR = higher score. |
| **25%** | Copper Salvage | `COPPER_CIR_COUNT * $200` | Proxy: $200/circuit salvage value. Addresses with more copper plant = more construction value recovered. |
| **20%** | BI Rank | `PRIORITY_RANK` from BI scoring | Inverted (lower rank = higher score). Captures dispatch/sales/geo synergy from Beyond Infinity model. |
| **15%** | Unit Density | `NO_OF_UNITS` | MDU/MTU bonus. Caps at 10 units (score=1.0). SFU = 1 unit (score=0.1). |

### Growth Scoring (how candidate road-groups are added to a growing hub)

| Weight | Component | Description |
|--------|-----------|-------------|
| 35% | Financial quality | Average formation score of the candidate road-group |
| 25% | Proximity | Distance from hub centroid (closer = better), normalized by `MAX_RADIUS` |
| 25% | IRR gravity | Max IRR score in the candidate group (high-IRR groups attract growth) |
| 15% | Street affinity | Bonus if candidate shares a road name with existing hub members |

### 6-Phase Clustering Architecture

1. **Formation scoring** -- score each address 0-100 (financial/engineering only)
2. **Road-group units** -- aggregate addresses snapped to the same road segment into road-group units; unsnapped addresses assigned to nearest group
3. **Group adjacency** -- build adjacency graph from shared road-graph nodes + proximity fallback (morphology-dependent distance)
4. **Seed-and-grow** -- sort road-groups by financial score descending; seed highest-scoring unassigned group; grow hub by adding best adjacent candidates until TARGET_UNITS reached or no candidates remain; radius-capped, water-barrier-aware
5. **Size enforcement** -- merge undersized hubs into nearest neighbor (up to MAX_UNITS, within 1.5x MAX_RADIUS); split oversized hubs by geographic bisection; second-pass merge for remaining runts
6. **Renumber + centroids** -- compact hub IDs; compute weighted centroids (bias toward high-score addresses); optional core-fiber centroid pull (15% toward nearest existing fiber)

---

## Morphology Parameters

Hub sizing and radius constraints vary by `MARKET_DENSITY` (from `GPSAA.WIRECENTER`). Derived from IVAPP analysis of 2,000+ real FiOS hubs.

| Morphology | Target | Min | Max | Radius (ft) | Prox Fallback (ft) | Example |
|------------|--------|-----|-----|-------------|--------------------:|---------|
| ULTRADENSE-URBAN | 91 | 48 | 180 | 700 | 500 | Manhattan, downtown Brooklyn |
| DENSE-URBAN | 176 | 92 | 261 | 1,100 | 800 | Philly core, Malden, Boston |
| URBAN | 177 | 96 | 278 | 1,600 | 1,200 | Trenton, Harrisburg |
| URBAN-SUBURBAN | 190 | 96 | 300 | 2,000 | 1,500 | Transition zones |
| SUBURBAN | 204 | 105 | 330 | 2,600 | 2,000 | Dulles VA, Toms River NJ |
| SUBURBAN-RURAL | 180 | 90 | 300 | 4,000 | 3,000 | Transition zones |
| RURAL-SUBURBAN | 160 | 80 | 280 | 6,000 | 4,000 | Transition zones |
| DENSE-RURAL | 140 | 70 | 250 | 7,500 | 5,000 | Dense rural areas |
| RURAL | 133 | 66 | 228 | 10,000 | 6,000 | Coventry RI, Millville NJ |
| ULTRA-RURAL | 113 | 67 | 241 | 21,300 | 10,000 | Peru NY, Gouverneur NY |
| *(default)* | 170 | 85 | 300 | 3,000 | 2,000 | Unknown morphology fallback |

---

## Obligation Tagging (Post-Formation)

`tag_obligations_v2.py` runs **after** clustering. It tags each of the 12.3M addresses with exactly one obligation bucket, then aggregates to cluster level.

### Obligation Buckets (priority order -- first match wins)

| Priority | Bucket | Trigger |
|----------|--------|---------|
| 1 | `COP_2026_OBLIG` | Copper recycling, start date <= 2026 |
| 2 | `COP_2027_OBLIG` | Copper recycling, start date = 2027 |
| 3 | `COP_FUTURE_OBLIG` | Copper recycling, start date > 2027 or unspecified |
| 4 | `SBB_OBLIG` | State broadband obligation (WC-level or address-level SBB flag) |
| 5 | `NSI_OBLIG` | National Security Interest (via `NSEEPRD.NSI_INQUIRIES` bridged through `GPSAA.NTAS_LOCUS_MAP`) |
| 6 | `LFA_OBLIG` | Local Franchise Authority (via `COUNTY_TYPE = 'LFA'` in `G_ALL_LOCUS_ADDRESS_KEY`) |
| 7 | `DISCRETIONARY` | No obligation |

### Cluster-Level Aggregation

Each cluster gets:
- `obligation_fill` -- dict of bucket counts (e.g., `{COP_2026_OBLIG: 42, DISCRETIONARY: 108}`)
- `top_obligation` -- highest-priority bucket present in the cluster
- `obligation_fraction` -- fraction of addresses that are non-discretionary (0.0 to 1.0)

---

## Eisenhower Scoring Matrix

`eisenhower_scoring.py` assigns each cluster a 2D score: **Urgency** (how soon must it be built?) and **Value** (how valuable is it to build?). The intersection determines the Eisenhower quadrant.

### Urgency Score (0-100)

| Weight | Component | Description |
|--------|-----------|-------------|
| **50%** | Obligation tier | Tier score (0-100) of the cluster's top obligation. `COP_2026_OBLIG`=100, `DISCRETIONARY`=0. Blended: 70% tier score + 30% obligation density (fraction of non-discretionary addresses). |
| **30%** | Copper recycling imminence | Weighted sum of copper circuits by timeline: 2026=1.0x, 2027=0.7x, 2028-2029=0.3x. Blended: 60% imminence ratio + 40% circuit density. |
| **20%** | Dispatch frequency | Average 1-year dispatch count per address. Normalized to 5+ dispatches/addr/yr = max. |

### Value Score (0-100)

| Weight | Component | Description |
|--------|-----------|-------------|
| **50%** | IRR V2 | Median address-level IRR. Mapped: 0%->0, 15%->50, 30%+->100. |
| **25%** | Penetration terminal | Average terminal penetration rate. Normalized to 60%+ = max. |
| **25%** | Revenue potential | EBITDA per unit. Mapped: $0->0, $500->50, $1,000+->100. |

### Eisenhower Quadrants

| Quadrant | Urgency | Value | Action |
|----------|---------|-------|--------|
| **Q1 -- Do First** | >= 50 | >= 50 | High urgency + high value. Build immediately. |
| **Q2 -- Schedule** | < 50 | >= 50 | High value but not urgent. Schedule for future build years. |
| **Q3 -- Must Do** | >= 50 | < 50 | Urgent (obligation/copper) but lower financial return. Must build, manage costs. |
| **Q4 -- Deprioritize** | < 50 | < 50 | Neither urgent nor high-value. Lowest build priority. |

Obligation tier scores used in urgency computation (from `clustering_config.json`):

| Bucket | Score |
|--------|-------|
| COP_2026_OBLIG | 100 |
| COP_2026_PT | 95 |
| COP_2027_OBLIG | 85 |
| COP_2027_PT | 80 |
| SBB_OBLIG | 75 |
| SBB_PT | 70 |
| NSI_OBLIG | 60 |
| LFA_OBLIG | 55 |
| WB_SC / ALEXANDRIA | 50 |
| DISCRETIONARY | 0 |

---

## Data Sources

### Oracle Tables

| Table | Schema | Role |
|-------|--------|------|
| `S_BEYOND_INFINITY_RANKING_SCORING` | GPSAA | Primary: BI scoring, CPO rates, priority rank, copper counts, dispatch, market density |
| `G_ALL_LOCUS_ADDRESS_KEY` | GPSAA | Lat/lon, category (NON_OFS filter), ODN flag, SBB flag, county type |
| `WIRECENTER` | GPSAA | Region, sub-region, NT type, planned copper recycling, SBB flag |
| `NSI_INQUIRIES` | NSEEPRD | NSI address IDs (bridged via NTAS_LOCUS_MAP) |
| `NTAS_LOCUS_MAP` | GPSAA | Bridge: ADDRESS_ID -> LOCUS_ADDRESS_ID for NSI/EWO joins |
| `EWO_W_CMTDATE_NODUP_PIPELINE_2026` | GPSAA | EWO pipeline addresses with commitment dates |

### Local Files (pre-computed)

| File | Description |
|------|-------------|
| `irr_all_base_addresses.csv` | V2 penetration ramp IRR: 12.3M addresses, 15yr DCF, logistic S-curves by morphology x NT type |
| `clli_county_fips_all.csv` | CLLI -> county FIPS mapping (primary + all overlapping counties) |
| `TIGER/roads_{FIPS}.geojson` | TIGER/Line 2024 road networks per county (~1 GB total, 178+ counties cached) |

### Oracle Connection

```
User:     tableau_user
Password: Verizon1#
DSN:      f1btpap-scan.verizon.com:1521/NARPROD
```

---

## Output Files

### Intermediate

| File | Size (approx) | Description |
|------|---------------|-------------|
| `all_nonofs_12m.csv` | ~4 GB | Raw extraction: 12.3M addresses with all Oracle + IRR fields |
| `all_nonofs_12m_sorted.csv` | ~4 GB | Same data sorted by CLLI (required for streaming) |
| `v6_checkpoint.json` | <1 KB | Resume checkpoint: completed CLLIs + cluster count |

### Primary Outputs

| File | Size (approx) | Description |
|------|---------------|-------------|
| `v6_clusters_cache.json` | ~200 MB | Master cluster cache: all clusters with addresses, scores, obligations, Eisenhower |
| `v6_cluster_summary.csv` | ~10 MB | Cluster-level summary (no address lists) |
| `v6_cluster_eisenhower.csv` | ~12 MB | Cluster summary with urgency/value/quadrant columns |
| `addr_obligation_tags.csv` | ~300 MB | Address-level obligation bucket tags (12.3M rows) |
| `all_nonofs_cluster_polygons.geojson` | ~100 MB | Convex-hull polygons with full metadata properties |

### Oracle Tables

| Table | Rows | Description |
|-------|------|-------------|
| `GPSAA.CLUSTER_ASSIGNMENT_MASTER` | ~12.3M | Address-level: LAID, cluster ID, obligation, scores, IRR, copper, etc. Partitioned by RUN_DATE. |
| `GPSAA.CLUSTER_SUMMARY_MASTER` | ~50-80K | Cluster-level: centroid, units, CAPEX, Eisenhower scores, obligation fill. Partitioned by RUN_DATE. |

### ArcGIS GDB

| Feature Class | Type | Description |
|---------------|------|-------------|
| `Cluster_Polygons` | Polygon | Convex hull polygons with all scoring metadata |
| `Address_Points` | Point | 12.3M address points with cluster ID, obligation, priority |
| `WC_Boundaries` | Polygon | Verizon ILEC wirecenter boundaries |
| `Fiber_FTTP` | Polyline | Existing FTTP fiber cable routes |

GDB path: `C:\Users\v267429\Downloads\AI_Sessions\NONOFS_Master.gdb`

---

## How to Run

### Prerequisites

- Python 3.11 (system): `C:/Users/v267429/AppData/Local/Programs/Python/Python311/python.exe`
- Python arcgispro-py3: `C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe` (GDB export only)
- Packages: `oracledb` (system python), `arcpy` (arcgispro-py3 only)
- Oracle network access to `f1btpap-scan.verizon.com:1521/NARPROD`
- TIGER roads cached in `TIGER/` directory (~1 GB)
- `irr_all_base_addresses.csv` pre-computed (see `irr_v2_ramp_computation.md`)

### Full Run (first time or re-cluster)

```bash
cd C:\Users\v267429\Downloads\AI_Sessions

# 1. Extract addresses from Oracle (~30 min, 12.3M rows)
python extract_all_nonofs_addresses.py

# 2. Sort by CLLI (required for streaming)
python -c "
import csv
rows = list(csv.DictReader(open('all_nonofs_12m.csv', encoding='utf-8')))
rows.sort(key=lambda r: r['CLLI'])
with open('all_nonofs_12m_sorted.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
"

# 3. Cluster all WCs (~2-4 hours, streams one WC at a time)
python cluster_all_nonofs.py

# 4. Tag obligations (~10 min)
python tag_obligations_v2.py

# 5. Eisenhower scoring (~5 min)
python eisenhower_scoring.py

# 6. Generate polygons (~10 min)
python generate_cluster_polygons.py

# 7. Write to Oracle (~30 min)
python write_oracle_master_tables.py

# 8. Export ArcGIS GDB (arcgispro-py3 only, ~45 min)
"C:/Program Files/ArcGIS/Pro/bin/Python/envs/arcgispro-py3/python.exe" export_arcgis_gdb.py
```

### Single WC Test

```bash
python cluster_all_nonofs.py --clli KGTNPAES
```

### Resume After Interruption

```bash
python cluster_all_nonofs.py --resume
```

Reads `v6_checkpoint.json` to skip already-completed WCs and appends to existing `v6_clusters_cache.json`.

### Monthly Refresh (scores only, boundaries stable)

```bash
# Full refresh with Oracle write
python monthly_refresh.py

# Dry run (score locally, skip Oracle write)
python monthly_refresh.py --dry-run
```

The monthly refresh pipeline:
1. Re-extracts address data (updated BI scores, copper flags, dispatch counts)
2. Re-tags obligations (updated NSI/EWO/LFA/SBB flags)
3. Re-scores Eisenhower matrix
4. Regenerates polygon metadata (geometry unchanged)
5. Writes Oracle tables with new `RUN_DATE` partition

Estimated runtime: ~15 minutes for the dry run, ~45 minutes including Oracle write.

### Oracle-Only Update

```bash
# Write both tables
python write_oracle_master_tables.py

# Summary table only (skip 12.3M address rows)
python write_oracle_master_tables.py --summary-only

# Drop and recreate tables first
python write_oracle_master_tables.py --drop
```

---

## Key Design Decisions

### Streaming CSV (Memory)
The 12.3M address CSV is ~4 GB. Loading it all into memory would require 16+ GB RAM. Instead, `cluster_all_nonofs.py` streams the CSV one WC at a time (the file is pre-sorted by CLLI). Peak memory is limited to the largest single WC (~50K addresses). After each WC, the address list is explicitly deleted and `gc.collect()` runs every 50 WCs.

### Sorted by CLLI
Streaming requires the CSV to be sorted by CLLI so all addresses for a given wirecenter appear contiguously. The `stream_wcs()` generator yields `(clli, meta, addrs_list)` tuples, switching to a new WC whenever the CLLI changes. This avoids needing to load the full file or build a CLLI index.

### Checkpoint/Resume
Every 50 WCs, the orchestrator writes `v6_checkpoint.json` (list of completed CLLIs) and dumps the full `v6_clusters_cache.json`. On `--resume`, it loads the checkpoint, skips completed WCs, and appends new clusters. This is critical because a full run takes 2-4 hours and network/DB interruptions are common on the corporate VPN.

### Copper Salvage Proxy ($200/circuit)
True copper salvage value per address is not available in any Oracle table. The pipeline uses a flat $200/circuit proxy (`COPPER_CIR_COUNT * 200`). This figure comes from internal Verizon copper recovery program estimates. It intentionally overweights addresses with many active copper circuits to prioritize construction that enables plant retirement.

### Obligations Excluded from Formation
V5 used obligations as a formation input (35% weight). V6 removes them entirely from formation scoring. The rationale: obligation deadlines are external regulatory constraints that change quarterly, while cluster boundaries should reflect physical network topology and financial quality. Obligations are now a post-formation tag and feed the Eisenhower urgency axis only. This means a cluster boundary will not shift just because an obligation deadline changed.

### Passthrough Clustering
WCs with fewer than 3 addresses or no TIGER roads available get a single passthrough cluster (`{CLLI}_H000`) containing all addresses. This avoids failing the pipeline for small or unmapped WCs. The passthrough cluster still gets obligation tags and Eisenhower scores.

### Multi-County Road Merge
Some wirecenters span multiple counties. `cluster_all_nonofs.py` loads all county road GeoJSON files listed in `clli_county_fips_all.csv`, merges them into a temporary FeatureCollection, builds a single RoadGraph, then deletes the temporary file. This is critical for WCs like GNWCCTGN that straddle county lines.

---

## TIGER Roads

TIGER/Line 2024 road shapefiles are downloaded per county FIPS, converted to GeoJSON via a pure-Python shapefile reader (no `ogr2ogr` dependency), and cached in `TIGER/`.

### Current Status
- **~178 of 309 counties cached** (~499 files including dirs/zips)
- **~1 GB** total disk usage
- Corporate proxy (407) blocks new downloads -- must use a non-proxied connection or pre-download

### Connecticut FIPS Remap
Connecticut replaced its 8 counties with 9 planning regions in 2022. TIGER 2024 uses new FIPS codes (09110-09180). The pipeline maps old codes (09001-09015) to new planning region codes:

| Old FIPS | Old County | New FIPS |
|----------|-----------|----------|
| 09001 | Fairfield | 09110, 09120 |
| 09003 | Hartford | 09150, 09170 |
| 09005 | Litchfield | 09120, 09160 |
| 09007 | Middlesex | 09150, 09170 |
| 09009 | New Haven | 09120, 09170 |
| 09011 | New London | 09140, 09180 |
| 09013 | Tolland | 09140, 09180 |
| 09015 | Windham | 09140, 09180 |

### If Roads Are Missing for a WC
The WC falls back to passthrough clustering (single cluster, all addresses). To add missing roads:
```bash
python tiger_pipeline.py --fips 42079        # By county FIPS
python tiger_pipeline.py --clli KGTNPAES     # By CLLI (looks up FIPS)
python tiger_pipeline.py --status            # Show download coverage
```

---

## Configuration

All scoring weights, obligation tier scores, and thresholds are in `clustering_config.json`:

```json
{
  "formation_weights": {"W_IRR_V2": 0.40, "W_COPPER_SALVAGE": 0.25, "W_BI_RANK": 0.20, "W_DENSITY": 0.15},
  "eisenhower_urgency_weights": {"obligation_tier": 0.50, "copper_recycling_imminence": 0.30, "dispatch_frequency": 0.20},
  "eisenhower_value_weights": {"irr_v2": 0.50, "penetration_terminal": 0.25, "revenue_potential": 0.25},
  "refresh_cadence": {"cluster_boundaries": "stable", "score_refresh": "monthly"}
}
```

Changes to weights in this file take effect on next `eisenhower_scoring.py` run. Formation weights in `telecom_clustering_v6.py` are currently hardcoded constants (must edit the .py file to change).

---

## File Inventory

All files live in `C:\Users\v267429\Downloads\AI_Sessions\`.

### Scripts

| File | Lines | Role |
|------|-------|------|
| `extract_all_nonofs_addresses.py` | ~180 | Oracle extraction + IRR join |
| `cluster_all_nonofs.py` | ~405 | Main clustering orchestrator (streaming, checkpoint/resume) |
| `telecom_clustering_v6.py` | ~557 | V6 clustering module (formation scoring, 6-phase algorithm) |
| `telecom_clustering_v5.py` | ~500+ | V5 module (morphology params, haversine, imported by V6) |
| `road_graph.py` | ~200+ | Road graph builder (TIGER GeoJSON -> graph, address snapping) |
| `tiger_pipeline.py` | ~300+ | TIGER road download + shapefile-to-GeoJSON conversion |
| `tag_obligations_v2.py` | ~248 | Post-formation obligation tagging |
| `eisenhower_scoring.py` | ~306 | Urgency x Value scoring + quadrant assignment |
| `generate_cluster_polygons.py` | ~199 | Convex-hull polygon generation |
| `write_oracle_master_tables.py` | ~310 | Oracle table DDL + bulk insert |
| `export_arcgis_gdb.py` | ~200 | ArcGIS file GDB export (requires arcgispro-py3) |
| `monthly_refresh.py` | ~84 | Monthly score refresh orchestrator |
| `clustering_config.json` | ~79 | Scoring weights + thresholds config |

### Data Files

| File | Description |
|------|-------------|
| `all_nonofs_12m.csv` | Raw extraction (12.3M rows, ~4 GB) |
| `all_nonofs_12m_sorted.csv` | Sorted by CLLI for streaming (~4 GB) |
| `irr_all_base_addresses.csv` | V2 IRR ramp results (12.3M rows) |
| `clli_county_fips_all.csv` | CLLI -> FIPS mapping |
| `v6_clusters_cache.json` | Master cluster cache (~200 MB) |
| `v6_checkpoint.json` | Resume checkpoint |
| `v6_cluster_summary.csv` | Cluster summary |
| `v6_cluster_eisenhower.csv` | Cluster summary + Eisenhower scores |
| `addr_obligation_tags.csv` | Address obligation tags (12.3M rows) |
| `all_nonofs_cluster_polygons.geojson` | Cluster polygons (~100 MB) |
| `TIGER/` | TIGER road GeoJSON files (~1 GB) |
| `NONOFS_Master.gdb` | ArcGIS file geodatabase |

---

## Coordinate Filtering

All scripts apply an ILEC bounding box filter to exclude bad geocodes (e.g., Iowa outliers from upstream data):

```
Latitude:  36.3 - 47.5
Longitude: -83.0 - -66.5
```

This covers the Verizon ILEC footprint from Virginia to Maine. Addresses outside this box are silently dropped during extraction and polygon generation.
