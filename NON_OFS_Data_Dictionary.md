# NON-OFS ILEC Master Clustering — Data Dictionary & Methodology

**Run Date:** 2026-04-22
**Scope:** 12,262,162 NON-OFS ILEC addresses across 2,078 wire centers, clustered into 68,071 build-priority hubs
**Oracle Tables:** `TABLEAU_USER.CLUSTER_ASSIGNMENT_MASTER` (address-level) and `TABLEAU_USER.CLUSTER_SUMMARY_MASTER` (cluster-level)

---

## What This Is

This analysis clusters every NON-OFS address in the Verizon ILEC footprint into geographically contiguous, build-ready hubs using road-graph connectivity and a financial quality model. Each hub is then scored on two independent axes — **Urgency** (obligation and copper retirement pressure) and **Value** (IRR, penetration, and revenue potential) — producing a 2x2 Eisenhower prioritization matrix.

The intent is to identify **where to build next** by balancing financial return with regulatory and operational urgency.

---

## Table 1: CLUSTER_ASSIGNMENT_MASTER (Address-Level)

12,262,162 rows — one per address. Primary key: `(LOCUS_ADDRESS_ID, RUN_DATE)`.

| Column | Definition | Source / Calculation |
|--------|-----------|---------------------|
| **LOCUS_ADDRESS_ID** | Unique address identifier | `GPSAA.G_ALL_LOCUS_ADDRESS_KEY` |
| **CLUSTER_ID** | Hub assignment (format: `{CLLI}_H{nnn}`) | Clustering algorithm output. Each address belongs to exactly one cluster. |
| **CLLI** | Wire center code (8-char) | `GPSAA.S_BEYOND_INFINITY_RANKING_SCORING` |
| **REGION** | Verizon operating region (e.g., NE Metro, Potomac, Tri-State) | BI scoring table |
| **SUB_REGION** | Sub-region within region (e.g., NY Upstate, VA, Eastern MA) | BI scoring table |
| **STATE** | Two-letter state code | BI scoring table |
| **MARKET_DENSITY** | Morphology tier (URBAN, SUBURBAN, RURAL, etc.) | BI scoring table. 10 tiers from ULTRADENSE-URBAN to ULTRA-RURAL. |
| **AUI** | Address Use Indicator — SFU (single family), SBU (small business), MDU (multi-dwelling), MTU (multi-tenant) | `GPSAA.S_BEYOND_INFINITY_RANKING_SCORING` |
| **NO_OF_UNITS** | NTAS unit count at this address. MDU/MTU addresses may have 2-500+ units. | BI scoring table. Defaults to 1 if null. |
| **LATITUDE / LONGITUDE** | WGS84 coordinates | `G_ALL_LOCUS_ADDRESS_KEY` |
| **CPO_NTAS** | **Cost Per Opportunity — NTAS level.** BI-predicted cost to pass this sub-location. | `GPS_MODELS.COST_MODEL_BASE_ADDR_NON_OFS` via BI scoring. Units: dollars per unit. |
| **CPO_PRED** | **Cost Per Opportunity — Predicted.** BI-predicted cost at the base address level. | Same source as CPO_NTAS but at address level (not sub-location). |
| **TOTAL_CAPEX** | **Total estimated capital expenditure** to build fiber to this address. | BI scoring table. Includes CO overhead allocation. |
| **COMPUTED_IRR** | **15-year Internal Rate of Return.** Based on V2 penetration ramp model with year-by-year S-curve revenue buildup, OFS-weighted by morphology and NT type. | Calculated via `irr_v2_ramp_computation.py`. 30-year DCF discounted to 15-year equivalent (×0.85 factor). Values are in percentage format (e.g., 15.7 = 15.7%). Mean: 14.3%, Median: 11.0%. |
| **PEN_TERMINAL** | **Terminal penetration rate** — projected steady-state FiOS take rate for this address's morphology/NT type. | V2 Penetration Ramp Model. OFS-weighted logistic S-curves. Urban ~41%, Suburban ~54%, Rural ~57%. |
| **AVG_ANNUAL_EBITDA** | **Average annual EBITDA** contribution from this address over the model period. | BI scoring table. Derived from penetration × ARPU − operating costs. |
| **IRR_BUCKET** | IRR tier classification | `<0%`, `0-5%`, `5-10%`, `10-15%`, `15-20%`, `20%+` from BI scoring |
| **NT_TYPE** | Network termination type — NT1 (existing plant, lower cost) or NT2 (new build, higher cost) | BI scoring table. NT1 mult: 1.18×, NT2 mult: 1.35×. |
| **ODN_FLAG** | Whether address requires ODN (Outside Plant Distribution Network) construction | BI scoring table |
| **COPPER_CIR_COUNT** | Number of active copper circuits at this address | BI scoring table. Key input to copper salvage value. |
| **COPPER_CUST_COUNT** | Number of copper customers at this address | BI scoring table |
| **DISPATCH_1YR** | Truck roll / dispatch count in the past 12 months | BI scoring table. Addresses with high dispatch = high maintenance cost = urgency driver. |
| **OBLIGATION_BUCKET** | **Regulatory/strategic obligation classification.** Assigned post-clustering in priority order (first match wins). | See Obligation Buckets section below. |
| **PRIORITY_RANK** | BI composite priority rank (lower = higher priority) | `GPSAA.S_BEYOND_INFINITY_RANKING_SCORING`. Combines dispatch, sales propensity, geographic synergy. |
| **FORMATION_SCORE** | Financial quality score used during cluster formation (0-100) | See Formation Scoring section below. |
| **RUN_DATE** | Date this data was generated. Enables monthly partitioned refreshes. | Pipeline run date. |

---

## Table 2: CLUSTER_SUMMARY_MASTER (Cluster-Level)

68,071 rows — one per cluster. Primary key: `(CLUSTER_ID, RUN_DATE)`.

| Column | Definition | Source / Calculation |
|--------|-----------|---------------------|
| **CLUSTER_ID** | Unique cluster identifier. Format: `{CLLI}_H{nnn}` (e.g., `NWRKNJMD_H003`). CLLI prefix = wire center, H number = hub sequence within that WC. | Clustering algorithm |
| **CLLI** | Wire center code | Inherited from member addresses |
| **REGION / SUB_REGION** | Operating region and sub-region | Inherited from member addresses |
| **MARKET_DENSITY** | Predominant morphology of the cluster | Inherited from first address in cluster |
| **CENTROID_LAT / CENTROID_LON** | Geographic center of the cluster | Mean of hub centroid coordinates (weighted by road-group positions) |
| **TOTAL_UNITS** | Sum of `NO_OF_UNITS` across all member addresses | Direct sum. MDU/MTU addresses contribute multiple units. |
| **TOTAL_ADDRS** | Count of distinct addresses in the cluster | Direct count of LOCUS_ADDRESS_IDs |
| **TOTAL_CAPEX** | Sum of `TOTAL_CAPEX` across all member addresses | Direct sum, rounded to nearest dollar |
| **AVG_CPP** | **Average Cost Per Premises.** Total CAPEX / Total Units. | `TOTAL_CAPEX / TOTAL_UNITS`. This is the key efficiency metric — lower CPP = more cost-effective to build. |
| **MEDIAN_IRR** | Median `COMPUTED_IRR` of member addresses (excludes zeros) | Sorted middle value of non-zero IRR values |
| **COPPER_CIRCUITS** | Total copper circuits across all member addresses | Sum of `COPPER_CIR_COUNT` |
| **URGENCY_SCORE** | **Urgency axis (0-100).** How pressing is the need to build here? | See Eisenhower Scoring section below. |
| **VALUE_SCORE** | **Value axis (0-100).** How financially attractive is this cluster? | See Eisenhower Scoring section below. |
| **BUILD_PRIORITY_TIER** | **Eisenhower quadrant assignment.** | `Q1_Do_First` (urgent + valuable), `Q2_Schedule` (valuable, less urgent), `Q3_Must_Do` (urgent, less valuable), `Q4_Deprioritize`. See below. |
| **TOP_OBLIGATION** | Highest-priority obligation bucket present in this cluster | First non-DISCRETIONARY bucket found in priority order. If all addresses are discretionary, this = `DISCRETIONARY`. |
| **OBLIGATION_FRACTION** | Fraction of cluster addresses carrying any obligation (0.0 to 1.0) | `(total addrs - discretionary addrs) / total addrs` |
| **AVG_FORMATION_SCORE** | Mean financial quality score of member addresses (0-100) | Average of per-address `fin_score` from V6 formation |
| **AUI_SFU / AUI_SBU / AUI_MDU / AUI_MTU** | Unit count by Address Use Indicator | Sum of units by AUI type within the cluster |
| **OBLIGATION_FILL** | Detailed breakdown of obligation buckets | Pipe-delimited string: `COP_2026_OBLIG:42|SBB_OBLIG:118|DISCRETIONARY:340` |
| **RUN_DATE** | Partition date | Pipeline run date |

---

## Formation Scoring (How Clusters Are Built)

Clusters are formed using a **6-phase geospatial-first algorithm** that groups addresses along road networks into build-ready hubs. The financial quality score determines **seed priority** (which road groups become hub centers) and **growth order** (which groups merge into which hubs).

### Formation Score (0-100)

Each address receives a composite financial quality score:

| Component | Weight | What It Measures | Normalization |
|-----------|--------|-----------------|---------------|
| **IRR V2** | 40% | 15-year internal rate of return from V2 ramp model | Min-max within wire center, higher = better |
| **Copper Salvage** | 25% | Estimated recoverable value of existing copper plant | `copper_circuits × $200/circuit` (industry avg), normalized to WC max |
| **BI Priority Rank** | 20% | BI composite rank (dispatch, sales, geo synergy) | Inverted min-max (lower rank = higher score) |
| **Unit Density** | 15% | MDU/MTU density bonus | `min(units / 10, 1.0)` — caps at 10+ units |

**Why these weights:** IRR dominates because it directly measures build economics. Copper salvage is second because it represents both a financial recovery and an operational imperative (copper retirement). BI rank captures operational synergies not in IRR. Density rewards MDU/MTU efficiency.

**Why obligations are excluded from formation:** Obligations change (new mandates, date shifts, scope changes) but cluster boundaries should be stable. Separating obligation tagging from formation means monthly score refreshes don't require re-clustering.

### Clustering Algorithm (6 Phases)

1. **Formation Scoring** — Score every address (above formula)
2. **Road-Group Construction** — Snap addresses to nearest TIGER road segment, group by connected road segments
3. **Adjacency Graph** — Build a graph of which road groups are physically adjacent (within morphology-specific radius caps)
4. **Seed & Grow** — Highest-scored road groups become hub seeds; adjacent groups merge in via growth scoring (financial 35%, proximity 25%, IRR gravity 25%, street affinity 15%)
5. **Size Enforcement** — Split oversized hubs, merge undersized ones (target sizes vary by morphology: 150-450 units suburban, 400-700 urban, 50-200 rural)
6. **Finalize** — Assign hub IDs, compute centroids, build output records

---

## Obligation Buckets (Post-Formation Tags)

Each address is tagged with exactly one obligation bucket. **Priority-ordered — first match wins:**

| Priority | Bucket | Definition | Count | % |
|----------|--------|-----------|-------|---|
| 1 | **COP_2026_OBLIG** | Address is in a planned copper recycling area with start date in 2026. Fiber must be built before copper is retired. | 496,908 | 4.1% |
| 2 | **COP_2027_OBLIG** | Same as above, but copper recycling start date in 2027. | 386,214 | 3.1% |
| 3 | **COP_FUTURE_OBLIG** | Copper recycling planned but start date is 2028+. | 14,385 | 0.1% |
| 4 | **SBB_OBLIG** | Address is in a state broadband obligation area (state-level grant or regulatory commitment). | 2,344,904 | 19.1% |
| 5 | **NSI_OBLIG** | Address matches a National Security Interest inquiry via the NTAS bridge table. | 20,280 | 0.2% |
| 6 | **LFA_OBLIG** | Address is in a Local Franchise Authority obligation area. | 0 | 0.0% |
| 7 | **DISCRETIONARY** | No external obligation — build decision is purely economic. | 8,999,471 | 73.4% |

**Data sources:** Copper recycling dates from `PLANNED_COPPER_RECYCLING` and `COPPER_RECYCLING_START_DATE` fields in BI scoring. SBB from `WC_SBB_FLAG` and `ADDR_SBB_FLAG`. NSI via `NSEEPRD.NSI_INQUIRIES` → `GPSAA.NTAS_LOCUS_MAP` bridge (27,981 matched LAIDs). LFA from `COUNTY_TYPE = 'LFA'` (currently unpopulated for NON-OFS).

---

## Eisenhower Scoring (Build Prioritization)

Each cluster is scored on two independent 0-100 axes, then assigned to a quadrant.

### Urgency Score (0-100) — "How pressing is the need to build?"

| Component | Weight | What It Measures | Scoring |
|-----------|--------|-----------------|---------|
| **Obligation Tier** | 50% | Highest obligation bucket in the cluster | COP_2026=100, COP_2027=80, COP_FUTURE=60, SBB=70, NSI=50, LFA=40, DISC=0 |
| **Copper Recycling Imminence** | 30% | % of addresses with copper retirement date in 2026-2027 | Linear: 0% → 0, 100% → 100 |
| **Dispatch Frequency** | 20% | Average truck rolls per address in past 12 months | `min(avg_dispatch × 20, 100)` — 5+ dispatches/yr = 100 |

### Value Score (0-100) — "How financially attractive is this cluster?"

| Component | Weight | What It Measures | Scoring |
|-----------|--------|-----------------|---------|
| **IRR V2** | 50% | Average 15-year IRR across cluster | 0% → 0, 15% → 50, 30%+ → 100 (linear) |
| **Terminal Penetration** | 25% | Average projected FiOS take rate | `min(pen × 200, 100)` — 50%+ terminal = 100 |
| **Revenue Potential** | 25% | Average annual EBITDA per address | `min(ebitda / $500, 100)` — $500+/yr = 100 |

### Eisenhower Quadrants

|  | **Value >= 50** | **Value < 50** |
|--|----------------|----------------|
| **Urgency >= 50** | **Q1 — Do First** (6,651 clusters, 1.1M units) Build immediately: high return AND obligation/operational pressure | **Q3 — Must Do** (1,039 clusters, 98K units) Obligation-driven but lower returns. Build to meet commitments. |
| **Urgency < 50** | **Q2 — Schedule** (44,525 clusters, 11.1M units) Economically attractive, no urgency. Schedule into build plan by ROI order. | **Q4 — Deprioritize** (15,856 clusters, 3.5M units) Low return, no obligation. Defer or evaluate selectively. |

---

## Key Statistics (2026-04-22 Run)

| Metric | Value |
|--------|-------|
| Total addresses | 12,262,162 |
| Total units (premises) | 15,858,739 |
| Wire centers | 2,078 |
| Clusters (hubs) | 68,071 |
| Avg addresses per cluster | 180 |
| Avg units per cluster | 233 |
| Addresses with obligations | 3,262,691 (26.6%) |
| Clusters with obligations | 25,333 (37.2%) |
| GeoJSON polygon features | 65,386 |
| ArcGIS File GDB | `NONOFS_Master.gdb` |

---

## Data Sources

| Source Table | What It Provides |
|-------------|-----------------|
| `GPSAA.S_BEYOND_INFINITY_RANKING_SCORING` | Address-level BI scoring: CPO, IRR, penetration, EBITDA, dispatch, copper, AUI, market density |
| `GPSAA.G_ALL_LOCUS_ADDRESS_KEY` | Address master: LOCUS_ADDRESS_ID, coordinates, CLLI, CATEGORY, CENSUSBLOCKID |
| `GPSAA.NTAS_LOCUS_MAP` | Bridge table: maps NTAS ADDRESS_ID to LOCUS_ADDRESS_ID |
| `NSEEPRD.NSI_INQUIRIES` | National Security Interest inquiries (ADDRESS_ID) |
| `GPSAA.EWO_W_CMTDATE_NODUP_PIPELINE_2026` | EWO pipeline addresses with commitment dates |
| US Census TIGER/Line | Road network geometry for 178 counties (road-graph clustering) |

---

## Monthly Refresh Process

Run `monthly_refresh.py` to update scores without re-clustering:

1. Re-extract address data (updated BI scores from Oracle)
2. Re-tag obligations (updated copper dates, SBB flags)
3. Re-score Eisenhower matrix (updated urgency/value)
4. Regenerate polygon metadata (geometry unchanged)
5. Write Oracle tables with new `RUN_DATE` partition

Cluster boundaries remain stable — only scores and tags change. Use `--dry-run` to score without writing Oracle.

---

## Output Files

| File | Size | Description |
|------|------|-------------|
| `v6_clusters_cache.json` | ~200 MB | Full cluster data with all scores and address lists |
| `v6_cluster_summary.csv` | ~8 MB | Flat CSV of cluster-level metrics |
| `v6_cluster_eisenhower.csv` | ~5 MB | Urgency/value scores and quadrant assignments |
| `addr_obligation_tags.csv` | ~319 MB | Address-level obligation bucket assignments |
| `all_nonofs_cluster_polygons.geojson` | 54 MB | Convex hull polygons for all clusters |
| `NONOFS_Master.gdb` | ~2 GB | ArcGIS File GDB with polygons, points, WC boundaries, fiber basemap |
