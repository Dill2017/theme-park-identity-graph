# Databricks notebook source
# MAGIC %md
# MAGIC # The Roadmap to Graph: a native ladder walkthrough
# MAGIC
# MAGIC This notebook brings the **"climb the ladder" build-vs-buy story** to life on Databricks. The idea (from
# MAGIC Databricks Field Engineering's *Roadmap to Graph*): most "we need a graph database" asks are really
# MAGIC multi-hop-query and agent-context asks. So **start at the lowest rung that answers your benchmark
# MAGIC questions, and only climb to the next rung with a concrete, demonstrated reason.** You buy an external
# MAGIC graph engine only when a named question the rungs below cannot answer proves you need to.
# MAGIC
# MAGIC It builds directly on the tables created by `identity_spine_to_graph_demo` (the Splink golden guests and
# MAGIC the booking / party / transaction data for a fictional theme-park operator). Run that notebook first, or
# MAGIC point the widgets at the schema it wrote.
# MAGIC
# MAGIC **The one idea that ties every rung together:** a single standard **edge table** (`gold_triplets`). Produce
# MAGIC it once off the identity spine and every rung consumes it unchanged. That is what makes the path
# MAGIC reversible and free of dead-ends: nothing built on an early rung is thrown away when you add the next.
# MAGIC
# MAGIC **The rungs demonstrated here (the native ones that matter most):**
# MAGIC - **Rung 0 - Managed semantics.** Governed metric views over the lakehouse, read by Genie. Check first.
# MAGIC - **Rung 1 - Edge table into Genie.** The `gold_triplets` contract plus enriched edge and node-degree views.
# MAGIC - **Rung 4 - Native traversal.** Multi-hop reachability with recursive SQL, no graph database.
# MAGIC - **Rung 6 - Graph algorithms.** The GraphFrames suite: community detection, centrality, motifs, triangles.
# MAGIC
# MAGIC Higher rungs (agentic knowledge graph, ontology engines, GraphRAG, extreme scale, GNNs) exist and are also
# MAGIC largely native; they are out of scope for this walkthrough.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Requirements and setup
# MAGIC
# MAGIC - **Compute is rung-dependent, and that is part of the story:**
# MAGIC   - Rungs 0, 1 and 4 are **pure SQL**. Run them on **serverless SQL / a SQL warehouse, or a cluster on
# MAGIC     Databricks Runtime 17.0+** (recursive CTEs, `WITH RECURSIVE`, are GA from DBR 17.0). They do not run on
# MAGIC     older runtimes.
# MAGIC   - Rung 6 uses **GraphFrames**, which needs the Maven library attached and does **not** run on serverless.
# MAGIC     Use a classic cluster with `graphframes:graphframes:0.8.4-spark3.5-s_2.12` (or a matching ML runtime).
# MAGIC - **Prerequisite tables:** the `identity_spine_to_graph_demo` outputs must exist in the target schema
# MAGIC   (`guests_raw`, `guests_resolved`, `party_members`, `bookings`, `transactions`).
# MAGIC - Set the widgets below. Nothing defaults to a shared catalog.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Target catalog")
dbutils.widgets.text("schema", "identity_graph_demo", "Target schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
assert CATALOG, "Set the 'catalog' widget to the catalog holding the identity_spine_to_graph_demo tables."

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Confirm the prerequisite tables are present.
required = {"guests_resolved", "party_members", "bookings"}
present = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()}
missing = required - present
assert not missing, f"Missing prerequisite tables {missing}. Run identity_spine_to_graph_demo first."
print(f"Using {CATALOG}.{SCHEMA}. Prerequisite tables present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The shared spine: the `gold_triplets` edge table
# MAGIC
# MAGIC The graph is not a separate database, it is an edge table plus properties. We standardise on the field's
# MAGIC 8-column contract (`subject_id, subject_type, predicate, object_id, object_type, confidence, source_method,
# MAGIC source_agent`) and build three edge types off the identity spine:
# MAGIC
# MAGIC - **RESOLVES_TO** - a raw source record resolves to a golden guest (the identity seed, from Splink).
# MAGIC - **VISITED_WITH** - two golden guests appeared in the same party (the relationship edge that forms cohorts).
# MAGIC - **PAID_FOR** - the booking payer paid for another party member (the "my dad paid" edge, a directed
# MAGIC   value/influence signal).
# MAGIC
# MAGIC Every rung below reads this one table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_triplets AS
# MAGIC -- RESOLVES_TO: raw record -> golden guest (identity seed)
# MAGIC SELECT r.record_id  AS subject_id, 'raw_record' AS subject_type,
# MAGIC        'RESOLVES_TO' AS predicate,
# MAGIC        r.golden_id  AS object_id,  'guest'      AS object_type,
# MAGIC        CAST(0.95 AS DOUBLE) AS confidence, 'entity_resolution' AS source_method, 'splink' AS source_agent
# MAGIC FROM guests_resolved r
# MAGIC UNION ALL
# MAGIC -- VISITED_WITH: two golden guests in the same party (undirected, both directions stored)
# MAGIC SELECT DISTINCT ga.golden_id, 'guest', 'VISITED_WITH', gb.golden_id, 'guest',
# MAGIC        CAST(1.0 AS DOUBLE), 'shared_party', 'graph_build'
# MAGIC FROM party_members pa
# MAGIC JOIN guests_resolved ga ON pa.record_id = ga.record_id
# MAGIC JOIN party_members  pb ON pa.party_id  = pb.party_id
# MAGIC JOIN guests_resolved gb ON pb.record_id = gb.record_id
# MAGIC WHERE ga.golden_id <> gb.golden_id
# MAGIC UNION ALL
# MAGIC -- PAID_FOR: booking payer -> each other party member (directed)
# MAGIC SELECT DISTINCT gp.golden_id, 'guest', 'PAID_FOR', gm.golden_id, 'guest',
# MAGIC        CAST(1.0 AS DOUBLE), 'booking_payer', 'graph_build'
# MAGIC FROM bookings b
# MAGIC JOIN guests_resolved gp ON b.payer_record_id = gp.record_id
# MAGIC JOIN party_members   pm ON b.party_id        = pm.party_id
# MAGIC JOIN guests_resolved gm ON pm.record_id       = gm.record_id
# MAGIC WHERE gp.golden_id <> gm.golden_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The contract, and the shape of the graph, in one look.
# MAGIC SELECT predicate, count(*) AS edges FROM gold_triplets GROUP BY predicate ORDER BY edges DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rung 0 - Managed semantics (check this first)
# MAGIC
# MAGIC Before building anything graph-shaped, ask whether the **managed semantic path** already answers the
# MAGIC question: Genie Ontology, the Unity Catalog business glossary, and **metric views**. These are governed,
# MAGIC low-code, and read by Genie directly. Many "graph" asks are really metric questions in disguise
# MAGIC ("average spend by cohort", "uplift after the free-pass campaign") and stop right here.
# MAGIC
# MAGIC Below we define a **metric view** over cohort value. If your workspace/runtime does not support the metric
# MAGIC view syntax, the guarded fallback creates an equivalent governed SQL view, so the rung still demonstrates.

# COMMAND ----------

# Rung 0: a governed metric view (productized managed semantics), with a governed-view fallback.
metric_view_yaml = f"""version: 0.1
source: {CATALOG}.{SCHEMA}.cohort_spend
dimensions:
  - name: segment
    expr: segment
measures:
  - name: total_spend_gbp
    expr: SUM(total_spend_gbp)
  - name: cohorts
    expr: COUNT(DISTINCT cohort_id)
  - name: members
    expr: SUM(cohort_members)
"""
created = None
try:
    spark.sql(f"DROP VIEW IF EXISTS {CATALOG}.{SCHEMA}.mv_cohort_value")
    spark.sql(
        f"CREATE VIEW {CATALOG}.{SCHEMA}.mv_cohort_value WITH METRICS "
        f"LANGUAGE YAML AS $$\n{metric_view_yaml}$$"
    )
    created = "metric view (mv_cohort_value)"
except Exception as e:
    print(f"Metric view syntax not available here ({type(e).__name__}); using a governed SQL view instead.")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_cohort_value AS
        SELECT segment,
               SUM(total_spend_gbp)        AS total_spend_gbp,
               COUNT(DISTINCT cohort_id)   AS cohorts,
               SUM(cohort_members)         AS members
        FROM {CATALOG}.{SCHEMA}.cohort_spend
        GROUP BY segment
    """)
    created = "governed SQL view (mv_cohort_value)"
print(f"Rung 0: created {created}. Add it to your Genie space so Genie answers metric questions from it.")
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.mv_cohort_value ORDER BY total_spend_gbp DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rung 1 - Edge table into Genie
# MAGIC
# MAGIC The lowest graph rung: submit the edge table to a Genie space so Genie can answer **relationship** questions
# MAGIC by crafting joins over it ("who did guest X visit with", "who paid for whom"). We add two helper views that
# MAGIC make the edges human-readable and expose node degree, which is all Genie needs to reason about connections.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Human-readable edges: attach the golden guest's best-known name to each end.
# MAGIC CREATE OR REPLACE VIEW v_edges_labeled AS
# MAGIC WITH guest_name AS (
# MAGIC   SELECT gr.golden_id,
# MAGIC          max_by(concat_ws(' ', gr2.first_name, gr2.last_name), length(concat_ws(' ', gr2.first_name, gr2.last_name))) AS name
# MAGIC   FROM guests_resolved gr
# MAGIC   JOIN guests_raw gr2 ON gr.record_id = gr2.record_id
# MAGIC   GROUP BY gr.golden_id
# MAGIC )
# MAGIC SELECT t.subject_id, sn.name AS subject_name, t.predicate,
# MAGIC        t.object_id, on2.name AS object_name, t.confidence, t.source_method
# MAGIC FROM gold_triplets t
# MAGIC LEFT JOIN guest_name sn ON t.subject_id = sn.golden_id
# MAGIC LEFT JOIN guest_name on2 ON t.object_id  = on2.golden_id
# MAGIC WHERE t.predicate IN ('VISITED_WITH','PAID_FOR');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Node degree: how connected each guest is (a first, cheap centrality signal Genie can rank on).
# MAGIC CREATE OR REPLACE VIEW v_node_degree AS
# MAGIC SELECT subject_id AS guest_id, count(*) AS degree
# MAGIC FROM gold_triplets
# MAGIC WHERE predicate = 'VISITED_WITH'
# MAGIC GROUP BY subject_id;
# MAGIC
# MAGIC SELECT d.guest_id, n.name, d.degree
# MAGIC FROM v_node_degree d
# MAGIC LEFT JOIN (SELECT DISTINCT subject_id, subject_name FROM v_edges_labeled) n ON d.guest_id = n.subject_id
# MAGIC ORDER BY d.degree DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC **To finish Rung 1:** add `gold_triplets`, `v_edges_labeled` and `v_node_degree` to your Genie space, with a
# MAGIC couple of sample questions ("who did the most-connected guest visit with?", "which guests did guest G pay
# MAGIC for?"). Genie now answers relationship questions with no graph engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rung 4 - Native traversal, no graph database
# MAGIC
# MAGIC When you need multi-hop traversal ("everyone within N hops of this guest through shared parties"), you do
# MAGIC **not** immediately need a graph database. Databricks has two native answers:
# MAGIC
# MAGIC 1. **Recursive SQL** (`WITH RECURSIVE`, GA from DBR 17.0). Genie can generate this directly against the edge table.
# MAGIC 2. **Pre-generated hop tables**, when query cost and predictability matter, which we also materialise for Genie.
# MAGIC
# MAGIC This is the rung most "we need Neo4j" conversations actually stop at.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- k-hop reachability from a seed guest over VISITED_WITH, native recursive SQL (requires DBR 17.0+ / DBSQL).
# MAGIC WITH RECURSIVE reach(guest_id, depth, path) AS (
# MAGIC   SELECT object_id, 1, array(subject_id, object_id)
# MAGIC   FROM gold_triplets
# MAGIC   WHERE predicate = 'VISITED_WITH'
# MAGIC     AND subject_id = (SELECT guest_id FROM v_node_degree ORDER BY degree DESC LIMIT 1)  -- seed: most connected guest
# MAGIC   UNION ALL
# MAGIC   SELECT t.object_id, r.depth + 1, array_append(r.path, t.object_id)
# MAGIC   FROM reach r
# MAGIC   JOIN gold_triplets t ON t.subject_id = r.guest_id AND t.predicate = 'VISITED_WITH'
# MAGIC   WHERE r.depth < 3 AND NOT array_contains(r.path, t.object_id)  -- bound depth, avoid cycles
# MAGIC )
# MAGIC SELECT depth, count(DISTINCT guest_id) AS guests_reached
# MAGIC FROM reach GROUP BY depth ORDER BY depth;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pre-generated 2-hop neighbourhood table for Genie (predictable, cheap to query).
# MAGIC CREATE OR REPLACE TABLE guest_2hop AS
# MAGIC WITH RECURSIVE reach(seed, guest_id, depth) AS (
# MAGIC   SELECT subject_id, object_id, 1
# MAGIC   FROM gold_triplets WHERE predicate = 'VISITED_WITH'
# MAGIC   UNION ALL
# MAGIC   SELECT r.seed, t.object_id, r.depth + 1
# MAGIC   FROM reach r
# MAGIC   JOIN gold_triplets t ON t.subject_id = r.guest_id AND t.predicate = 'VISITED_WITH'
# MAGIC   WHERE r.depth < 2
# MAGIC )
# MAGIC SELECT seed AS guest_id, guest_id AS reachable_guest, min(depth) AS hops
# MAGIC FROM reach WHERE seed <> guest_id
# MAGIC GROUP BY seed, guest_id;
# MAGIC
# MAGIC SELECT hops, count(*) AS pairs FROM guest_2hop GROUP BY hops ORDER BY hops;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rung 6 - Graph algorithms at scale (GraphFrames)
# MAGIC
# MAGIC When the questions are genuinely algorithmic (community detection, centrality, motifs), Databricks runs the
# MAGIC full graph-analytics suite natively. We build the GraphFrame **from the same `gold_triplets` edge table** and
# MAGIC run:
# MAGIC - **Community detection** (label propagation) -> activity cohorts.
# MAGIC - **Centrality** (degree + PageRank) -> influencers.
# MAGIC - **Triangle counting** -> tightly-knit groups.
# MAGIC - **Motif finding** -> relationship patterns ("A visited with B, and B paid for C").
# MAGIC
# MAGIC **Compute note:** this cell needs GraphFrames (Maven) on a classic cluster, not serverless. Connected-components
# MAGIC checkpointing must use DBFS on a single-user cluster.

# COMMAND ----------

from graphframes import GraphFrame
from pyspark.sql import functions as F

spark.sparkContext.setCheckpointDir("dbfs:/tmp/gf_checkpoints")

# Vertices = golden guests; edges = VISITED_WITH from the shared contract.
edges = (spark.table("gold_triplets")
         .where("predicate = 'VISITED_WITH'")
         .selectExpr("subject_id AS src", "object_id AS dst"))
vertices = (edges.select(F.col("src").alias("id"))
            .union(edges.select(F.col("dst").alias("id")))
            .distinct())
g = GraphFrame(vertices, edges)
print(f"GraphFrame: {g.vertices.count()} guests, {g.edges.count()} VISITED_WITH edges")

# COMMAND ----------

# Community detection (label propagation) -> cohorts.
communities = g.labelPropagation(maxIter=5)
n_comm = communities.select("label").distinct().count()
print(f"Label propagation found {n_comm} communities (cohorts)")
communities.write.mode("overwrite").saveAsTable("guest_communities")

# Centrality: degree + PageRank -> influencers.
deg = g.degrees
pr = g.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "pagerank")
influence = deg.join(pr, "id").orderBy(F.desc("pagerank"))
influence.write.mode("overwrite").saveAsTable("guest_centrality")
display(influence.limit(10))

# COMMAND ----------

# Triangle counting (tightly-knit groups) and a motif (A visited-with B, B paid-for C).
triangles = g.triangleCount()
print(f"Guests in at least one triangle: {triangles.where('count > 0').count()}")

paid = (spark.table("gold_triplets").where("predicate = 'PAID_FOR'")
        .selectExpr("subject_id AS src", "object_id AS dst"))
g_full = GraphFrame(vertices, edges.withColumn("rel", F.lit("VISITED_WITH"))
                    .unionByName(paid.withColumn("rel", F.lit("PAID_FOR"))))
motifs = (g_full.find("(a)-[e1]->(b); (b)-[e2]->(c)")
          .where("e1.rel = 'VISITED_WITH' AND e2.rel = 'PAID_FOR' AND a.id <> c.id"))
print(f"'visited-with then paid-for' motif instances: {motifs.count()}")
display(motifs.select("a.id", "b.id", "c.id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The payoff: climb only with evidence
# MAGIC
# MAGIC Everything above ran natively on Databricks, on one `gold_triplets` edge table, with no graph database:
# MAGIC
# MAGIC - **Rung 0** answered metric questions from a governed semantic layer.
# MAGIC - **Rung 1** gave Genie relationship-aware answers over the edge table.
# MAGIC - **Rung 4** did multi-hop traversal in recursive SQL.
# MAGIC - **Rung 6** ran community detection, centrality, triangles and motifs.
# MAGIC
# MAGIC **You buy an external graph engine only at the point a named benchmark question fails these rungs** (for
# MAGIC example a genuine sub-second, high-concurrency traversal need). Because every rung and any external engine
# MAGIC read the same edge table, that decision stays reversible, and Databricks stays the system of record, the
# MAGIC identity layer, and the governance and semantic plane underneath whatever sits on top.
