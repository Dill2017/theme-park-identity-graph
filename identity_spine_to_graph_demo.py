# Databricks notebook source
# MAGIC %md
# MAGIC # Identity Spine to Knowledge Graph on Databricks (Phase 0 demo)
# MAGIC
# MAGIC A self-contained walkthrough of the "build the 80 to 90 percent now, natively" story for a theme-park
# MAGIC operator that already has a large, manually maintained identity spine and wants a relationship / network
# MAGIC graph on top of it, without copying data out of the lakehouse.
# MAGIC
# MAGIC **What this notebook shows, end to end:**
# MAGIC 1. Synthetic guest / booking / party / transaction data in Unity Catalog, with intentional duplicates
# MAGIC    and multilingual names (English, Japanese, Korean, Arabic, Russian) so the matching challenge is real.
# MAGIC 2. **Entity resolution with Splink** (probabilistic, inspectable, not a black box) to collapse duplicate
# MAGIC    source records into golden guest IDs. This is the "modernise the manual spine" step.
# MAGIC 3. **The relationship graph with GraphFrames**: activity cohorts (connected components) and influencers
# MAGIC    (PageRank / degree) built on the golden entities. These are the traversal questions that SQL, and
# MAGIC    therefore Genie, cannot express well.
# MAGIC 4. **Value attribution**: which cohorts drove downstream park spend (the "free annual pass" question).
# MAGIC 5. **The Genie link**: the same Unity Catalog tables power Genie for the tabular / metric questions, while
# MAGIC    the graph answers the connection questions. One lakehouse, one governance plane, no copy.
# MAGIC
# MAGIC **Honest status:** this notebook is authored but has not yet been executed in a live workspace. Library
# MAGIC and API versions (Splink, GraphFrames) should be validated on your target runtime. See the setup notes in
# MAGIC the next cell. It is deliberately small-scale (a few thousand guests) so it runs quickly on any cluster;
# MAGIC the closing section explains what changes at production scale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Requirements and setup notes
# MAGIC
# MAGIC - **Compute:** a cluster or serverless with Spark. For GraphFrames you must attach the GraphFrames library
# MAGIC   as a **Maven** cluster library matching your Spark/Scala version, for example
# MAGIC   `graphframes:graphframes:0.8.4-spark3.5-s_2.12` (Databricks ML runtimes often already include it). The
# MAGIC   `pip` package alone does not ship the JVM classes.
# MAGIC - **Python libs:** `faker` (synthetic multilingual names) and `splink` (entity resolution) are installed in
# MAGIC   the first cell.
# MAGIC - **Entity resolution engine:** we use **Splink** with its DuckDB backend for a reliable, self-contained
# MAGIC   demo. Splink also has a Spark backend for scale, and the Databricks **Customer Entity Resolution**
# MAGIC   accelerator (Zingg-based, `databricks-industry-solutions/customer-er`) is the packaged alternative. Do not
# MAGIC   use the older ARC accelerator; its own README says it is abandoned and to use Splink directly.
# MAGIC - **Catalog / schema:** set the widgets below. Nothing defaults to a shared catalog; the schema is created
# MAGIC   if it does not exist.

# COMMAND ----------

# MAGIC %pip install --quiet faker "splink>=4,<5" duckdb
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Parameters. Set these to a catalog and schema you can write to.
dbutils.widgets.text("catalog", "", "Target catalog")
dbutils.widgets.text("schema", "identity_graph_demo", "Target schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
assert CATALOG, "Set the 'catalog' widget to a catalog you can write to."

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
# GraphFrames connected-components needs a RELIABLE Spark checkpoint dir to truncate
# iteration lineage. A /Volumes/... path is rejected on a single-user cluster, and a local
# path is unreliable for Spark checkpointing (intermittent stalls), so use DBFS scratch
# (transient algorithm checkpoint, not data storage).
spark.sparkContext.setCheckpointDir("dbfs:/tmp/gf_checkpoints")
print(f"Writing to {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate synthetic data with intentional duplicates and multilingual names
# MAGIC
# MAGIC We create a set of *true* guests, then emit 1 to 3 noisy *source records* per guest across systems
# MAGIC (CRM, web, loyalty, box office) with typos, nickname and transliteration variants, email variants and
# MAGIC missing fields. The raw table therefore contains duplicates that entity resolution has to reconcile. We
# MAGIC keep a hidden `true_id` only so we can measure how well ER recovers the truth. Bookings and transactions
# MAGIC are keyed by the **source-record id**, exactly as they would arrive in real life.

# COMMAND ----------

import random
from faker import Faker

random.seed(42)
Faker.seed(42)

# Multilingual name pools. Weighted mostly UK, with the non-Latin scripts that make matching hard.
LOCALE_WEIGHTS = {"en_GB": 60, "ja_JP": 12, "ko_KR": 10, "ar_AA": 10, "ru_RU": 8}
fakers = {loc: Faker(loc) for loc in LOCALE_WEIGHTS}
locales = list(LOCALE_WEIGHTS)
locale_p = [LOCALE_WEIGHTS[l] for l in locales]

N_TRUE_GUESTS = 1200
SYSTEMS = ["crm", "web", "loyalty", "boxoffice"]
PARKS = ["Northgate", "Riverside", "Summit Bay", "Lakeside", "Old Harbour", "Pinewood"]

NICKNAMES = {
    "Robert": "Bob", "William": "Will", "James": "Jim", "Elizabeth": "Liz",
    "Katherine": "Kate", "Michael": "Mike", "Alexander": "Alex", "Margaret": "Maggie",
}


def typo(s: str) -> str:
    """Introduce a small, realistic typo."""
    if not s or len(s) < 3:
        return s
    i = random.randrange(len(s) - 1)
    kind = random.random()
    if kind < 0.4:  # transpose
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]
    if kind < 0.7:  # drop a char
        return s[:i] + s[i + 1:]
    return s[:i] + s[i] + s[i:]  # duplicate a char


def vary_email(email: str) -> str:
    if random.random() < 0.5 and "@" in email:
        local, domain = email.split("@", 1)
        return f"{local}{random.randint(1, 99)}@{domain}"
    return email


# --- true guests ---
true_guests = []
for gid in range(N_TRUE_GUESTS):
    loc = random.choices(locales, weights=locale_p, k=1)[0]
    f = fakers[loc]
    first = f.first_name()
    last = f.last_name()
    # A stable base email derived from a romanised-ish handle.
    handle = fakers["en_GB"].user_name()
    email = f"{handle}@{fakers['en_GB'].free_email_domain()}"
    true_guests.append({
        "true_id": gid,
        "locale": loc,
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": fakers["en_GB"].msisdn(),
        "postcode": fakers["en_GB"].postcode(),
        "dob": f.date_of_birth(minimum_age=6, maximum_age=85).isoformat(),
        "home_park": random.choice(PARKS),
    })

# --- noisy source records (the raw guests table) ---
raw_rows = []
rid = 0
for g in true_guests:
    n_dupes = random.choices([1, 2, 3], weights=[55, 30, 15], k=1)[0]
    for _ in range(n_dupes):
        fn = g["first_name"]
        ln = g["last_name"]
        if random.random() < 0.25:
            fn = NICKNAMES.get(fn, fn)
        if random.random() < 0.30:
            fn = typo(fn)
        if random.random() < 0.20:
            ln = typo(ln)
        raw_rows.append({
            "record_id": f"r{rid:06d}",
            "source_system": random.choice(SYSTEMS),
            "first_name": fn,
            "last_name": ln,
            "email": vary_email(g["email"]) if random.random() < 0.8 else None,
            "phone": g["phone"] if random.random() < 0.6 else None,
            "postcode": g["postcode"] if random.random() < 0.85 else None,
            "dob": g["dob"] if random.random() < 0.7 else None,
            "home_park": g["home_park"],
            "true_id": g["true_id"],  # hidden ground truth, for evaluation only
        })
        rid += 1

guests_raw = spark.createDataFrame(raw_rows)
(guests_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.guests_raw"))

spark.sql(f"COMMENT ON TABLE {CATALOG}.{SCHEMA}.guests_raw IS "
          f"'Raw guest source records across CRM, web, loyalty and box-office systems. Contains duplicates.'")

print(f"true guests: {len(true_guests):,} | raw source records: {len(raw_rows):,}")
display(spark.sql(
    f"SELECT record_id, source_system, first_name, last_name, email, postcode, true_id "
    f"FROM {CATALOG}.{SCHEMA}.guests_raw WHERE true_id IN (0,1,2) ORDER BY true_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC The rows above show the same true guests appearing several times with name and email variants. That is
# MAGIC the fragmentation the identity spine has to resolve before any graph is meaningful.

# COMMAND ----------

# Bookings, parties and transactions, keyed by SOURCE RECORD ids (as they would really arrive).
# A party is a group of true guests attending together; one member is the payer. We pick a random source
# record for each participant, mimicking that each booking arrives with whatever id that channel held.
raw_by_true = {}
for r in raw_rows:
    raw_by_true.setdefault(r["true_id"], []).append(r["record_id"])

booking_rows, party_member_rows, txn_rows = [], [], []
for pid in range(600):
    size = random.choices([1, 2, 3, 4, 5], weights=[20, 30, 25, 15, 10], k=1)[0]
    members_true = random.sample(range(N_TRUE_GUESTS), size)
    payer_true = members_true[0]
    park = random.choice(PARKS)
    booking_id = f"b{pid:06d}"
    payer_rec = random.choice(raw_by_true[payer_true])
    booking_rows.append({"booking_id": booking_id, "party_id": f"p{pid:06d}",
                         "payer_record_id": payer_rec, "park": park,
                         "booking_date": fakers["en_GB"].date_this_decade().isoformat()})
    for mt in members_true:
        rec = random.choice(raw_by_true[mt])
        party_member_rows.append({"party_id": f"p{pid:06d}", "record_id": rec, "booking_id": booking_id})
        # in-park spend for each attendee on this visit
        txn_rows.append({"txn_id": f"t{len(txn_rows):07d}", "record_id": rec, "park": park,
                         "amount_gbp": round(random.gammavariate(2.0, 25.0), 2),
                         "txn_date": fakers["en_GB"].date_this_decade().isoformat()})

for name, rows in [("bookings", booking_rows), ("party_members", party_member_rows), ("transactions", txn_rows)]:
    (spark.createDataFrame(rows).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
print("wrote bookings, party_members, transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entity resolution with Splink: collapse duplicates into golden guest IDs
# MAGIC
# MAGIC Splink is a probabilistic record-linkage engine (Fellegi-Sunter). It is **inspectable**: every match has a
# MAGIC weight you can trace to the fields that drove it, which is exactly what an ER-sceptical audience asked for.
# MAGIC We run it on the DuckDB backend for a self-contained demo. At production scale you would use the Spark backend
# MAGIC or the Zingg-based Customer Entity Resolution accelerator.
# MAGIC
# MAGIC > If a Splink comparison or training call errors, it is almost always a version difference. Check the call
# MAGIC > against the installed Splink version (`import splink; splink.__version__`). The structure below targets
# MAGIC > Splink 4.

# COMMAND ----------

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, DuckDBAPI, block_on

pdf = (spark.table(f"{CATALOG}.{SCHEMA}.guests_raw")
       .select("record_id", "first_name", "last_name", "email", "phone", "postcode", "dob")
       .toPandas())
pdf = pdf.rename(columns={"record_id": "unique_id"})

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("email"),
        block_on("postcode", "last_name"),
        block_on("phone"),
    ],
    comparisons=[
        cl.ForenameSurnameComparison("first_name", "last_name"),
        cl.EmailComparison("email"),
        cl.PostcodeComparison("postcode") if hasattr(cl, "PostcodeComparison") else cl.ExactMatch("postcode"),
        cl.ExactMatch("phone").configure(term_frequency_adjustments=True),
        cl.ExactMatch("dob"),
    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
linker = Linker(pdf, settings, db_api=db_api)

# Train the model.
deterministic_rules = [block_on("email"), block_on("phone"), block_on("postcode", "last_name")]
linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)
linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
for rule in [block_on("email"), block_on("postcode", "last_name")]:
    linker.training.estimate_parameters_using_expectation_maximisation(rule)

# Predict pairwise matches, then cluster into golden entities.
df_predict = linker.inference.predict(threshold_match_probability=0.8)
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predict, threshold_match_probability=0.9)
clusters_pdf = clusters.as_pandas_dataframe()  # columns include unique_id and cluster_id

resolved = (spark.createDataFrame(clusters_pdf[["unique_id", "cluster_id"]])
            .withColumnRenamed("unique_id", "record_id")
            .withColumnRenamed("cluster_id", "golden_id"))
(resolved.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.guests_resolved"))

n_records = resolved.count()
n_golden = resolved.select("golden_id").distinct().count()
print(f"{n_records:,} source records collapsed into {n_golden:,} golden guests "
      f"(compression {n_records / n_golden:.2f}x). True count was {N_TRUE_GUESTS:,}.")
display(resolved.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Quick sanity check** against the hidden ground truth: how often do records that share a true identity end
# MAGIC up in the same golden cluster? (For demonstration only; in production you would use Splink's own quality
# MAGIC diagnostics and a labelled sample.)

# COMMAND ----------

eval_df = spark.sql(f"""
    WITH j AS (
      SELECT r.record_id, r.true_id, g.golden_id
      FROM {CATALOG}.{SCHEMA}.guests_raw r
      JOIN {CATALOG}.{SCHEMA}.guests_resolved g USING (record_id)
    )
    SELECT
      count(*) AS true_groups,
      round(avg(CASE WHEN n_golden = 1 THEN 1 ELSE 0 END) * 100, 1) AS pct_true_groups_fully_merged
    FROM (SELECT true_id, count(DISTINCT golden_id) AS n_golden FROM j GROUP BY true_id)
""")
display(eval_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the relationship graph with GraphFrames
# MAGIC
# MAGIC Now the part SQL and Genie cannot do well. We build a property graph on the **golden** guests:
# MAGIC - **attended_with** edges connect guests who shared a party (the activity cohort signal).
# MAGIC - **paid_for** edges connect the payer to the other attendees (the "my dad paid" relationship).
# MAGIC
# MAGIC Then we run **connected components** to find activity cohorts, and **PageRank** to rank influence within
# MAGIC the network. This is native, batch graph analytics on Databricks via GraphFrames.

# COMMAND ----------

from graphframes import GraphFrame
from pyspark.sql import functions as F

# Map every booking participant to its golden id.
pm = (spark.table(f"{CATALOG}.{SCHEMA}.party_members")
      .join(spark.table(f"{CATALOG}.{SCHEMA}.guests_resolved"), "record_id")
      .select("party_id", "booking_id", "golden_id").distinct())

# attended_with: pairs of golden guests in the same party (undirected, stored both directions).
a = pm.alias("a")
b = pm.alias("b")
attended = (a.join(b, (F.col("a.party_id") == F.col("b.party_id")) & (F.col("a.golden_id") < F.col("b.golden_id")))
            .select(F.col("a.golden_id").alias("src"), F.col("b.golden_id").alias("dst"))
            .withColumn("relationship", F.lit("attended_with")))

# paid_for: payer golden id -> each attendee golden id.
payer = (spark.table(f"{CATALOG}.{SCHEMA}.bookings")
         .join(spark.table(f"{CATALOG}.{SCHEMA}.guests_resolved"),
               F.col("payer_record_id") == F.col("record_id"))
         .select("party_id", F.col("golden_id").alias("payer_golden")))
paid = (payer.join(pm, "party_id")
        .where(F.col("payer_golden") != F.col("golden_id"))
        .select(F.col("payer_golden").alias("src"), F.col("golden_id").alias("dst"))
        .withColumn("relationship", F.lit("paid_for")))

edges = attended.unionByName(paid).distinct()
vertices = spark.table(f"{CATALOG}.{SCHEMA}.guests_resolved").select(F.col("golden_id").alias("id")).distinct()

g = GraphFrame(vertices, edges)

# Activity cohorts via connected components (undirected reachability over attended_with + paid_for).
# Checkpointing (default interval) truncates iteration lineage; the checkpoint dir is set above.
cohorts = g.connectedComponents().withColumnRenamed("component", "cohort_id")
(cohorts.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.guest_cohorts"))

# Influence within the network via PageRank.
ranks = g.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "pagerank")
(ranks.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.guest_influence"))

print(f"cohorts found: {cohorts.select('cohort_id').distinct().count():,}")
display(spark.sql(f"""
  SELECT cohort_id, count(*) AS cohort_size
  FROM {CATALOG}.{SCHEMA}.guest_cohorts GROUP BY cohort_id ORDER BY cohort_size DESC LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Value attribution: which cohorts drive park spend
# MAGIC
# MAGIC This is Simon's worked example. We flag a sample of guests as having received a "free annual pass", then
# MAGIC compare downstream park spend of the cohorts that contain a pass holder against those that do not. All of
# MAGIC this is plain SQL over the golden entities and the graph-derived cohort table.

# COMMAND ----------

# Attach golden id + cohort to each transaction, and simulate a free-pass campaign on ~5% of guests.
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.cohort_spend AS
WITH txn_g AS (
  SELECT t.txn_id, t.amount_gbp, c.cohort_id, r.golden_id
  FROM {CATALOG}.{SCHEMA}.transactions t
  JOIN {CATALOG}.{SCHEMA}.guests_resolved r USING (record_id)
  JOIN {CATALOG}.{SCHEMA}.guest_cohorts c ON c.id = r.golden_id
),
pass_holders AS (
  SELECT id AS golden_id FROM {CATALOG}.{SCHEMA}.guest_cohorts
  WHERE abs(hash(id)) % 100 < 5
),
cohort_flag AS (
  SELECT DISTINCT c.cohort_id
  FROM {CATALOG}.{SCHEMA}.guest_cohorts c JOIN pass_holders p ON p.golden_id = c.id
)
SELECT
  g.cohort_id,
  CASE WHEN f.cohort_id IS NOT NULL THEN 'has_pass_holder' ELSE 'no_pass_holder' END AS segment,
  count(DISTINCT g.id) AS cohort_members,
  round(sum(coalesce(txn_g.amount_gbp, 0)), 2) AS total_spend_gbp
FROM {CATALOG}.{SCHEMA}.guest_cohorts g
LEFT JOIN txn_g ON txn_g.cohort_id = g.cohort_id AND txn_g.golden_id = g.id
LEFT JOIN cohort_flag f ON f.cohort_id = g.cohort_id
GROUP BY g.cohort_id, segment
""")

display(spark.sql(f"""
  SELECT segment,
         count(*) AS cohorts,
         round(avg(total_spend_gbp), 2) AS avg_cohort_spend_gbp,
         round(avg(total_spend_gbp / nullif(cohort_members,0)), 2) AS avg_spend_per_member_gbp
  FROM {CATALOG}.{SCHEMA}.cohort_spend
  GROUP BY segment ORDER BY segment
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The Genie link: same tables, two complementary query modes
# MAGIC
# MAGIC Everything above lives in Unity Catalog as governed Delta tables. Genie reads the **Genie Ontology** (a
# MAGIC semantic grounding layer built from metric views, table and column comments, Pages and usage) to answer
# MAGIC natural-language **metric** questions. It is not a graph engine and does not traverse relationships; the
# MAGIC GraphFrames layer above is what answers the connection questions. Both read the same UC definitions, so you
# MAGIC define the semantics once.
# MAGIC
# MAGIC Below we publish a simple aggregation view with comments for Genie to ground on. In production you would
# MAGIC formalise the KPIs as Unity Catalog **metric views** so Genie prefers the certified definitions.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_cohort_value (
  cohort_id COMMENT 'Activity cohort identifier (a graph connected component).',
  segment COMMENT 'Whether the cohort contains a free-pass holder.',
  cohort_members COMMENT 'Number of distinct golden guests in the activity cohort.',
  total_spend_gbp COMMENT 'Total in-park spend attributed to the cohort, in GBP.'
)
COMMENT 'Activity cohorts with member count and total park spend. A cohort is a group of guests connected by shared bookings and payments (from the graph).'
AS
SELECT cohort_id, segment, cohort_members, total_spend_gbp
FROM {CATALOG}.{SCHEMA}.cohort_spend
""")
print("published v_cohort_value for Genie grounding")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Genie space specification (build this over the tables above)
# MAGIC
# MAGIC **Include tables:** `guests_resolved`, `bookings`, `party_members`, `transactions`, `guest_cohorts`,
# MAGIC `guest_influence`, and the view `v_cohort_value`.
# MAGIC
# MAGIC **Questions Genie answers well (tabular, over the ontology):**
# MAGIC - "What is the total in-park spend by park last year?"
# MAGIC - "Average spend per member for cohorts that contain a free-pass holder versus those that do not?"
# MAGIC - "How many golden guests do we have, and how many source records collapsed into them?"
# MAGIC
# MAGIC **Questions the graph answers (traversal, not Genie):**
# MAGIC - "Starting from free-pass holders, which activity cohorts do they belong to, and rank those cohorts by the
# MAGIC   downstream spend of connected members." (connected components + join)
# MAGIC - "Who are the most influential guests within the highest-spending cohorts?" (PageRank)
# MAGIC - "Find guests within two hops of this guest through shared bookings." (multi-hop traversal)
# MAGIC
# MAGIC The graph results (cohort ids, influence scores) are written back to Delta as tables, so Genie can then
# MAGIC answer aggregation questions over the graph-derived cohorts. That is the round trip: the graph produces the
# MAGIC connections, Genie reports over them, all on the same lakehouse.
# MAGIC
# MAGIC *(To create this Genie space programmatically, use the `databricks-genie-agents` skill against your
# MAGIC workspace and SQL warehouse.)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Closing: how this maps to the build-vs-buy roadmap
# MAGIC
# MAGIC - **Phase 0 (this notebook):** entity resolution (Splink) plus a network graph (GraphFrames) on the
# MAGIC   existing spine, results back in Delta, explored through Genie. Native, no data copy, no new product. This
# MAGIC   is the "80 to 90 percent, quickly" proof.
# MAGIC - **Phase 1:** for **interactive** traversal at scale (ad-hoc "explore this cohort, add a property, ask what
# MAGIC   people like this do"), GraphFrames is batch, so add a no-copy interactive graph layer: Stardog (semantic,
# MAGIC   W3C, federated) or PuppyGraph (lakehouse-native openCypher), and harden the OntoBricks ontology pattern,
# MAGIC   anchored on the same Unity Catalog semantics that back the Genie Ontology.
# MAGIC - **Phase 2:** CustomerLake AIR when it is GA and proven; a materialised serving graph only if a genuine
# MAGIC   low-latency hot path returns.
# MAGIC
# MAGIC **What changes at production scale (tens of millions of profiles):** run Splink on its Spark backend or use
# MAGIC the Zingg-based Customer Entity Resolution accelerator instead of DuckDB; GraphFrames connected-components
# MAGIC scales to that range on a suitable cluster but should be validated; and the interactive exploration moves to
# MAGIC the Phase 1 graph engine. The multilingual (Japanese, Korean, Arabic, Russian) matching in the data here is
# MAGIC deliberate: it is where tuning matters most and where CustomerLake AIR's English-name sweet spot is weakest.
# MAGIC
# MAGIC **The one-line message:** Genie and metric views give you the meaning, the graph gives you the connections,
# MAGIC and both read the same Unity Catalog definitions, so you define your ontology once and you are not building
# MAGIC a Frankenstein.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: validation summary (for a headless run)
# MAGIC Compact counts so the notebook can be validated when run as a job. Harmless when run interactively.

# COMMAND ----------

import json
_summary = {
    "raw_source_records": spark.table(f"{CATALOG}.{SCHEMA}.guests_raw").count(),
    "golden_guests": spark.table(f"{CATALOG}.{SCHEMA}.guests_resolved").select("golden_id").distinct().count(),
    "activity_cohorts": spark.table(f"{CATALOG}.{SCHEMA}.guest_cohorts").select("cohort_id").distinct().count(),
    "cohort_spend_rows": spark.table(f"{CATALOG}.{SCHEMA}.cohort_spend").count(),
}
print(_summary)
dbutils.notebook.exit(json.dumps(_summary))
