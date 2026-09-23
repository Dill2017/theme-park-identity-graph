# Theme Park Identity Spine to Knowledge Graph

A self-contained Databricks notebook that turns a messy pile of duplicate guest
records into a **golden identity spine** and then a **relationship graph**, and
shows how the same data answers both metric questions (through Genie) and
connection questions (through the graph). It runs end to end on synthetic data
for a fictional theme-park operator, with no data leaving the lakehouse.

New to graphs? Read **[GRAPH_INTUITION.md](GRAPH_INTUITION.md)** first. It explains,
from zero, what a graph is, why you would build one here, and how this one is put
together. This README is the technical overview.

---

## What it is

A single Databricks notebook, `identity_spine_to_graph_demo.py`, written in the
Databricks source-notebook format so it imports straight into a workspace. It is a
"Phase 0" demonstration: the fastest, fully native way to get most of the value of a
customer relationship graph, using tools that are available today.

It uses:

- **[Splink](https://github.com/moj-analytical-services/splink)** for probabilistic
  entity resolution (collapsing duplicate records into one golden guest).
- **[GraphFrames](https://graphframes.io/)** for graph analytics (activity cohorts
  and influence) on top of the resolved guests.
- **Unity Catalog** Delta tables as the store, and a governed view for
  **[Genie](https://docs.databricks.com/aws/en/genie/genie-ontology)** to answer
  metric questions.

## What it shows

The notebook walks through five stages, each writing its output back to Delta:

1. **Generate synthetic data.** Guests with deliberate duplicates and multilingual
   names (English, Japanese, Korean, Arabic, Russian), plus bookings, parties (who
   attended together), payers, and in-park transactions. Bookings are keyed by the
   raw source-record id, exactly as data arrives in real life.
2. **Entity resolution (Splink).** Collapse the duplicate source records into stable
   golden guest ids, with an inspectable match weight rather than a black box.
3. **Relationship graph (GraphFrames).** Build a graph whose nodes are golden guests
   and whose edges are "attended together" and "paid for", then run connected
   components to find **activity cohorts** and PageRank to find **influencers**.
4. **Value attribution.** Simulate a free-pass campaign and compare downstream park
   spend of cohorts that contain a pass holder against those that do not.
5. **The Genie link.** Publish a governed view so Genie can answer the metric
   questions (average spend per cohort), while the graph answers the connection
   questions (who influences whom). Both read the same Unity Catalog definitions.

## Example result

A representative run on the default sample size:

| Stage | Count |
|---|---|
| Raw source records (with duplicates) | ~1,900 |
| Golden guests after entity resolution | ~1,350 |
| Activity cohorts (connected components) | ~530 |

The `N_TRUE_GUESTS` constant near the top scales the sample up or down.

---

## How to run it

1. **Import the notebook** into a Databricks workspace (`identity_spine_to_graph_demo.py`,
   Import as a notebook, or `databricks workspace import`).
2. **Attach a classic cluster** running a Spark 3.5 runtime (for example DBR 15.4 LTS),
   with the **GraphFrames** library installed as a Maven library:
   `graphframes:graphframes:0.8.4-spark3.5-s_2.12`, repo `https://repos.spark-packages.org/`.
   GraphFrames needs the JVM package, so the Python `pip` package alone is not enough,
   and it does not run on serverless.
3. **Set the widgets** at the top: `catalog` (a catalog you can write to) and `schema`
   (defaults to `identity_graph_demo`).
4. **Run all.** The first cell installs `faker`, `splink`, and `duckdb` with `%pip`.

### Requirements

- A Databricks workspace with Unity Catalog and a classic cluster (Spark 3.5).
- GraphFrames attached as a Maven library (see above).
- Python packages `faker`, `splink` (v4), `duckdb`, installed by the notebook.

### A note on the graph checkpoint

GraphFrames connected components needs a reliable Spark checkpoint location to keep
its iteration lineage from growing. The notebook sets it to DBFS scratch
(`dbfs:/tmp/gf_checkpoints`). A local disk path is unreliable for Spark checkpointing
and can cause intermittent stalls, and a Unity Catalog Volume path is not accepted as
a checkpoint dir on a single-user cluster.

---

## Files

| File | Purpose |
|---|---|
| `identity_spine_to_graph_demo.py` | The Databricks notebook (source format). |
| `GRAPH_INTUITION.md` | Plain-language explanation of the graph, for any reader. |
| `README.md` | This overview. |

## What it is not

This is a demonstration on synthetic data, sized for a quick, reliable run. It is the
"build it natively now" starting point. At production scale you would run Splink on its
Spark backend or the Databricks Customer Entity Resolution accelerator, and for
interactive traversal at scale you would add a graph engine such as Stardog or
PuppyGraph. The data is entirely synthetic and represents a fictional theme-park
operator; any resemblance to a real business is coincidental.
