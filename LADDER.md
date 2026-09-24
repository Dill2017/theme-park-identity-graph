# The Roadmap to Graph: a native ladder

This repo has two notebooks that build on each other:

1. `identity_spine_to_graph_demo.py` builds the golden identity spine and a first
   relationship graph (the "Phase 0" story). Start there.
2. `graph_ladder_walkthrough.py` takes those tables and walks up the **build-vs-buy
   ladder**, rung by rung, all natively on Databricks. This file explains that ladder.

---

## The idea: climb only when a question forces you to

Most "we need a graph database" asks are really multi-hop-query and agent-context asks.
So the field approach is a **complexity ladder**: start at the lowest rung that answers
your benchmark questions, and only climb to the next rung with a concrete, demonstrated
reason. **You buy an external graph engine only when a named question the rungs below
cannot answer proves you need one** (for example a genuine sub-second, high-concurrency
traversal need).

That framing turns "build or buy" from a guess into a gated decision, and it is the
surest way to avoid a dead-end you have to rebuild later.

## The one thing that ties every rung together: one edge table

The graph is not a separate database, it is an **edge table** plus properties. This demo
standardises on an 8-column contract, `gold_triplets`:

```
subject_id, subject_type, predicate, object_id, object_type, confidence, source_method, source_agent
```

Produce that table once off the identity spine, and **every rung consumes it unchanged**:
Genie, graph analytics, recursive-SQL traversal, an explorer app, an agent, and even an
external engine you might later buy all read the same table. That is what keeps the path
reversible and free of dead-ends.

In this demo the table carries three edge types built from the theme-park data:

| Predicate | Meaning | Built from |
|---|---|---|
| `RESOLVES_TO` | a raw source record resolves to a golden guest (the identity seed) | Splink output (`guests_resolved`) |
| `VISITED_WITH` | two golden guests appeared in the same party | `party_members` |
| `PAID_FOR` | the booking payer paid for another party member | `bookings` + `party_members` |

## The rungs this notebook demonstrates

| Rung | What it is | How it is shown here | Compute |
|---|---|---|---|
| **0** | **Managed semantics.** Metric views, glossary, Genie ontology. Check first. | A governed metric view over cohort value that Genie reads. | SQL warehouse / DBR 17+ |
| **1** | **Edge table into Genie.** | Submit `gold_triplets` plus enriched-edge and node-degree views to a Genie space so it answers relationship questions. | SQL warehouse / DBR 17+ |
| **4** | **Native traversal.** Multi-hop, no graph database. | Recursive SQL (`WITH RECURSIVE`) k-hop reachability, plus a pre-generated 2-hop table for Genie. | SQL warehouse / DBR 17+ |
| **6** | **Graph algorithms.** | GraphFrames over the same edge table: community detection (cohorts), centrality (influence), triangle counting, motif finding. | Classic cluster + GraphFrames (Maven) |

Rungs 2, 3, 5, 7, 8 and 9 (Knowledge-Assistant context, the full agentic knowledge-graph
consumption layer, ontology engines such as OntoBricks, GraphRAG, extreme scale, and graph
neural networks) exist and are also largely native. They are out of scope for this
walkthrough, which focuses on the rungs that answer the cohort, influence, traversal and
value-attribution questions most operators actually start with.

## A note on compute (it is part of the story)

- **Rungs 0, 1 and 4 are pure SQL.** Recursive CTEs (`WITH RECURSIVE`) are generally
  available from **Databricks Runtime 17.0**, so run these on a **SQL warehouse / serverless
  SQL, or a DBR 17.0+ cluster**. They will not run on older runtimes.
- **Rung 6 uses GraphFrames**, which needs the Maven library on a **classic cluster** and
  does not run on serverless.

So a realistic demo uses a SQL warehouse for Rungs 0/1/4 and a GraphFrames cluster for
Rung 6, both reading the one `gold_triplets` table. That split is not an accident; it
reflects where each capability runs today.

## The payoff

Everything here runs natively on Databricks, on one edge table, with no graph database.
Databricks stays the system of record, the identity layer, and the governance and semantic
plane. You only reach for an external engine at the rung where a benchmark question proves
you need it, and because every rung reads the same table, that choice stays reversible.
