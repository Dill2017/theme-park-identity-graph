# Understanding the graph: the intuition, the purpose, and how it is built

This guide assumes **no prior experience with graphs**. If you have never heard the
word "graph" used this way, you are in exactly the right place. We will build the idea
up slowly, using a theme park as the running example, and by the end you will
understand what the notebook in this repository does and why.

No code is required to read this. There are small diagrams drawn with plain text.

---

## 1. What is a graph, really

Forget bar charts and line charts. In this world, a **graph** is just a way of writing
down **things** and the **connections between them**.

- The things are called **nodes** (you will also hear "vertices").
- The connections are called **edges**.

That is the whole idea. A social network is a graph: the people are nodes, and
"is friends with" is an edge. A map is a graph: cities are nodes, roads are edges.

Here is a tiny one, three people and who knows whom:

```
   Ana ────── Ben
    │
    │
   Chi
```

Ana is connected to Ben. Ana is connected to Chi. Ben and Chi are not directly
connected. That is a graph with three nodes and two edges.

A node can carry **properties** (facts about it): Ana's home city, her age, how much
she spent. An edge can also carry properties: when Ana and Ben went to the park
together, or who paid.

## 2. Why a normal table is not enough

Most data lives in tables: rows and columns, like a spreadsheet.

```
guest_id | name | home_city | total_spend
---------+------+-----------+------------
   1     | Ana  | London    |   120
   2     | Ben  | Leeds     |    80
```

Tables are excellent at questions about **one row at a time**, or **totals**:
"What did Ana spend?" or "What is the average spend by city?"

They are bad at questions about **how rows connect to each other**, especially when the
connection can be many steps long:

- "Who does Ana go to the park with, and who do *those* people go with?"
- "Which small groups of guests always visit together?"
- "Who is the most influential person in a group?"

To answer those with tables you end up joining a table to itself again and again, once
per step, and it gets slow and painful fast. A graph is built for exactly these
"follow the connections" questions.

## 3. The two graphs (this is the key idea)

People say "identity graph" and "knowledge graph" and it gets confusing. In practice
there are **two different graphs**, and it helps to keep them separate.

### Graph 1: the identity graph (who is who)

Real data is messy. The same person shows up many times under slightly different
details, because they booked on the web once, called the box office another time, and
joined the loyalty scheme with a different email.

```
"Ana Smith"      ana.smith@mail.com     London
"A. Smith"       ana.s@mail.com         London
"Ana Smyth"      ana.smith@mail.com     Londin (typo)
```

Are these three records three people, or one person entered three times? The
**identity graph** answers "who is who". It connects records that are probably the same
real person, and collapses them into a single **golden** guest. This step is called
**entity resolution**.

### Graph 2: the relationship graph (how people connect)

Once you know each real guest as one golden identity, you can ask how those guests
relate to each other. Who books together, who pays for whom, who ends up in the same
group. That is the **relationship graph**, and it is where the interesting questions
live.

The important sentence: **the identity graph is the foundation, and the relationship
graph is the value built on top of it.** You cannot ask "who does Ana go with" until
you are confident who Ana is.

## 4. The theme-park story: why we want this at all

Imagine you run several theme parks. You want to move beyond "who is this guest" to
questions like:

- Guests rarely visit alone. Someone explores the idea, someone books, someone pays,
  and a group actually attends. **Who travels together?** That group is more valuable
  to understand than any single person.
- **Who influences whom?** If one person in a group is the reason five others come, that
  person is worth a lot to you.
- **Where does value come from?** Last year you gave some guests a free annual pass.
  Which *groups* of guests, with which characteristics, then spent more in the parks?

None of these are one-guest questions. They are all "how are guests connected"
questions. That is why you build a relationship graph.

## 5. How this graph is built, step by step

Here is exactly what the notebook does, in plain terms.

### Step 1: make some realistic mess

We generate synthetic guests, then create several noisy copies of each one (typos,
nicknames, a different email, a missing field), across pretend systems (web, loyalty,
box office). We also generate bookings: each booking is a small group of guests, one
of whom is the payer, attending one park. This gives us duplicates to resolve and
relationships to discover, just like real data.

### Step 2: entity resolution (build the identity graph)

We use a tool called **Splink** to compare records and score how likely two records are
the same person, based on how well the names, emails, postcodes and so on agree. Records
that score highly enough get linked, and each linked cluster becomes one **golden guest
id**. Crucially this is **inspectable**: for any match you can see which fields drove the
score, so it is not a black box.

```
raw records                        golden guests
"Ana Smith"   ┐
"A. Smith"    ├──  resolve  ──►     Ana  (golden_id = 1)
"Ana Smyth"   ┘
"Ben Okoro"   ───────────────►     Ben  (golden_id = 2)
```

### Step 3: build the relationship graph

Now we lay down nodes and edges.

- **Nodes:** one per golden guest.
- **Edges:** two kinds.
  - *attended together*: if two guests were in the same booking party.
  - *paid for*: from the payer to each other person in the party.

A booking where Ana and Chi went and Ana's dad paid becomes:

```
        paid for            attended together
  Dad ───────────► Ana ◄──────────────────► Chi
                    ▲                          │
                    └──── paid for ────────────┘
```

We build this with **GraphFrames**, which does graph analytics on top of Spark.

### Step 4: ask the graph two classic questions

**Connected components = activity cohorts.** A "connected component" is just a clump of
nodes that are all reachable from each other by following edges, with no edges leaving
the clump. In our world, a connected component is a group of guests who are linked by
shared bookings and payments: an **activity cohort**. Think of it as automatically
discovering the friend-and-family groups without anyone telling you who is in them.

```
  Ana ── Chi ── Dad        Ben ── Lena
  (cohort A)                (cohort B)
```

**PageRank = influence.** PageRank is the algorithm Google originally used to rank web
pages: a page is important if many important pages link to it. Applied to guests, it
finds the people who are central to their groups, the ones many connections flow
through. High PageRank means "this guest looks like a hub of their cohort".

### Step 5: attribute value, and connect it back to plain questions

We flag some guests as having received a free pass, then compare the total park spend of
cohorts that contain a pass holder against cohorts that do not. The graph found the
cohorts; ordinary SQL then measures the money. The cohort ids and influence scores are
written back to Delta as tables, so a normal analyst (or Genie) can answer everyday
metric questions over them.

This is the punchline: **Genie and metrics give you the meaning ("what did this cohort
spend"), and the graph gives you the connections ("who is in the cohort and who leads
it"). Both read the same governed tables, so you define things once.**

---

## 6. A tiny worked example you can follow by hand

Three real people: **Ana**, **Chi**, **Ben**. Two bookings.

- Booking 1: Ana and Chi attend. Ana pays. Park spend: Ana 40, Chi 30.
- Booking 2: Ben attends alone. Ben pays. Park spend: Ben 25.

Edges:
- Ana attended-with Chi.
- Ana paid-for Chi.

Now the graph questions:

- **Cohorts (connected components):** {Ana, Chi} are connected, so they are one cohort.
  Ben is on his own, so he is a second cohort of size one.
- **Influence (PageRank):** within {Ana, Chi}, Ana has the incoming and outgoing edges
  (she paid and attended with Chi), so Ana scores as the more central guest.
- **Value:** cohort {Ana, Chi} spent 70. Cohort {Ben} spent 25. If Ana held a free pass,
  the 70 of downstream spend is attributable to a pass-holding cohort.

That is the entire notebook in miniature. The full version just does this for hundreds
of guests and thousands of bookings, which is where doing it by hand stops being
possible and the graph earns its place.

---

## 7. Why keep it all on the lakehouse

You might ask: why not export everything into a dedicated graph database? Two reasons
this demo stays inside Databricks:

1. **No copying data.** Moving data into a separate system means keeping two copies in
   sync and paying for both. Here the data never leaves the lakehouse.
2. **No tool sprawl.** One governed place for the tables, the identity resolution, the
   graph analytics and the metrics is far easier to run and trust than a chain of
   different systems stitched together.

For very large scale or for fast, interactive "explore this cohort" traversal, you would
add a specialised graph engine on top. But you would keep the lakehouse as the system of
record underneath, and everything in this notebook still applies.

---

## 8. Glossary

- **Node (vertex):** a thing in the graph. Here, a golden guest.
- **Edge:** a connection between two nodes. Here, "attended together" or "paid for".
- **Property:** a fact attached to a node or edge (home city, amount paid).
- **Entity resolution:** deciding which messy records are the same real person, and
  merging them into one golden identity.
- **Golden id:** the single, stable identifier for one real guest after resolution.
- **Identity graph:** the graph that answers "who is who".
- **Relationship graph:** the graph that answers "how are guests connected".
- **Connected component:** a clump of nodes all reachable from each other. Here, an
  activity cohort.
- **PageRank:** a score of how central or influential a node is in the graph.
- **Splink:** the tool used here for probabilistic entity resolution.
- **GraphFrames:** the tool used here for graph analytics on Spark.
- **Lakehouse:** the single governed data platform (Databricks) that holds everything.
