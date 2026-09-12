# Snowflake Time Travel: Simulating and Recovering from a Bad Update

A deliberate, end-to-end test of Snowflake's Time Travel feature: corrupt a production table on purpose, then recover it using query history and Time Travel rather than a traditional backup — because there wasn't one, and that's the point.

## Why this exists

Clustering keys and `COPY INTO` loading prove you can move and organize data in Snowflake. They don't prove you can recover when something goes wrong. This write-up is that proof: a real incident, simulated on purpose, with the actual before/after state captured at each step.

## Setup — capture a known-good baseline

Before touching anything, the table's state was captured so any change afterward would be provable, not just claimed:

```sql
SELECT COUNT(*) AS row_count, SUM(sales) AS total_sales
FROM tributary.analytics.fct_orders;
```

| row_count | total_sales |
|---|---|
| 9,994 | $2,297,200.86 |

*(Screenshot 1 — baseline query result)*

## The incident — a destructive update, on purpose

A change that mimics a real mistake — someone runs an `UPDATE` without a `WHERE` clause, or with the wrong one:

```sql
UPDATE tributary.analytics.fct_orders
SET sales = 0;
```

Confirming the damage:

```sql
SELECT COUNT(*) AS row_count, SUM(sales) AS total_sales
FROM tributary.analytics.fct_orders;
```

| row_count | total_sales |
|---|---|
| 9,994 | $0.00 |

Row count unchanged, every dollar of sales gone. In a system without Time Travel or backups, this is unrecoverable the moment the transaction commits.

*(Screenshot 2 — damage confirmed)*

## Detection and recovery — proving the old data still exists

Snowflake retains the pre-change state of a table for a configurable retention window (default: 1 day, up to 90 on Enterprise), addressable by timestamp, offset, or query ID. Rather than guessing at a timestamp, the exact query ID of the destructive `UPDATE` was pulled from the query history and used directly:

```sql
SELECT COUNT(*) AS row_count, SUM(sales) AS total_sales
FROM tributary.analytics.fct_orders
BEFORE (STATEMENT => '<query_id_of_the_update>');
```

| row_count | total_sales |
|---|---|
| 9,994 | $2,297,200.86 |

This is the key proof point: the data was never gone. Snowflake was still holding the pre-`UPDATE` version of the table, addressable with a single clause, with no restore job and no separate backup system involved.

*(Screenshot 3 — BEFORE query proving the old data is intact)*

## Restoring the table

With the recovery point confirmed, it was used to rebuild the table itself:

```sql
CREATE OR REPLACE TABLE tributary.analytics.fct_orders AS
SELECT *
FROM tributary.analytics.fct_orders
BEFORE (STATEMENT => '<query_id_of_the_update>');
```

```sql
SELECT COUNT(*) AS row_count, SUM(sales) AS total_sales
FROM tributary.analytics.fct_orders;
```

| row_count | total_sales |
|---|---|
| 9,994 | $2,297,200.86 |

Numbers match the original baseline exactly.

*(Screenshot 4 — restored table matching baseline)*

## Cleaning up after the recovery

`CREATE OR REPLACE TABLE` rebuilds the table from scratch, which also drops the clustering key that had been set on `order_date`. Rather than re-applying the clustering key by hand, the table was rebuilt properly through its normal path — `dbt run` — so the model definition (clustering key included) is the single source of truth for what the table should look like, not a one-off manual fix:

```bash
dbt run --select fct_orders
```

```sql
SHOW TABLES LIKE 'FCT_ORDERS' IN SCHEMA tributary.analytics;
-- cluster_by: LINEAR(order_date)
```

*(Screenshot 5 — clean dbt run, clustering key confirmed restored)*

## Takeaways

- **Time Travel is not a substitute for backups on its own** — it's bounded by the retention window (1–90 days depending on edition), and it protects against accidental damage, not against someone deliberately trying to destroy data within that window. It's a fast, no-infrastructure first line of defense for exactly the kind of mistake simulated here.
- **`BEFORE (STATEMENT => ...)` beats guessing at a timestamp.** Anchoring the recovery to the specific query ID that caused the damage removed any ambiguity about which point in time was "before."
- **A destructive rebuild (`CREATE OR REPLACE TABLE ... AS SELECT`) resets table-level metadata** like clustering keys. Recovering data and restoring the table's full intended state are two different steps — worth remembering before assuming a restore is complete.
- **What I'd add for production:** a monitoring check on business-critical aggregates (e.g., alert if `SUM(sales)` drops more than some threshold day-over-day) so an incident like this is caught in minutes instead of whenever someone happens to look at a dashboard, plus a documented, tested runbook for this exact recovery pattern rather than reconstructing it under pressure.
