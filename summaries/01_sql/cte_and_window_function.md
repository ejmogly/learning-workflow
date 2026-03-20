# Summary - CTE and Window Functions for Data Analytics Engineering

## 1. Overview
- Date: 2026-03-21
- Category: SQL
- Source: Self-structured learning workflow
- Status: In Progress
- Target Role: DAE (Mart) / DAE (CPC Mart)

---

## 2. What I Learned
This section captures the core concepts I learned from this topic.

- CTEs (Common Table Expressions) help break complex SQL logic into smaller and more readable steps.
- Window functions allow me to calculate row-level metrics while keeping the original granularity of the dataset.
- Functions such as `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `SUM() OVER()`, `AVG() OVER()`, and `LAG()` are especially useful for analytics engineering use cases.
- CTEs improve maintainability when building transformation logic for marts, especially when multiple intermediate steps are required.
- Window functions are essential for deduplication, sequencing, ranking, cumulative metrics, period-over-period comparison, and event flow analysis.
- Key takeaway: CTEs make SQL easier to structure, and window functions make SQL more powerful for reusable analytics logic without collapsing the dataset too early.

---

## 3. What I Built
This section describes what I actually created, practiced, or implemented.

- Practiced writing multi-step SQL using chained CTEs for transformation and aggregation.
- Used `ROW_NUMBER()` to identify duplicate or latest records by partition.
- Used `SUM() OVER()` and `AVG() OVER()` to calculate cumulative and rolling metrics without changing row-level detail.
- Organized SQL patterns that can be reused in mart-building, KPI tracking, and validation logic.
- Began treating SQL as a transformation layer rather than only a query language for one-time analysis.

---

## 4. Why It Matters
This section explains why this topic is important in real-world work.

### Business Value
- These SQL patterns help create more accurate and explainable KPI datasets for reporting and decision-making.
- They make it easier to answer business questions such as latest status, top contributors, retention sequence, and period changes.

### Technical Value
- CTEs improve readability and reduce the risk of writing overly nested or hard-to-review SQL.
- Window functions enable advanced calculations without losing dataset granularity, which is critical in event-level and transaction-level transformations.

### Operational Value
- Reusable CTE and window function patterns make data pipelines easier to review, debug, and maintain.
- They also support validation logic such as duplicate detection, latest-record selection, and consistency checks in mart generation.

---

## 5. How It Connects to My Past Work
This section connects the learning topic to my previous work experience.

- Related past project: KPI dashboards, funnel analysis, retention reporting, and DW/DM-oriented reporting support
- Similar problem I solved before: I previously needed to identify key user behaviors, aggregate performance by period, and structure logic for reporting-ready datasets.
- What is newly strengthened now: I am strengthening the ability to write cleaner and more reusable SQL that supports data mart construction, validation logic, and scalable analytics use cases.

---

## 6. Resume Bullet Draft
This section turns the learning outcome into resume-ready bullet points.

- Strengthened SQL transformation capabilities by applying CTEs and window functions to deduplication, ranking, cumulative metrics, and reusable mart-oriented logic.
- Improved SQL maintainability and readability by structuring multi-step analytics logic into reviewable CTE-based workflows.
- Expanded analytics engineering skills by using window functions for event-level sequencing, latest-state extraction, and KPI-ready dataset preparation.

---

## 7. Interview Talking Points
This section helps me explain the topic clearly in an interview.

### Why I studied this
- I studied CTEs and window functions because they are fundamental tools for writing reusable and maintainable SQL in data warehouse and mart environments.

### What problem it solves
- They solve the problem of turning complex business logic into structured SQL that is easier to review, debug, and scale across analytics use cases.

### What changed in my capability
- I became more intentional about writing SQL in layers and using row-level analytical functions to support richer transformations without sacrificing structure or readability.

---

## 8. Evidence
This section links to related files and outputs.

- SQL file: sql/cte_and_window_functions.sql
- Python file:
- Notebook:
- Diagram:
- Screenshot:
- Related repo path: summaries/01_sql/cte_and_window_functions.md

---

## 9. Next Step
This section defines the next learning step.

- Next topic 1: Validation SQL patterns for duplicates, null checks, and reconciliation
- Next topic 2: Mart-oriented SQL design with source / intermediate / output layers
- Next project idea: Build a small event mart using CTEs, deduplication logic, and window functions for latest status and cumulative metrics
