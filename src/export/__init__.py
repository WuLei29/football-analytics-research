"""Web export: gold + silver -> static JSON files under web/public/data.

Spec: md/WEB_DATA.md. Architecture decision: md/WEB_PLAN.md §2.

Read-only on Postgres. Whole-tree rewrite, idempotent, atomic per file.
"""
