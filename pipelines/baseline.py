"""Baseline pipeline: models analytics-grade MDM.

Last-write-wins upserts, physical deletes, no schema validation, no conflict
resolution, no idempotency, no referential integrity. This is deliberately the
common production posture the paper argues against, not a strawman: each
omitted control corresponds to a widely deployed default.
"""
from __future__ import annotations
import time
from corpus.model import Event


class BaselinePipeline:
    name = "baseline"

    def __init__(self):
        self.state: dict[str, dict[str, dict]] = {
            "category": {}, "store": {}, "item": {}, "assortment": {},
        }
        self.latencies_us: list[float] = []

    @staticmethod
    def _pk(entity_type: str) -> str:
        return {"category": "category_id", "store": "store_id",
                "item": "item_id", "assortment": "assortment_id"}[entity_type]

    def apply(self, ev: Event) -> str:
        t0 = time.perf_counter()
        table = self.state[ev.entity_type]
        pk = self._pk(ev.entity_type)
        if ev.op == "upsert":
            table[ev.payload[pk]] = dict(ev.payload)
            outcome = "applied"
        else:  # delete: physical, no cascade -> dangling references persist
            table.pop(ev.payload[pk], None)
            outcome = "applied"
        self.latencies_us.append((time.perf_counter() - t0) * 1e6)
        return outcome

    def run(self, events: list[Event]) -> None:
        for ev in events:
            self.apply(ev)

    def visible_state(self) -> dict[str, dict[str, dict]]:
        return self.state
