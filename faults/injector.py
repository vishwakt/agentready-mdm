"""Fault injector: corrupts the update phase of a clean event stream.

Eight fault classes (F1-F8), each mapped in the paper to the trust property it
violates. Faults are injected only into the update phase (post initial load) so
both pipelines start from an identical clean base state, and every injected
fault is labeled in Event.injected_faults for ground-truth scoring. Pipelines
never read that field.

Rates are expressed as the fraction of update-phase events that carry a fault.
The class mix is fixed (weights below) so runs at different rates are comparable.
"""
from __future__ import annotations
import copy
import random
from corpus.model import Event, Corpus, TEMPERATURE_CLASSES

FAULT_WEIGHTS = {
    "F1_duplicate_entity": 0.15,   # same real-world item under a new ID
    "F2_orphan_reference": 0.15,   # item points to a deleted/unknown category
    "F3_conflicting_values": 0.15, # two sources disagree within a tight window
    "F4_stale_record": 0.13,       # re-emit an old version as if current
    "F5_schema_violation": 0.12,   # wrong type / unknown enum value
    "F6_missing_mandatory": 0.10,  # drop a mandatory field
    "F7_out_of_order": 0.10,       # deliver an older update after a newer one
    "F8_duplicate_delivery": 0.10, # exact re-delivery (retry storm)
}
assert abs(sum(FAULT_WEIGHTS.values()) - 1.0) < 1e-9


def _clone(ev: Event) -> Event:
    return copy.deepcopy(ev)


def inject(corpus: Corpus, rate: float, seed: int) -> list[Event]:
    """Return a new event list with faults injected at `rate` into the update phase."""
    rng = random.Random(seed * 1000003 + int(rate * 100))
    events = [copy.deepcopy(e) for e in corpus.events]
    load_len = sum(1 for e in events if True and e.seq <= 0) or _load_boundary(corpus)
    update_idx = [i for i, e in enumerate(events) if i >= load_len and e.entity_type == "item"]
    n_faults = int(len(update_idx) * rate)
    if n_faults == 0:
        return events

    chosen = rng.sample(update_idx, min(n_faults, len(update_idx)))
    classes = rng.choices(list(FAULT_WEIGHTS), weights=list(FAULT_WEIGHTS.values()), k=len(chosen))
    out: list[Event] = list(events)
    appended: list[tuple[int, Event]] = []  # (insert_after_index, event)

    for idx, fclass in zip(chosen, classes):
        ev = out[idx]
        if ev.op != "upsert":
            fclass = "F8_duplicate_delivery"  # deletes can still be redelivered
        if fclass == "F1_duplicate_entity":
            dup = _clone(ev)
            dup.event_id += "-DUP"
            dup.payload["item_id"] = ev.payload["item_id"].replace("ITM-", "ITX-")
            dup.injected_faults = ["F1_duplicate_entity"]
            appended.append((idx, dup))
        elif fclass == "F2_orphan_reference":
            ev.payload["category_id"] = "CAT-9999"
            ev.injected_faults.append("F2_orphan_reference")
        elif fclass == "F3_conflicting_values":
            rival = _clone(ev)
            rival.event_id += "-CONFLICT"
            rival.source = "vendor-feed" if ev.source != "vendor-feed" else "merch-tool"
            rival.payload["source"] = rival.source
            rival.payload["sellable"] = not bool(ev.payload.get("sellable", True))
            rival.injected_faults = ["F3_conflicting_values"]
            appended.append((idx, rival))
        elif fclass == "F4_stale_record":
            ev.payload["effective_from"] = max(0, int(ev.payload.get("effective_from", 0)) - 2500)
            ev.payload["effective_to"] = ev.payload["effective_from"] + 100  # expired window
            ev.injected_faults.append("F4_stale_record")
        elif fclass == "F5_schema_violation":
            if rng.random() < 0.5:
                ev.payload["temperature_class"] = "lava"  # unknown enum
            else:
                ev.payload["sellable"] = "yes"            # wrong type
            ev.injected_faults.append("F5_schema_violation")
        elif fclass == "F6_missing_mandatory":
            victim_field = rng.choice(["category_id", "temperature_class", "name"])
            ev.payload.pop(victim_field, None)
            ev.injected_faults.append("F6_missing_mandatory")
        elif fclass == "F7_out_of_order":
            late = _clone(ev)
            late.event_id += "-LATE"
            late.payload["effective_from"] = max(0, int(ev.payload.get("effective_from", 1)) - 1)
            late.payload["temperature_class"] = rng.choice(TEMPERATURE_CLASSES)
            late.injected_faults = ["F7_out_of_order"]
            # delivered later than a fresher event, with an older logical time
            appended.append((min(idx + rng.randint(3, 25), len(out) - 1), late))
        elif fclass == "F8_duplicate_delivery":
            dup = _clone(ev)
            dup.injected_faults = ["F8_duplicate_delivery"]
            appended.append((min(idx + rng.randint(1, 10), len(out) - 1), dup))

    # splice appended events after their anchor positions, preserving relative order
    for insert_after, ev in sorted(appended, key=lambda t: t[0], reverse=True):
        out.insert(insert_after + 1, ev)
    return out


def _load_boundary(corpus: Corpus) -> int:
    n = (len(corpus.categories) + len(corpus.stores)
         + len(corpus.items) + len(corpus.assortments))
    return n


def fault_summary(events: list[Event]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for e in events:
        for f in e.injected_faults:
            counts[f] = counts.get(f, 0) + 1
    return dict(sorted(counts.items()))
