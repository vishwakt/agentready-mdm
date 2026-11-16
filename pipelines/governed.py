"""Governed pipeline: the matcher-survivor-router control layer.

Stages per event:
  1. Idempotency gate  - exact redelivery (same event_id) is a safe no-op (F8)
  2. Matcher           - schema validation against the versioned contract:
                         mandatory fields (F6), types and enums (F5),
                         referential integrity (F2), staleness window (F4)
  3. Survivor          - conflict resolution BEFORE commit: monotonic
                         effective_from (F7), duplicate-entity detection (F1),
                         source-priority + recency + completeness survivorship (F3)
  4. Commit            - immutable versioned write (append, never overwrite)
  5. Router            - contract-scoped fan-out to consumer views
Rejected events land in exception states (quarantine) with a machine-readable
reason, or in the DLQ if handling itself fails. Nothing is silently dropped.
"""
from __future__ import annotations
import time
from corpus.model import Event, TEMPERATURE_CLASSES, SOURCES, SCHEMA_VERSION, MANDATORY_ITEM_FIELDS

SOURCE_PRIORITY = {s: i for i, s in enumerate(SOURCES)}  # lower = higher priority

CONSUMER_CONTRACTS = {
    # consumer_name: fields exposed (typed contract projection)
    "ordering-agent": ["item_id", "name", "category_id", "temperature_class",
                        "equipment_required", "sellable", "effective_from"],
    "personalization": ["item_id", "name", "category_id", "sellable"],
}


class GovernedPipeline:
    name = "governed"

    def __init__(self, dup_detection: bool = True, disable: set | None = None,
                 conflict_policy: str = "survivorship"):
        """disable: subset of {'idempotency','matcher','survivor','cascade'} for
        ablation runs. conflict_policy: 'survivorship' (optimistic, scored
        resolution) or 'quarantine' (pessimistic: concurrent conflicts are
        always routed to exception state for stewardship)."""
        self.dup_detection = dup_detection
        self.disable = disable or set()
        self.conflict_policy = conflict_policy
        self.head: dict[str, dict[str, dict]] = {
            "category": {}, "store": {}, "item": {}, "assortment": {},
        }
        self.versions: dict[str, dict[str, list[dict]]] = {
            "category": {}, "store": {}, "item": {}, "assortment": {},
        }
        self.event_log: list[dict] = []          # immutable append-only accepted log
        self.quarantine: list[dict] = []         # exception states with reasons
        self.dlq: list[dict] = []                # handler failures
        self.seen_event_ids: set[str] = set()
        self.consumer_views: dict[str, dict[str, dict]] = {c: {} for c in CONSUMER_CONTRACTS}
        self.latencies_us: list[float] = []
        # entity fingerprint index for duplicate detection (F1)
        self._fingerprints: dict[str, str] = {}  # fp -> item_id

    @staticmethod
    def _pk(entity_type: str) -> str:
        return {"category": "category_id", "store": "store_id",
                "item": "item_id", "assortment": "assortment_id"}[entity_type]

    # ---------- Stage 2: Matcher ----------
    def _match(self, ev: Event) -> str | None:
        if "matcher" in self.disable:
            return None
        p = ev.payload
        if ev.op == "delete":
            return None
        if ev.entity_type != "item":
            return None  # item contract is the evaluated surface; others pass structural check
        for f in MANDATORY_ITEM_FIELDS:
            if f not in p:
                return f"missing_mandatory:{f}"
        if p.get("schema_version") != SCHEMA_VERSION:
            return "unknown_schema_version"
        if not isinstance(p["sellable"], bool):
            return "type_violation:sellable"
        if p["temperature_class"] not in TEMPERATURE_CLASSES:
            return "enum_violation:temperature_class"
        if p["category_id"] not in self.head["category"]:
            return "orphan_reference:category_id"
        if p.get("effective_to") is not None and p["effective_to"] < len(self.event_log):
            return "stale_record:expired_window"
        return None

    # ---------- Stage 3: Survivor ----------
    def _survive(self, ev: Event) -> str | None:
        if "survivor" in self.disable:
            return None
        if ev.op == "delete" or ev.entity_type != "item":
            return None
        p = ev.payload
        current = self.head["item"].get(p["item_id"])
        if current is not None:
            if p["effective_from"] < current["effective_from"]:
                return "out_of_order:older_than_head"
            if p["effective_from"] == current["effective_from"] and p != current:
                if self.conflict_policy == "quarantine":
                    return "conflict_ambiguous:steward_review"
                # optimistic policy: survivorship scoring
                if self._score(p) <= self._score(current):
                    return "conflict_lost:survivorship"
        if self.dup_detection:
            fp = self._fingerprint(p)
            owner = self._fingerprints.get(fp)
            if owner is not None and owner != p["item_id"]:
                return f"duplicate_entity:of:{owner}"
        return None

    @staticmethod
    def _score(rec: dict) -> tuple:
        completeness = sum(1 for v in rec.values() if v not in (None, "", []))
        return (-SOURCE_PRIORITY.get(rec.get("source", "vendor-feed"), 9),
                rec.get("effective_from", 0), completeness)

    @staticmethod
    def _fingerprint(p: dict) -> str:
        return f"{p.get('name','').lower()}|{p.get('category_id')}"

    # ---------- Stage 4/5: Commit + Router ----------
    def _commit(self, ev: Event) -> None:
        table, pk = self.head[ev.entity_type], self._pk(ev.entity_type)
        key = ev.payload[pk]
        if ev.op == "upsert":
            rec = dict(ev.payload)
            if ev.entity_type == "assortment" and "cascade" not in self.disable:
                # referential integrity on inbound refs: scrub ids not in head
                live = self.head["item"]
                rec["item_ids"] = [i for i in rec.get("item_ids", []) if i in live]
                rec["store_ids"] = [s for s in rec.get("store_ids", []) if s in self.head["store"]]
            table[key] = rec
            self.versions[ev.entity_type].setdefault(key, []).append(rec)
            if ev.entity_type == "item":
                self._fingerprints[self._fingerprint(rec)] = key
            self._route(ev.entity_type, rec)
        else:
            self._cascade_delete(ev.entity_type, key)
        self.event_log.append({"seq_accepted": len(self.event_log), "event": ev.to_json()})

    def _cascade_delete(self, entity_type: str, key: str) -> None:
        """Smart cascade: remove the entity and scrub inbound references."""
        self.head[entity_type].pop(key, None)
        if "cascade" in self.disable:
            return
        if entity_type == "item":
            for a in self.head["assortment"].values():
                if key in a.get("item_ids", []):
                    a["item_ids"] = [i for i in a["item_ids"] if i != key]
            for view in self.consumer_views.values():
                view.pop(key, None)

    def _route(self, entity_type: str, rec: dict) -> None:
        if entity_type != "item":
            return
        for consumer, fields in CONSUMER_CONTRACTS.items():
            self.consumer_views[consumer][rec["item_id"]] = {f: rec.get(f) for f in fields}

    # ---------- Driver ----------
    def apply(self, ev: Event) -> str:
        t0 = time.perf_counter()
        try:
            if ev.event_id in self.seen_event_ids and "idempotency" not in self.disable:
                outcome = "skipped:idempotent"
            else:
                self.seen_event_ids.add(ev.event_id)
                reason = self._match(ev) or self._survive(ev)
                if reason:
                    pk = self._pk(ev.entity_type)
                    self.quarantine.append({"event_id": ev.event_id, "reason": reason,
                                            "entity_id": ev.payload.get(pk),
                                            "faults": list(ev.injected_faults)})
                    outcome = f"quarantined:{reason}"
                else:
                    self._commit(ev)
                    outcome = "committed"
        except Exception as exc:  # handler failure -> DLQ, never silent loss
            self.dlq.append({"event_id": ev.event_id, "error": repr(exc)})
            outcome = "dlq"
        self.latencies_us.append((time.perf_counter() - t0) * 1e6)
        return outcome

    def run(self, events: list[Event]) -> None:
        for ev in events:
            self.apply(ev)

    def visible_state(self) -> dict[str, dict[str, dict]]:
        return self.head

    def replay_to(self, n: int) -> dict:
        """Reconstruct item head-state from the first n accepted log entries."""
        import json
        replayed = GovernedPipeline()
        for entry in self.event_log[:n]:
            raw = json.loads(entry["event"])
            replayed.apply(Event(**raw))
        return replayed.head
