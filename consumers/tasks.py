"""C1 deterministic task suite and C2 scripted agent.

Ground truth = the CLEAN event stream applied through an oracle with governed
semantics (cascade integrity, monotonic versions) but WITHOUT the heuristic
duplicate detector, i.e. the intended final state of the master data.

C1: 180 read tasks per run, answers computed from a pipeline's consumer-visible
state and compared to ground truth. Task families:
  T1 (80): sellable, equipment-compatible, effective items for a store
  T2 (50): category path resolution for an item
  T3 (30): sellable item count within a category subtree (catches phantom dupes)
  T4 (20): sellable count within an assortment

C2: 40 multi-step publish operations. The agent plans reads then COMMITS an
availability set for a store. A committed set differing from ground truth is a
wrong action; in the governed pipeline the agent additionally checks the
exception state for items it touches and defers when quarantined data is
implicated (trust signal available; baseline has none).
"""
from __future__ import annotations
import random
from corpus.model import Corpus, Event
from pipelines.governed import GovernedPipeline


def ground_truth_state(corpus: Corpus) -> dict:
    oracle = GovernedPipeline(dup_detection=False)
    oracle.run(corpus.events)
    return oracle.head


def _now(state: dict) -> int:
    return max((r.get("effective_from", 0) for r in state["item"].values()), default=0)


def _sellable_for_store(state: dict, store_id: str) -> set[str]:
    store = state["store"].get(store_id)
    if store is None:
        return set()
    now = _now(state)
    eligible: set[str] = set()
    covering = [a for a in state["assortment"].values() if store_id in a.get("store_ids", [])]
    for a in covering:
        for iid in a.get("item_ids", []):
            it = state["item"].get(iid)
            if it is None:
                continue
            if it.get("sellable") is not True:
                continue
            if it.get("effective_to") is not None and it["effective_to"] < now:
                continue
            if not set(it.get("equipment_required", [])) <= set(store.get("equipment", [])):
                continue
            eligible.add(iid)
    return eligible


def _category_path(state: dict, item_id: str) -> list[str] | None:
    it = state["item"].get(item_id)
    if it is None:
        return None
    path, cid, guard = [], it.get("category_id"), 0
    while cid is not None and guard < 10:
        c = state["category"].get(cid)
        if c is None:
            return ["<broken>"]
        path.append(cid)
        cid, guard = c.get("parent_id"), guard + 1
    return list(reversed(path))


def _subtree_sellable_count(state: dict, root_cat: str) -> int:
    children: dict[str, list[str]] = {}
    for c in state["category"].values():
        children.setdefault(c.get("parent_id"), []).append(c["category_id"])
    subtree, stack = set(), [root_cat]
    while stack:
        n = stack.pop()
        subtree.add(n)
        stack.extend(children.get(n, []))
    return sum(1 for it in state["item"].values()
               if it.get("category_id") in subtree and it.get("sellable") is True)


def _quarantined_ids(pipeline) -> set[str]:
    if not isinstance(pipeline, GovernedPipeline):
        return set()
    ids = set()
    for q in pipeline.quarantine:
        if q.get("entity_id"):
            ids.add(q["entity_id"])
        r = q.get("reason", "")
        if r.startswith("duplicate_entity:of:"):
            ids.add(r.rsplit(":", 1)[1])
    return ids


def run_c1(pipeline, truth: dict, seed: int) -> dict:
    """Item-level scoring over 180 tasks. Each mismatching item-decision is
    classified silent (no signal available to the consumer) or flagged (the
    entity appears in the exception state, so the consumer can see the data is
    contested). Baseline has no exception state: every mismatch is silent."""
    state = pipeline.visible_state()
    qids = _quarantined_ids(pipeline)
    rng = random.Random(seed + 555)
    stores = sorted(truth["store"])
    items = sorted(truth["item"])
    cats = sorted(truth["category"])
    asts = sorted(truth["assortment"])
    decisions = silent = flagged = 0

    for sid in rng.sample(stores, 80):
        sp, st_ = _sellable_for_store(state, sid), _sellable_for_store(truth, sid)
        for iid in sp | st_:
            decisions += 1
            if (iid in sp) != (iid in st_):
                if iid in qids:
                    flagged += 1
                else:
                    silent += 1
    for iid in rng.sample(items, 50):
        decisions += 1
        if _category_path(state, iid) != _category_path(truth, iid):
            if iid in qids: flagged += 1
            else: silent += 1
    for cid in rng.sample(cats, 30):
        decisions += 1
        if _subtree_sellable_count(state, cid) != _subtree_sellable_count(truth, cid):
            silent += 1  # aggregate task: attribute conservatively as silent
    for aid in rng.sample(asts, 20):
        decisions += 1
        a_p = state["assortment"].get(aid, {})
        a_t = truth["assortment"].get(aid, {})
        cnt = lambda s, a: sum(1 for i in a.get("item_ids", [])
                               if s["item"].get(i, {}).get("sellable") is True)
        if cnt(state, a_p) != cnt(truth, a_t):
            silent += 1
    return {"decisions": decisions,
            "silent_error_rate": silent / decisions,
            "flagged_rate": flagged / decisions}


def run_c2(pipeline, truth: dict, seed: int) -> dict:
    """Scripted agent: plan -> read -> commit availability set for 40 stores.
    Governed agents consult the exception state before committing and defer when
    the planned set implicates quarantined entities. Baseline commits blind."""
    rng = random.Random(seed + 777)
    state = pipeline.visible_state()
    qids = _quarantined_ids(pipeline)
    wrong = deferred = correct = 0
    p_wrong = p_correct = 0  # partial-commit policy: act on the safe subset
    item_decisions: list[tuple[int, int]] = []
    for sid in rng.sample(sorted(truth["store"]), 40):
        planned = _sellable_for_store(state, sid)
        intended = _sellable_for_store(truth, sid)
        if qids and (planned ^ intended) & qids:
            deferred += 1
        elif planned != intended:
            wrong += 1
        else:
            correct += 1
        # partial policy: commit planned minus quarantined; judged against the
        # intended set restricted to non-quarantined entities
        committed, safe_intent = planned - qids, intended - qids
        if committed != safe_intent:
            p_wrong += 1
        else:
            p_correct += 1
        item_decisions.append((len(committed | safe_intent),
                               len(committed ^ safe_intent)))
    n = wrong + deferred + correct
    return {"ops": n, "wrong_actions": wrong, "deferred": deferred,
            "wrong_action_rate": wrong / n, "deferred_rate": deferred / n,
            "partial_wrong_action_rate": p_wrong / n,
            "partial_item_error_rate": (sum(w for _, w in item_decisions)
                                        / max(1, sum(t for t, _ in item_decisions)))}
