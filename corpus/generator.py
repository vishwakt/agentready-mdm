"""Deterministic generator for the synthetic master-data corpus and its event stream.

Design decisions (documented for the paper's methodology section):
- Everything derives from random.Random(seed): identical seed => identical corpus.
- The event stream = initial load (one upsert per entity) followed by an update
  phase (attribute changes, deletes, new items) so both pipelines see realistic
  churn rather than a static snapshot.
- Ground truth is the final state produced by applying the CLEAN stream in order.
"""
from __future__ import annotations
import random
from corpus.model import (
    Category, Item, Store, Assortment, Event, Corpus,
    TEMPERATURE_CLASSES, EQUIPMENT_TYPES, REGIONS, STORE_FORMATS, SOURCES,
)

ADJ = ["Golden", "Twilight", "Harbor", "Cedar", "Summit", "Velvet", "Copper",
       "Meadow", "Ember", "Frost", "Juniper", "Saffron", "Cobalt", "Maple"]
NOUN = ["Roast", "Blend", "Tart", "Wrap", "Bowl", "Cooler", "Loaf", "Bar",
        "Melt", "Chai", "Cold Brew", "Refresher", "Scone", "Panini"]


def _cat_tree(rng: random.Random, target_nodes: int = 150) -> dict[str, Category]:
    cats: dict[str, Category] = {}
    counter = 0

    def new_cat(name: str, parent: str | None, level: int) -> Category:
        nonlocal counter
        counter += 1
        c = Category(f"CAT-{counter:04d}", name, parent, level)
        cats[c.category_id] = c
        return c

    roots = [new_cat(n, None, 1) for n in ["Beverage", "Food", "Merchandise"]]
    frontier = list(roots)
    while len(cats) < target_nodes:
        parent = rng.choice(frontier)
        if parent.level >= 6:
            frontier.remove(parent)
            continue
        name = f"{parent.name}/{rng.choice(ADJ)}"
        child = new_cat(name, parent.category_id, parent.level + 1)
        frontier.append(child)
        if rng.random() < 0.15 and parent in frontier and parent.level > 1:
            frontier.remove(parent)
    return cats


def _make_item(rng: random.Random, i: int, cat_ids: list[str], ts: int) -> Item:
    return Item(
        item_id=f"ITM-{i:05d}",
        name=f"{rng.choice(ADJ)} {rng.choice(NOUN)} {rng.randint(1, 99)}",
        category_id=rng.choice(cat_ids),
        temperature_class=rng.choice(TEMPERATURE_CLASSES),
        equipment_required=sorted(rng.sample(EQUIPMENT_TYPES, rng.randint(0, 2))),
        sellable=rng.random() < 0.85,
        effective_from=ts,
        effective_to=None,
        source=rng.choice(SOURCES),
    )


def generate(seed: int = 7, n_items: int = 2000, n_stores: int = 300,
             n_assortments: int = 40, n_updates: int = 3000) -> Corpus:
    rng = random.Random(seed)
    cats = _cat_tree(rng)
    leaf_ids = [c.category_id for c in cats.values() if c.level >= 3]

    items = {it.item_id: it for it in (_make_item(rng, i, leaf_ids, ts=0) for i in range(1, n_items + 1))}

    stores = {}
    for s in range(1, n_stores + 1):
        stores[f"STR-{s:04d}"] = Store(
            store_id=f"STR-{s:04d}",
            region=rng.choice(REGIONS),
            store_format=rng.choice(STORE_FORMATS),
            equipment=sorted(rng.sample(EQUIPMENT_TYPES, rng.randint(2, 5))),
        )

    assortments = {}
    store_ids, item_ids = list(stores), list(items)
    for a in range(1, n_assortments + 1):
        assortments[f"AST-{a:03d}"] = Assortment(
            assortment_id=f"AST-{a:03d}",
            name=f"Assortment {a:03d}",
            store_ids=sorted(rng.sample(store_ids, rng.randint(10, 60))),
            item_ids=sorted(rng.sample(item_ids, rng.randint(40, 200))),
        )

    events: list[Event] = []
    seq = 0

    def emit(entity_type: str, op: str, payload: dict, source: str, ):
        nonlocal seq
        seq += 1
        events.append(Event(
            event_id=f"EVT-{seq:06d}", seq=seq, entity_type=entity_type,
            op=op, payload=payload, source=source,
        ))

    # Initial load: categories first (referential parents), then stores, items, assortments.
    for c in cats.values():
        emit("category", "upsert", c.__dict__.copy(), "erp")
    for s in stores.values():
        emit("store", "upsert", s.__dict__.copy(), "erp")
    for it in items.values():
        emit("item", "upsert", it.to_record(), it.source)
    for a in assortments.values():
        emit("assortment", "upsert", a.__dict__.copy(), "merch-tool")

    # Update phase: 70% item attribute updates, 15% new items, 10% assortment edits, 5% item deletes.
    live_items = dict(items)
    next_item_no = n_items + 1
    for _ in range(n_updates):
        r, ts = rng.random(), seq + 1
        if r < 0.70 and live_items:
            it = live_items[rng.choice(list(live_items))]
            rec = it.to_record()
            field_choice = rng.random()
            if field_choice < 0.4:
                rec["sellable"] = not rec["sellable"]
            elif field_choice < 0.7:
                rec["temperature_class"] = rng.choice(TEMPERATURE_CLASSES)
            else:
                rec["equipment_required"] = sorted(rng.sample(EQUIPMENT_TYPES, rng.randint(0, 3)))
            rec["effective_from"] = ts
            rec["source"] = rng.choice(SOURCES)
            emit("item", "upsert", rec, rec["source"])
            live_items[it.item_id] = Item(**{k: rec[k] for k in it.to_record()})
        elif r < 0.85:
            it = _make_item(rng, next_item_no, leaf_ids, ts=ts)
            next_item_no += 1
            live_items[it.item_id] = it
            emit("item", "upsert", it.to_record(), it.source)
        elif r < 0.95:
            a = assortments[rng.choice(list(assortments))]
            pool = [i for i in live_items if i not in set(a.item_ids)]
            if pool:
                a.item_ids = sorted(a.item_ids + [rng.choice(pool)])
            emit("assortment", "upsert", a.__dict__.copy(), "merch-tool")
        else:
            victim = rng.choice(list(live_items))
            del live_items[victim]
            emit("item", "delete", {"item_id": victim}, "erp")

    return Corpus(cats, items, stores, assortments, events)
