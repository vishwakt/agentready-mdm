"""Shared data model for the synthetic retail-style master data corpus.

Entities: Category (6-level hierarchy), Item, Store, Assortment.
Events: the unit of change flowing into either pipeline.
All construction is deterministic given a seed.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional
import json

SCHEMA_VERSION = "2.1"

TEMPERATURE_CLASSES = ["ambient", "chilled", "frozen", "hot-hold"]
EQUIPMENT_TYPES = ["espresso", "blender", "oven", "cold-case", "nitro-tap", "warmer"]
REGIONS = ["NA-W", "NA-E", "EMEA", "APAC", "LATAM"]
STORE_FORMATS = ["flagship", "standard", "kiosk", "drive-thru"]
SOURCES = ["erp", "merch-tool", "vendor-feed"]  # source systems, priority in this order

MANDATORY_ITEM_FIELDS = [
    "item_id", "name", "category_id", "temperature_class",
    "equipment_required", "sellable", "effective_from", "source", "schema_version",
]


@dataclass
class Category:
    category_id: str
    name: str
    parent_id: Optional[str]  # None for root
    level: int


@dataclass
class Item:
    item_id: str
    name: str
    category_id: str
    temperature_class: str
    equipment_required: list[str]
    sellable: bool
    effective_from: int          # logical timestamp
    effective_to: Optional[int]  # None = open-ended
    source: str
    schema_version: str = SCHEMA_VERSION

    def to_record(self) -> dict:
        return asdict(self)


@dataclass
class Store:
    store_id: str
    region: str
    store_format: str
    equipment: list[str]


@dataclass
class Assortment:
    assortment_id: str
    name: str
    store_ids: list[str]
    item_ids: list[str]


@dataclass
class Event:
    """A master-data update event. The unit both pipelines consume."""
    event_id: str
    seq: int                 # producer sequence number (logical time)
    entity_type: str         # item | category | store | assortment
    op: str                  # upsert | delete
    payload: dict            # full record for upsert; {"<pk>": id} for delete
    source: str
    injected_faults: list[str] = field(default_factory=list)  # ground-truth labels, invisible to pipelines

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)


@dataclass
class Corpus:
    categories: dict[str, Category]
    items: dict[str, Item]
    stores: dict[str, Store]
    assortments: dict[str, Assortment]
    events: list[Event]  # initial load + update stream, in seq order

    def counts(self) -> dict:
        return {
            "categories": len(self.categories),
            "items": len(self.items),
            "stores": len(self.stores),
            "assortments": len(self.assortments),
            "events": len(self.events),
        }
