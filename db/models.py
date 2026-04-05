"""
db/models.py

Pydantic models used to validate records before insertion.
These are plain data containers — not ORM models.
"""

from __future__ import annotations
from typing import Any
from pydantic import BaseModel, field_validator


class TraditionRecord(BaseModel):
    name: str  # "Abrahamic", "Dharmic", "Political", "Music"


class CorpusRecord(BaseModel):
    tradition_name: str       # looked up / inserted into corpus_tradition
    name: str                 # "KJV Bible", "Tao Te Ching (Legge)"
    type: str                 # scripture | legal | news | music | literature
    language: str = "en"      # ISO 639-1
    era: str | None = None    # ancient | medieval | modern
    metadata: dict[str, Any] = {}


class PassageRecord(BaseModel):
    corpus_id: int
    book: str | None = None
    section: str | None = None
    unit_number: int | None = None
    unit_label: str | None = None
    text: str
    metadata: dict[str, Any] = {}

    @field_validator("text")
    @classmethod
    def text_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("passage text must not be empty")
        return v.strip()


class EmbeddingRecord(BaseModel):
    passage_id: int
    model_name: str
    vector: list[float]

    @field_validator("vector")
    @classmethod
    def vector_not_empty(cls, v: list[float]) -> list[float]:
        if not v:
            raise ValueError("embedding vector must not be empty")
        return v
