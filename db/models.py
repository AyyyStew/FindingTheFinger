"""
db/models.py

SQLAlchemy ORM models for the Finding The Finger database.
"""

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger, DateTime, Float, ForeignKey, Index, Integer, Text, JSON,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Tradition(Base):
    __tablename__ = "tradition"

    id:   Mapped[int]  = mapped_column(Integer, primary_key=True)
    name: Mapped[str]  = mapped_column(Text, nullable=False, unique=True)

    corpora: Mapped[list["Corpus"]] = relationship(back_populates="tradition")


class Corpus(Base):
    __tablename__ = "corpus"

    id:           Mapped[int]         = mapped_column(Integer, primary_key=True)
    tradition_id: Mapped[int]         = mapped_column(ForeignKey("tradition.id"), nullable=False)
    name:         Mapped[str]         = mapped_column(Text, nullable=False, unique=True)
    type:         Mapped[str | None]  = mapped_column(Text)
    language:     Mapped[str | None]  = mapped_column(Text)
    era:          Mapped[str | None]  = mapped_column(Text)
    meta:         Mapped[dict | None] = mapped_column("metadata", JSON)

    tradition: Mapped["Tradition"]       = relationship(back_populates="corpora")
    levels:    Mapped[list["CorpusLevel"]] = relationship(back_populates="corpus")
    units:     Mapped[list["Unit"]]       = relationship(back_populates="corpus")


class CorpusLevel(Base):
    """Defines the natural level names for each corpus at each height."""
    __tablename__ = "corpus_level"

    corpus_id: Mapped[int] = mapped_column(ForeignKey("corpus.id"), primary_key=True)
    height:    Mapped[int] = mapped_column(Integer, primary_key=True)
    name:      Mapped[str] = mapped_column(Text, nullable=False)  # 'Verse', 'Surah', 'Ayah'...

    corpus: Mapped["Corpus"] = relationship(back_populates="levels")


class Unit(Base):
    """
    A content node at any level of the hierarchy.

    Height is leaf-up (0 = verse/leaf, 1 = chapter, 2 = book, ...).
    Depth is root-down (0 = book, 1 = chapter, 2 = verse, ...).
    Both are stored for query convenience.
    Parent is None for root nodes (books).
    """
    __tablename__ = "unit"

    id:        Mapped[int]         = mapped_column(BigInteger, primary_key=True)
    corpus_id: Mapped[int]         = mapped_column(ForeignKey("corpus.id"), nullable=False)
    parent_id: Mapped[int | None]  = mapped_column(BigInteger, ForeignKey("unit.id"))
    depth:     Mapped[int]         = mapped_column(Integer, nullable=False)
    height:    Mapped[int | None]  = mapped_column(Integer)
    label:     Mapped[str | None]  = mapped_column(Text)
    text:      Mapped[str | None]  = mapped_column(Text)
    meta:      Mapped[dict | None] = mapped_column("metadata", JSON)

    corpus:     Mapped["Corpus"]          = relationship(back_populates="units")
    children:   Mapped[list["Unit"]]      = relationship(back_populates="parent")
    parent:     Mapped["Unit | None"]     = relationship(back_populates="children", remote_side="Unit.id")
    embeddings: Mapped[list["Embedding"]] = relationship(back_populates="unit")

    __table_args__ = (
        Index("ix_unit_corpus_height", "corpus_id", "height"),
        Index("ix_unit_parent",        "parent_id"),
    )


class Embedding(Base):
    __tablename__ = "embedding"

    unit_id:    Mapped[int] = mapped_column(BigInteger, ForeignKey("unit.id"), primary_key=True)
    model_name: Mapped[str] = mapped_column(Text, primary_key=True)
    vector:     Mapped[list[float]] = mapped_column(Vector(768))

    unit: Mapped["Unit"] = relationship(back_populates="embeddings")


class UmapRun(Base):
    __tablename__ = "umap_run"

    id:          Mapped[int]           = mapped_column(Integer, primary_key=True)
    created_at:  Mapped[datetime]      = mapped_column(DateTime(timezone=True), server_default=func.now())
    label:       Mapped[str | None]    = mapped_column(Text)
    model_name:  Mapped[str]           = mapped_column(Text, nullable=False, default="nomic-embed-text-v1.5")
    n_neighbors: Mapped[int | None]    = mapped_column(Integer)
    min_dist:    Mapped[float | None]  = mapped_column(Float)

    points: Mapped[list["UmapPoint"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class UmapPoint(Base):
    __tablename__ = "umap_point"

    umap_run_id: Mapped[int]       = mapped_column(ForeignKey("umap_run.id", ondelete="CASCADE"), primary_key=True)
    unit_id:     Mapped[int]       = mapped_column(BigInteger, ForeignKey("unit.id"), primary_key=True)
    x:           Mapped[float]     = mapped_column(Float, nullable=False)
    y:           Mapped[float]     = mapped_column(Float, nullable=False)
    corpus_seq:  Mapped[int | None] = mapped_column(Integer)

    run: Mapped["UmapRun"] = relationship(back_populates="points")

    __table_args__ = (
        Index("ix_umap_point_run", "umap_run_id"),
    )
