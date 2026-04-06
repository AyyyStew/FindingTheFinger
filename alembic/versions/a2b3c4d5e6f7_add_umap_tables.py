"""add umap tables

Revision ID: a2b3c4d5e6f7
Revises: b8c733334785
Create Date: 2026-04-05
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a2b3c4d5e6f7'
down_revision: Union[str, Sequence[str], None] = 'b8c733334785'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'umap_run',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True),
                  server_default=sa.text('now()'), nullable=False),
        sa.Column('label', sa.Text(), nullable=True),
        sa.Column('model_name', sa.Text(), nullable=False,
                  server_default='nomic-embed-text-v1.5'),
        sa.Column('n_neighbors', sa.Integer(), nullable=True),
        sa.Column('min_dist', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'umap_point',
        sa.Column('umap_run_id', sa.Integer(), nullable=False),
        sa.Column('unit_id', sa.BigInteger(), nullable=False),
        sa.Column('x', sa.Float(), nullable=False),
        sa.Column('y', sa.Float(), nullable=False),
        sa.Column('corpus_seq', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['umap_run_id'], ['umap_run.id'],
                                ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['unit_id'], ['unit.id']),
        sa.PrimaryKeyConstraint('umap_run_id', 'unit_id'),
    )
    op.create_index('ix_umap_point_run', 'umap_point', ['umap_run_id'])


def downgrade() -> None:
    op.drop_index('ix_umap_point_run', table_name='umap_point')
    op.drop_table('umap_point')
    op.drop_table('umap_run')
