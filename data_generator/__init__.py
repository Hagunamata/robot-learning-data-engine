"""Synthetic episode augmenter (evolved from the prior repo's data_generator/).

Mints LeRobot-format episodes for under-represented tasks and routes them through the
SAME validation gates as real data. See docs/01-conception.md §4.5. Implemented in M5.
"""

from .augment import augment_dataset, under_represented_tasks

__all__ = ["augment_dataset", "under_represented_tasks"]
