"""Mint synthetic LeRobot-format episodes to balance under-represented tasks.

Synthetic episodes are clearly labelled as such in the catalog and pass through the
same schema + signal gates as real data (no laxer path). v1 generation is procedural
— no learned generative model.

See docs/01-conception.md §4.5. Implemented in M5.
"""

from __future__ import annotations


def generate_for_task(task: str, n_episodes: int, out_dir: str) -> list[str]:
    """Generate ``n_episodes`` synthetic LeRobot episodes for ``task``.

    TODO(M5): read the task distribution from the catalog, procedurally synthesize
    episodes for under-represented tasks, emit LeRobot format, label as synthetic.
    """
    raise NotImplementedError("augment is implemented in M5")
