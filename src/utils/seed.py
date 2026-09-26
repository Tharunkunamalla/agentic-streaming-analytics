"""Seed utility for deterministic experiment reproducibility."""

import os
import random
import numpy as np


def set_seed(seed: int = 42) -> None:
    """Set global seeds across Python standard library, OS environment, and NumPy.

    Parameters
    ----------
    seed : int
        The integer seed to apply everywhere.
    """
    random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    np.random.seed(seed)
