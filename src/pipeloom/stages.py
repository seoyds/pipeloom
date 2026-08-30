from typing import List, Tuple, Type


class Stage:
    EXTRACT = 0
    VALIDATE = 10
    ENRICH = 20
    AGENT = 30
    TRANSFORM = 40
    LOAD = 50
    MONITOR = 1_000_000


def get_stage_names(stage_cls: Type) -> List[Tuple[int, str]]:
    names: List[Tuple[int, str]] = []
    for key, val in vars(stage_cls).items():
        if isinstance(val, int) and not key.startswith("_"):
            names.append((val, key))
    names.sort()
    return names
