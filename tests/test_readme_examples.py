"""Keeps the README's "Why pipeloom" claims honest: the swap example runs, and
a handler can be unit-tested as a plain class."""

import importlib.util
from pathlib import Path

from pipeloom import Handler, RawRecord, Record, Stage, step

_SWAP_PATH = Path(__file__).resolve().parent.parent / "examples" / "sample_pipeline_swap.py"


def _load_swap_example():
    spec = importlib.util.spec_from_file_location("sample_pipeline_swap", _SWAP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_swapping_labeller_changes_only_the_labels():
    swap = _load_swap_example()
    parse, finalize = swap.ParseOrder(), swap.Finalize()

    before = await swap.run_with(swap.RuleLabeler(), parse, finalize)
    after = await swap.run_with(swap.LLMLabeler(), parse, finalize)

    assert [r["category"] for r in before] == ["bulk", "retail"]
    assert [r["category"] for r in after] == ["bulk", "gift"]
    # Everything the untouched parts produce is identical apart from the label.
    strip = lambda rows: [{k: v for k, v in r.items() if k != "category"} for r in rows]
    assert strip(before) == strip(after) == swap.ORDERS


def test_handler_is_unit_testable_as_a_plain_class():
    class Uppercase(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Record:
            return Record({k: str(v).upper() for k, v in message.value.items()})

    assert Uppercase().go(RawRecord({"name": "ada"}, time=0)) == {"name": "ADA"}
