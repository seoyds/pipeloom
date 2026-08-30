from __future__ import annotations

import inspect
import itertools
import time
import typing
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Tuple, Type, Union

from pipeloom.events import ErrorMessage, Message
from pipeloom.exceptions import PipelineError, PipelineFailed, PipelineStopped, RoutingError
from pipeloom.stages import Stage, get_stage_names

_step_counter = itertools.count()


@dataclass
class StepInfo:
    name: str
    stage: int
    rank: int
    accepts: Type
    returns: Any
    consume: bool
    func: Callable
    description: Optional[str]
    idx: int = -1
    next: "Optional[StepInfo]" = None

    def __repr__(self) -> str:
        return self.name

    __hash__ = object.__hash__


def _check_return_type(returns: Any) -> None:
    if returns in (None, type(None)):
        return
    origin = typing.get_origin(returns)
    if origin in (list, typing.Iterable) or getattr(returns, "_name", None) in ("Iterable", "List"):
        args = typing.get_args(returns)
        if args:
            _check_return_type(args[0])
        return
    if origin is Union:
        for subtype in typing.get_args(returns):
            _check_return_type(subtype)
        return
    if isinstance(returns, type) and issubclass(returns, Message):
        return
    raise AssertionError("@step has an unsupported return type")


def step(stage: int, consume: bool = False) -> Callable[[Callable], Callable]:
    """Decorator marking a Handler method as a pipeline step."""

    def decorator(func: Callable) -> Callable:
        spec = inspect.getfullargspec(func)
        args = spec.args
        # Resolve string annotations (from `from __future__ import annotations`,
        # postponed evaluation) back into real type objects.
        annotations = typing.get_type_hints(func)

        if "return" not in annotations:
            raise AssertionError(f"@step {func.__qualname__} must have an annotated return type")
        returns = annotations["return"]
        _check_return_type(returns)

        if len(args) != 2:
            raise AssertionError(f"@step {func.__qualname__} must take exactly 2 arguments (self, message)")
        message_arg = args[1]
        accepts = annotations.get(message_arg)
        if not (isinstance(accepts, type) and issubclass(accepts, Message)):
            raise AssertionError(
                f"@step {func.__qualname__}'s message argument must be annotated with a Message subclass"
            )

        func.__pipeloom_stage__ = stage
        func.__pipeloom_rank__ = next(_step_counter)
        func.__pipeloom_accepts__ = accepts
        func.__pipeloom_returns__ = returns
        func.__pipeloom_consume__ = consume
        return func

    return decorator


class Handler:
    def __str__(self) -> str:
        return self.__class__.__name__

    def get_steps(self) -> List[StepInfo]:
        infos: List[StepInfo] = []
        for _, method in inspect.getmembers(self, inspect.ismethod):
            if hasattr(method, "__pipeloom_stage__"):
                doc = method.__func__.__doc__
                doc = inspect.cleandoc(doc) if doc else None
                infos.append(
                    StepInfo(
                        name=f"{self}.{method.__func__.__name__}",
                        stage=method.__pipeloom_stage__,
                        rank=method.__pipeloom_rank__,
                        accepts=method.__pipeloom_accepts__,
                        returns=method.__pipeloom_returns__,
                        consume=method.__pipeloom_consume__,
                        func=method,
                        description=doc,
                    )
                )
        if not infos:
            raise AssertionError(f"{self} has no @step methods")
        infos.sort(key=lambda s: s.rank)
        prev_stage = -1
        for info in infos:
            if info.stage < prev_stage:
                raise AssertionError(f"@step {info.name} is out of stage order within {self}")
            prev_stage = info.stage
        return infos


_CATCH_ALL_STAGE = Stage.MONITOR + 1  # reserved: user Handlers must not use this stage


class _CatchAllHandler(Handler):
    """Built into every Pipeline. Turns an unhandled ErrorMessage into a fatal
    PipelineFailed instead of a silent drop or RoutingError, and silently
    drops any other otherwise-unrouted Message. Always runs last."""

    @step(_CATCH_ALL_STAGE, consume=True)
    def trap_error(self, message: ErrorMessage) -> None:
        raise PipelineFailed(f"Unhandled ErrorMessage: {message.exception!r}")

    @step(_CATCH_ALL_STAGE, consume=True)
    def trap_message(self, message: Message) -> None:
        return None


class Pipeline:
    def __init__(self) -> None:
        self._handlers: List[Handler] = []
        self._catch_all = _CatchAllHandler()
        self._dirty = True
        self._steps: List[StepInfo] = []
        self._entry: Optional[StepInfo] = None
        self._next_step_cache: Dict[Tuple[StepInfo, Type[Message]], StepInfo] = {}
        self._total_calls: List[int] = []
        self._total_time: List[float] = []
        self._emitted_counts: List[int] = []
        self._message_tallies: List[DefaultDict[Type[Message], int]] = []
        self._stage_names: List[Tuple[int, str]] = []

    def set_stage_labels(self, stage_cls: Type) -> None:
        self._stage_names = get_stage_names(stage_cls)

    def add_handler(self, handler: Handler) -> None:
        if handler in self._handlers:
            raise ValueError(f"{handler} is already registered")
        self._handlers.append(handler)
        self._dirty = True

    def remove_handler(self, handler: Handler) -> None:
        self._handlers.remove(handler)
        self._dirty = True

    def _rebuild(self) -> None:
        self._next_step_cache.clear()
        steps: List[StepInfo] = []
        for handler in self._handlers:
            steps += handler.get_steps()
        steps.sort(key=lambda s: s.stage)
        steps += self._catch_all.get_steps()  # always last, regardless of stage sort

        def _noop(message: Message) -> Message:
            return message

        entry = StepInfo(
            name="_entry", stage=-1, rank=-1, accepts=Message, returns=Message,
            consume=False, func=_noop, description="pipeline entry point",
        )
        entry.idx = 0
        prev = entry
        for i, s in enumerate(steps, start=1):
            s.idx = i
            prev.next = s
            prev = s

        self._entry = entry
        self._steps = steps
        n = len(steps) + 1
        self._total_calls = [0] * n
        self._total_time = [0.0] * n
        self._emitted_counts = [0] * n
        self._message_tallies = [defaultdict(int) for _ in range(n)]
        self._dirty = False

    def _find_next(self, current: StepInfo, message_type: Type[Message]) -> StepInfo:
        node = current.next
        while node is not None:
            if issubclass(message_type, node.accepts):
                return node
            node = node.next
        raise RoutingError(f"No handler found for message type {message_type.__name__}")

    async def process(self, message: Message) -> None:
        if self._dirty:
            self._rebuild()
        assert self._entry is not None

        stack: List[Tuple[StepInfo, Message]] = [(self._entry, message)]
        cache = self._next_step_cache
        target = self._entry
        msg = message

        while stack:
            try:
                while stack:
                    producer, msg = stack.pop()
                    msg_type = type(msg)
                    self._message_tallies[producer.idx][msg_type] += 1
                    key = (producer, msg_type)
                    target = cache.get(key)
                    if target is None:
                        target = self._find_next(producer, msg_type)
                        cache[key] = target

                    start = time.time()
                    result = target.func(msg)
                    if inspect.isawaitable(result):
                        result = await result
                    elapsed = time.time() - start

                    self._total_calls[target.idx] += 1
                    self._total_time[target.idx] += elapsed

                    if not target.consume:
                        stack.append((target, msg))

                    if result is None:
                        pass
                    elif isinstance(result, Message):
                        self._emitted_counts[target.idx] += 1
                        stack.append((target, result))
                    elif hasattr(result, "__iter__"):
                        results = list(result)
                        self._emitted_counts[target.idx] += len(results)
                        stack.extend((target, r) for r in reversed(results))
                    else:
                        raise PipelineError(f"{target} returned an invalid value: {result!r}")
            except (PipelineStopped, PipelineFailed):
                raise
            except Exception as exc:
                error_message = ErrorMessage(message=msg, exception=exc)
                stack.append((target, error_message))

    def get_profile_data(self) -> dict:
        if self._dirty:
            self._rebuild()
        assert self._entry is not None
        complete = [self._entry] + self._steps
        pipeline_entries = []
        for s in complete:
            emits = []
            for msg_type, count in self._message_tallies[s.idx].items():
                try:
                    next_name = self._find_next(s, msg_type).name
                except RoutingError:
                    next_name = None
                emits.append({"message_type": msg_type.__name__, "count": count, "next": next_name})
            pipeline_entries.append(
                {
                    "name": s.name,
                    "stage": s.stage,
                    "calls": self._total_calls[s.idx],
                    "time": self._total_time[s.idx],
                    "emitted": self._emitted_counts[s.idx],
                    "emits": emits,
                }
            )
        return {"stages": list(self._stage_names), "pipeline": pipeline_entries}

    def format_profile(self, profile: dict) -> str:
        if not profile["pipeline"]:
            return "No Profile"
        lines = ["\n"]
        stages = sorted(profile["stages"], reverse=True)
        total_time = sum(e["time"] for e in profile["pipeline"]) or 1.0
        for entry in profile["pipeline"]:
            while stages and stages[-1][0] <= entry["stage"]:
                stage_val, stage_name = stages.pop()
                lines.append(f"[ {stage_name} = {stage_val} ]".ljust(50, "-"))
            pct = entry["time"] / total_time
            lines.append(
                f"{entry['name'].ljust(40)} {entry['time']:.4f}s {pct:.1%} "
                f"{entry['calls']} calls -> {entry['emitted']} emitted"
            )
        return "\n".join(lines)
