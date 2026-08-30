from __future__ import annotations

import collections.abc
import inspect
import itertools
import time
import types
import typing
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Tuple, Type, Union

from pipeloom.messages import ErrorMessage, Message, Record
from pipeloom.exceptions import PipelineError, PipelineFailed, PipelineStopped, RoutingError
from pipeloom.stages import Stage, get_stage_names

_step_counter = itertools.count()

_SINK_TYPES: Dict[str, Callable[..., Any]] = {}


def register_sink_type(name: str, factory: Callable[..., Any]) -> None:
    """Register a sink implementation under `name` for Pipeline.add_sink's
    sink_type= config path. pipeloom.dataio.sinks/parquet/kafka/db each call
    this at import time -- a sink_type is only available once its module has
    been imported (directly, or transitively by importing something else
    from it)."""
    _SINK_TYPES[name] = factory


@dataclass
class _SinkRegistration:
    """A sink_type= registration: the constructed implementation, and the one
    Message type it was configured to fire on."""

    impl: Any
    trigger: Type[Message]


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
    if origin in (list, collections.abc.Iterable, collections.abc.Sequence) or getattr(
        returns, "_name", None
    ) in ("Iterable", "List"):
        args = typing.get_args(returns)
        if args:
            _check_return_type(args[0])
        return
    # PEP 604 unions (Record | ErrorMessage) report types.UnionType as their
    # origin, not typing.Union, on every Python before 3.14.
    if origin is Union or origin is types.UnionType:
        for subtype in typing.get_args(returns):
            _check_return_type(subtype)
        return
    if isinstance(returns, type) and issubclass(returns, Message):
        return
    raise AssertionError("@step has an unsupported return type")


def _return_type_names(returns: Any) -> List[str]:
    """Flatten a @step's return annotation (a Message subclass, an Iterable/List
    of one, a Union of several, or None) into the Message class names it can
    produce -- used to render the profiler's blueprint."""
    if returns in (None, type(None)):
        return []
    origin = typing.get_origin(returns)
    if origin in (list, collections.abc.Iterable, collections.abc.Sequence) or getattr(
        returns, "_name", None
    ) in ("Iterable", "List"):
        args = typing.get_args(returns)
        return _return_type_names(args[0]) if args else []
    if origin is Union or origin is types.UnionType:
        names: List[str] = []
        for subtype in typing.get_args(returns):
            names.extend(_return_type_names(subtype))
        return names
    if isinstance(returns, type) and issubclass(returns, Message):
        return [returns.__name__]
    return []


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
    """A unit of pipeline logic; groups one or more @step methods."""

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


_CATCH_ALL_STAGE = Stage.MONITOR + 1  # placed after the sort so it always runs last, regardless of what stage a user Handler picks


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
    """Flattens the @step methods of its Handlers into one stage-ordered chain
    and drives Messages through it."""

    def __init__(self) -> None:
        self._handlers: List[Handler] = []
        self._sinks: List[Union[Handler, _SinkRegistration]] = []
        self._catch_all = _CatchAllHandler()
        self._dirty = True
        self._steps: List[StepInfo] = []
        self._sink_steps: List[StepInfo] = []
        self._entry: Optional[StepInfo] = None
        self._next_step_cache: Dict[Tuple[StepInfo, Type[Message]], StepInfo] = {}
        self._total_calls: List[int] = []
        self._total_time: List[float] = []
        self._emitted_counts: List[int] = []
        self._message_tallies: List[DefaultDict[Type[Message], int]] = []
        self._stage_names: List[Tuple[int, str]] = get_stage_names(Stage)

    def set_stage_labels(self, stage_cls: Type) -> None:
        """Relabel the profiler's stage banners for a custom Stage class."""
        self._stage_names = get_stage_names(stage_cls)

    def add_handler(self, handler: Handler) -> None:
        if handler in self._handlers:
            raise ValueError(f"{handler} is already registered")
        self._handlers.append(handler)
        self._dirty = True

    def remove_handler(self, handler: Handler) -> None:
        self._handlers.remove(handler)
        self._dirty = True

    def add_sink(
        self,
        sink: Optional[Handler] = None,
        *,
        sink_type: Optional[str] = None,
        sink_params: Optional[dict] = None,
        sink_trigger: Type[Message] = Record,
    ) -> Any:
        """Register a sink to broadcast messages to. Two ways to call this:

        - add_sink(sink): a custom Handler you wrote yourself -- its own
          @step methods decide what message type(s) it reacts to (and can
          react to several), exactly like add_handler, just broadcast (see
          below) instead of single-path routed.
        - add_sink(sink_type=..., sink_params=..., sink_trigger=...): builds
          a built-in sink from the registry (see register_sink_type, and
          pipeloom.dataio.sinks/parquet/kafka/db for what's available and
          what sink_params each expects) that fires on exactly one message
          type -- sink_trigger, Record by default. To also persist
          ErrorMessage, make a second, separate add_sink(..., sink_trigger=
          ErrorMessage) call -- a sink never handles more than one trigger.

        Either way: unlike add_handler's single-path, consume-based routing
        -- where only the first matching step ever sees a given message --
        every registered sink whose step accepts a message's type is called,
        so any number of sinks can be registered and all of them see every
        matching message. A message handled by at least one sink is treated
        as fully consumed: it never also reaches add_handler-routed steps or
        the built-in catch-all. A failure in one sink (turned into an
        ErrorMessage, fed back into the pipeline) does not stop the
        remaining sinks from also receiving the message.

        Returns the registered object: `sink` itself for the Handler path,
        or the constructed sink implementation for the sink_type path (so
        you can call, or await, its close())."""
        if sink is not None:
            if sink_type is not None:
                raise ValueError("add_sink: pass either sink or sink_type, not both")
            if sink in self._sinks:
                raise ValueError(f"{sink} is already registered")
            self._sinks.append(sink)
            self._dirty = True
            return sink

        if sink_type is None:
            raise ValueError("add_sink: pass either sink or sink_type")
        try:
            factory = _SINK_TYPES[sink_type]
        except KeyError:
            raise ValueError(
                f"Unknown sink_type {sink_type!r}. Registered: {sorted(_SINK_TYPES)}. "
                "Did you forget to import the module that registers it "
                "(pipeloom.dataio.sinks, .parquet, .kafka, or .db)?"
            ) from None
        impl = factory(**(sink_params or {}))
        self._sinks.append(_SinkRegistration(impl=impl, trigger=sink_trigger))
        self._dirty = True
        return impl

    def remove_sink(self, sink: Any) -> None:
        """Remove a sink previously registered via add_sink -- pass the same
        object add_sink returned (the Handler for the sink= path, or the
        constructed implementation for the sink_type= path)."""
        for entry in self._sinks:
            target = entry if isinstance(entry, Handler) else entry.impl
            if target is sink:
                self._sinks.remove(entry)
                self._dirty = True
                return
        raise ValueError(f"{sink} is not registered")

    def _rebuild(self) -> None:
        self._next_step_cache.clear()
        steps: List[StepInfo] = []
        for handler in self._handlers:
            steps += handler.get_steps()
        steps.sort(key=lambda s: s.stage)
        steps += self._catch_all.get_steps()  # always last, regardless of stage sort

        sink_steps: List[StepInfo] = []
        for entry in self._sinks:
            if isinstance(entry, Handler):
                sink_steps += entry.get_steps()
            else:
                impl = entry.impl
                doc = inspect.cleandoc(impl.write.__doc__) if impl.write.__doc__ else None
                sink_steps.append(
                    StepInfo(
                        name=f"{type(impl).__name__}.write",
                        stage=Stage.LOAD,
                        rank=next(_step_counter),
                        accepts=entry.trigger,
                        returns=type(None),
                        consume=True,
                        func=impl.write,
                        description=doc,
                    )
                )

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

        # Sink steps are matched directly by type in process(), not spliced into
        # the routed chain -- but anything a sink emits (including an ErrorMessage
        # from a failing sink) must still be able to reach a real handler, so each
        # one's `.next` points at the start of the main chain, same as `entry`.
        for i, s in enumerate(sink_steps, start=len(steps) + 1):
            s.idx = i
            s.next = entry.next

        self._entry = entry
        self._steps = steps
        self._sink_steps = sink_steps
        n = len(steps) + len(sink_steps) + 1
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

    def _apply_result(self, step: StepInfo, result: Any, stack: List[Tuple[StepInfo, Message]]) -> None:
        if result is None:
            return
        if isinstance(result, Message):
            self._emitted_counts[step.idx] += 1
            stack.append((step, result))
            return
        if hasattr(result, "__iter__"):
            results = list(result)
            for r in results:
                if not isinstance(r, Message):
                    raise PipelineError(f"{step} returned an iterable containing a non-Message value: {r!r}")
            self._emitted_counts[step.idx] += len(results)
            stack.extend((step, r) for r in reversed(results))
            return
        raise PipelineError(f"{step} returned an invalid value: {result!r}")

    async def process(self, message: Message) -> None:
        """Route a Message through the pipeline by type: each step runs when the
        message matches its annotated argument. A consume=True step swallows its
        input; otherwise the input is re-queued and keeps routing downstream. An
        exception inside a step becomes an ErrorMessage that continues routing
        from the failing step, so failures travel as data.

        Before routing, a message is offered to every add_sink-registered sink
        whose step accepts its type (see add_sink); if any sink accepts it, the
        message is considered consumed and never reaches add_handler-routed
        steps or the built-in catch-all."""
        if self._dirty:
            self._rebuild()
        assert self._entry is not None

        # Invariant the error recovery below depends on for progress: every value
        # on the stack is a Message, and `target` always holds a live StepInfo.
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

                    matched_sinks = [s for s in self._sink_steps if issubclass(msg_type, s.accepts)]
                    if matched_sinks:
                        for sink_step in matched_sinks:
                            start = time.perf_counter()
                            try:
                                result = sink_step.func(msg)
                                if inspect.isawaitable(result):
                                    result = await result
                            except (PipelineStopped, PipelineFailed):
                                raise
                            except Exception as exc:
                                self._total_calls[sink_step.idx] += 1
                                self._total_time[sink_step.idx] += time.perf_counter() - start
                                error = ErrorMessage(message=msg, exception=exc, reason=getattr(exc, "reason", None))
                                stack.append((sink_step, error))
                                continue  # a failing sink must not block the others
                            self._total_calls[sink_step.idx] += 1
                            self._total_time[sink_step.idx] += time.perf_counter() - start
                            self._apply_result(sink_step, result, stack)
                        continue

                    key = (producer, msg_type)
                    next_step = cache.get(key)
                    if next_step is None:
                        next_step = self._find_next(producer, msg_type)  # may raise
                        cache[key] = next_step
                    target = next_step

                    start = time.perf_counter()
                    result = target.func(msg)
                    if inspect.isawaitable(result):
                        result = await result
                    elapsed = time.perf_counter() - start

                    self._total_calls[target.idx] += 1
                    self._total_time[target.idx] += elapsed

                    if not target.consume:
                        stack.append((target, msg))

                    self._apply_result(target, result, stack)
            except (PipelineStopped, PipelineFailed):
                raise
            except Exception as exc:
                # getattr keeps this duck-typed: any exception exposing .reason
                # surfaces it, with no import of anything agent-specific.
                error_message = ErrorMessage(message=msg, exception=exc, reason=getattr(exc, "reason", None))
                stack.append((target, error_message))

    def get_profile_data(self) -> dict:
        if self._dirty:
            self._rebuild()
        assert self._entry is not None
        # Sorted by stage for display -- self._steps is already ascending, but
        # self._sink_steps (LOAD, typically) would otherwise land after the
        # catch-all (stage MONITOR + 1) since it's appended last internally.
        complete = sorted([self._entry] + self._steps + self._sink_steps, key=lambda s: s.stage)
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
                    "accepts": s.accepts.__name__,
                    "returns": ",".join(_return_type_names(s.returns)),
                    "consume": s.consume,
                    "description": s.description,
                    "calls": self._total_calls[s.idx],
                    "time": self._total_time[s.idx],
                    "emitted": self._emitted_counts[s.idx],
                    "emits": emits,
                }
            )
        return {"stages": list(self._stage_names), "pipeline": pipeline_entries}

    def format_profile(self, profile: dict, blueprint: bool = False) -> str:
        """Render the profile as one line per step: timing/call-count always;
        pass blueprint=True to also show each step's stage, accepted/emitted
        types, consume flag, per-call latency, and docstring inline -- a
        static picture of the pipeline's shape alongside the runtime numbers."""
        if not profile["pipeline"]:
            return "No Profile"
        total_time = sum(e["time"] for e in profile["pipeline"]) or 1.0

        if blueprint:
            stages_asc = sorted(profile["stages"])

            def stage_label(value: int) -> str:
                label = f"ENTRY - {value}"
                for stage_val, stage_name in stages_asc:
                    if stage_val <= value:
                        label = f"{stage_name} - {stage_val}"
                    else:
                        break
                return label

            name_width = max(len(f"{e['name']}({e['accepts']})") for e in profile["pipeline"])

            lines = ["\n"]
            current_stage = None
            for entry in profile["pipeline"]:
                label = stage_label(entry["stage"])
                if label != current_stage:
                    lines.append(f"STAGE: {label}")
                    current_stage = label
                pct = entry["time"] / total_time
                full_name = f"{entry['name']}({entry['accepts']})"
                consumed = "consumed" if entry["consume"] else ""
                returns = entry["returns"] or "None"
                percall_ms = (entry["time"] / entry["calls"] * 1000) if entry["calls"] else 0.0
                lines.append(
                    f"    {full_name.ljust(name_width)}  {consumed.ljust(9)} ---> {returns.ljust(20)} "
                    f"{entry['time']:7.4f}s {pct:6.1%} {entry['calls']:>4} calls "
                    f"{percall_ms:7.2f}ms/call -> {entry['emitted']:>4} emitted"
                )
                if entry["description"]:
                    lines.append(f"        {entry['description']}")
            return "\n".join(lines)

        lines = ["\n"]
        stages = sorted(profile["stages"], reverse=True)
        for entry in profile["pipeline"]:
            while stages and stages[-1][0] <= entry["stage"]:
                stage_val, stage_name = stages.pop()
                lines.append(f"[ {stage_name} = {stage_val} ]".ljust(50, "-"))
            pct = entry["time"] / total_time
            lines.append(
                f"{entry['name'].ljust(40)} {entry['time']:7.4f}s {pct:6.1%} "
                f"{entry['calls']:>4} calls -> {entry['emitted']:>4} emitted"
            )
        return "\n".join(lines)
