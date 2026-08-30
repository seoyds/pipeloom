from __future__ import annotations

import inspect
import itertools
import typing
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Type, Union

from pipeloom.events import Message

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
        annotations = spec.annotations

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
