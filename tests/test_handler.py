import typing

import pytest

from pipeloom.core import step, Handler, StepInfo
from pipeloom.messages import ErrorMessage, Message, RawRecord, Record
from pipeloom.stages import Stage


class ExampleHandler(Handler):
    @step(Stage.TRANSFORM, consume=True)
    def transform(self, message: RawRecord) -> Record:
        """Turn a RawRecord into a Record."""
        return Record(message.value)


def test_get_steps_returns_one_step_info():
    handler = ExampleHandler()
    steps = handler.get_steps()
    assert len(steps) == 1
    info = steps[0]
    assert isinstance(info, StepInfo)
    assert info.stage == Stage.TRANSFORM
    assert info.accepts is RawRecord
    assert info.returns is Record
    assert info.consume is True
    assert info.description == "Turn a RawRecord into a Record."
    assert info.name == "ExampleHandler.transform"


def test_get_steps_calls_the_underlying_bound_method():
    handler = ExampleHandler()
    info = handler.get_steps()[0]
    result = info.func(RawRecord(value={"x": 1}, time=0.0))
    assert result == {"x": 1}


def test_handler_with_no_steps_raises():
    class EmptyHandler(Handler):
        pass

    with pytest.raises(AssertionError):
        EmptyHandler().get_steps()


def test_out_of_order_stages_within_one_handler_raises():
    class BadHandler(Handler):
        @step(Stage.TRANSFORM)
        def second(self, message: RawRecord) -> None:
            return None

        @step(Stage.EXTRACT)
        def first(self, message: RawRecord) -> None:
            return None

    with pytest.raises(AssertionError):
        BadHandler().get_steps()


def test_step_requires_annotated_return_type():
    with pytest.raises(AssertionError):
        class MissingReturn(Handler):
            @step(Stage.EXTRACT)
            def go(self, message: RawRecord):
                return None


def test_step_requires_exactly_two_arguments():
    with pytest.raises(AssertionError):
        class TooManyArgs(Handler):
            @step(Stage.EXTRACT)
            def go(self, message: RawRecord, extra: int) -> None:
                return None


def test_step_requires_message_subclass_argument():
    with pytest.raises(AssertionError):
        class BadArgType(Handler):
            @step(Stage.EXTRACT)
            def go(self, message: int) -> None:
                return None


def test_step_accepts_a_pep604_union_return_type():
    class Pep604(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> Record | ErrorMessage:
            return Record(message.value)

    assert Pep604().get_steps()[0].returns == Record | ErrorMessage


def test_step_accepts_a_typing_union_return_type():
    class TypingUnion(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> typing.Union[Record, ErrorMessage]:
            return Record(message.value)

    assert TypingUnion().get_steps()[0].returns == typing.Union[Record, ErrorMessage]


def test_step_accepts_an_optional_return_type():
    class OptionalReturn(Handler):
        @step(Stage.TRANSFORM, consume=True)
        def go(self, message: RawRecord) -> typing.Optional[Record]:
            return None

    assert OptionalReturn().get_steps()[0].returns == typing.Optional[Record]
