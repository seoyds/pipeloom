import time as time_module

from pipeloom.events import (
    Message,
    RawRecord,
    Record,
    ErrorMessage,
    Tick,
    Heartbeat,
    Checkpoint,
    Shutdown,
)


def test_raw_record_is_a_message():
    r = RawRecord(value={"a": 1}, time=1.0)
    assert isinstance(r, Message)
    assert r.value == {"a": 1}
    assert r.time == 1.0


def test_record_behaves_like_a_dict():
    r = Record({"a": 1, "b": 2})
    assert isinstance(r, Message)
    assert r["a"] == 1
    assert dict(r) == {"a": 1, "b": 2}


def test_error_message_carries_original_message_and_exception():
    original = RawRecord(value={}, time=0.0)
    exc = ValueError("boom")
    err = ErrorMessage(message=original, exception=exc)
    assert isinstance(err, Message)
    assert err.message is original
    assert err.exception is exc
    assert err.reason is None


def test_error_message_reason_is_settable():
    err = ErrorMessage(message=RawRecord(value={}, time=0.0), exception=ValueError(), reason="timeout")
    assert err.reason == "timeout"


def test_tick_defaults_time_to_now():
    before = time_module.time()
    tick = Tick()
    after = time_module.time()
    assert before <= tick.time <= after


def test_heartbeat_has_time_and_prev_time():
    hb = Heartbeat(time=10.0, prev_time=5.0)
    assert hb.time == 10.0
    assert hb.prev_time == 5.0


def test_checkpoint_and_shutdown_are_messages():
    assert isinstance(Checkpoint(time=1.0), Message)
    assert isinstance(Shutdown(time=1.0), Message)
