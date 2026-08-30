from pipeloom.exceptions import PipelineError
from pipeloom.agents.exceptions import AgentCallFailed, AttemptFailed


def test_agent_call_failed_is_a_pipeline_error():
    assert issubclass(AgentCallFailed, PipelineError)


def test_agent_call_failed_carries_reason_and_message():
    err = AgentCallFailed(reason="timeout", message="gave up after 3 attempts")
    assert err.reason == "timeout"
    assert str(err) == "gave up after 3 attempts"


def test_attempt_failed_is_a_plain_exception_not_a_pipeline_error():
    assert issubclass(AttemptFailed, Exception)
    assert not issubclass(AttemptFailed, PipelineError)


def test_attempt_failed_carries_reason():
    err = AttemptFailed(reason="failed")
    assert err.reason == "failed"
