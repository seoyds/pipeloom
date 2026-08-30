from pipeloom.exceptions import PipelineError, RoutingError, PipelineStopped, PipelineFailed


def test_routing_error_is_a_pipeline_error():
    assert issubclass(RoutingError, PipelineError)


def test_pipeline_stopped_is_a_pipeline_error():
    assert issubclass(PipelineStopped, PipelineError)


def test_pipeline_failed_is_a_pipeline_error():
    assert issubclass(PipelineFailed, PipelineError)


def test_pipeline_error_carries_message():
    err = RoutingError("no handler for Foo")
    assert str(err) == "no handler for Foo"
