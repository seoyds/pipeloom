class PipelineError(Exception):
    """Base class for all pipeloom errors."""


class RoutingError(PipelineError):
    """Raised when no downstream step accepts a given message type."""


class PipelineStopped(PipelineError):
    """Raised by a Handler to end the run successfully."""


class PipelineFailed(PipelineError):
    """Raised by a Handler, or the pipeline's built-in catch-all, to end the run with failure."""
