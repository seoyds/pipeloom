from pipeloom.exceptions import PipelineError


class AgentCallFailed(PipelineError):
    """Raised by AgentHandler when an agent call ends without success — either
    a non-retryable terminal task state, or a retryable one whose retry
    budget was exhausted. Caught by Pipeline.process like any other
    exception and converted into an ErrorMessage.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class AttemptFailed(Exception):
    """Internal to pipeloom.agents: a single attempt failed in a way that may
    be retried within AgentHandler's retry loop. Every AttemptFailed is by
    construction retryable — a non-retryable terminal state raises
    AgentCallFailed directly instead. Never escapes AgentHandler.call.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
