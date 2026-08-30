"""Optional AI-agent-backed Handlers reached over the A2A protocol.

Nothing in pipeloom.core, pipeloom.messages, pipeloom.exceptions, pipeloom.stages,
or pipeloom.dataio imports from this package. This package freely imports from
those, but never the reverse.
"""

from pipeloom.agents.agent_handler import AgentCallMetrics, AgentHandler
from pipeloom.agents.exceptions import AgentCallFailed, AttemptFailed
from pipeloom.agents.retry import RetryPolicy

__all__ = [
    "AgentHandler",
    "AgentCallMetrics",
    "RetryPolicy",
    "AgentCallFailed",
    "AttemptFailed",
]
