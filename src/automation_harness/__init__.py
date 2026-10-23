"""Reusable infrastructure for purpose-driven automations."""

from .config import HarnessConfig
from .pi_rpc import PiRpcClient, PiRpcError, PiRunResult
from .runtime import AlreadyRunningError, Harness, JobContext, ServiceContext
from .store import Store

__version__ = "0.4.0"

__all__ = [
    "AlreadyRunningError",
    "Harness",
    "HarnessConfig",
    "JobContext",
    "PiRpcClient",
    "PiRpcError",
    "PiRunResult",
    "ServiceContext",
    "Store",
    "__version__",
]
