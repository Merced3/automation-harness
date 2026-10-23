"""Minimal asynchronous client for Pi's newline-delimited JSON RPC mode.

Extracted into the harness 2026-10-29 (Principle #3: third real consumer —
socratic-partner, teaching-agent, dev-agent). Merges the two drifted copies:

- socratic-partner ran Pi ISOLATED (--no-tools/--no-extensions/...): the
  model is the whole product. Pass ``isolated=True`` for that spawn mode.
- teaching-agent runs Pi WITH its normal file tools and context files in a
  pinned working directory (behavior lives in markdown; Pi reads/writes it
  directly), and added: named sessions, a tool allowlist, extra CLI
  arguments, the Windows batch-shim bypass, and abort().

Code here is plumbing; what the agent IS lives in the consuming project.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

_SUBPROCESS_STREAM_LIMIT = 8 * 1024 * 1024

# socratic-partner's spawn mode: no tools, no user config, the model is the
# whole product. teaching-agent/dev-agent want the opposite (full tools).
_ISOLATED_FLAGS = (
    "--no-tools",
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-context-files",
)


class PiRpcError(RuntimeError):
    """Raised when Pi cannot complete an RPC operation."""


@dataclass(frozen=True, slots=True)
class PiRunResult:
    text: str
    session_id: str
    session_file: str | None
    provider: str | None
    model_id: str | None
    input_tokens: int
    output_tokens: int
    cost: float


class PiRpcClient:
    """Own one persistent Pi RPC subprocess and serialize model runs through it."""

    def __init__(
        self,
        *,
        executable: str,
        working_directory: Path,
        session_directory: Path,
        session_name: str = "pi",
        session_file: str | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
        isolated: bool = False,
        extra_arguments: Sequence[str] = (),
        timeout_seconds: int = 300,
    ) -> None:
        self.executable = executable
        self.working_directory = working_directory
        self.session_directory = session_directory
        self.session_name = session_name
        self.session_file = session_file
        self.model = model
        self.system_prompt = system_prompt
        self.isolated = isolated
        # Tool allowlist for the subprocess (pi --tools). None = pi defaults
        # (read, bash, edit, write). Consumers narrow this to enforce a write
        # boundary by tool surface, not by model cooperation. Ignored when
        # isolated=True (tools are already off).
        self.tools: tuple[str, ...] | None = None
        self.extra_arguments = tuple(extra_arguments)
        self.timeout_seconds = timeout_seconds
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._run_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    async def start(self) -> None:
        if self.is_running:
            return

        executable = shutil.which(self.executable)
        if executable is None:
            raise PiRpcError(f"Pi executable was not found: {self.executable}")

        self.session_directory.mkdir(parents=True, exist_ok=True)
        arguments = [
            *self._resolve_launcher(executable),
            "--mode",
            "rpc",
            "--session-dir",
            str(self.session_directory),
            "--name",
            self.session_name,
        ]
        if self.isolated:
            arguments.extend(_ISOLATED_FLAGS)
        if self.session_file:
            arguments.extend(("--session", self.session_file))
        if self.model:
            arguments.extend(("--model", self.model))
        if self.system_prompt:
            arguments.extend(("--system-prompt", self.system_prompt))
        if self.tools and not self.isolated:
            arguments.extend(("--tools", ",".join(self.tools)))
        arguments.extend(self.extra_arguments)

        logger.info(
            "Starting Pi RPC process (session name: %s, isolated=%s, tools=%s, extra=%s).",
            self.session_name,
            self.isolated,
            self.tools,
            self.extra_arguments,
        )
        try:
            self._process = await asyncio.create_subprocess_exec(
                *arguments,
                cwd=self.working_directory,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_SUBPROCESS_STREAM_LIMIT,
                creationflags=_subprocess_creationflags(),
                # The laptop user's bash-guard extension confirms risky bash
                # via TUI; a headless pi needs the auto-allow escape hatch or
                # even `git add` blocks. CLI flags don't reach extension
                # getFlag() in RPC mode — the env var does.
                env={**os.environ, "PI_BASH_GUARD_AUTO_ALLOW": "1"},
            )
        except OSError as exc:
            raise PiRpcError(f"Could not start Pi: {exc}") from exc

        self._stdout_task = asyncio.create_task(self._read_stdout(), name="pi-rpc-stdout")
        self._stderr_task = asyncio.create_task(self._read_stderr(), name="pi-rpc-stderr")

        try:
            await self.get_state()
        except Exception:
            await self.close()
            raise

    @staticmethod
    def _resolve_launcher(executable: str) -> list[str]:
        """On Windows, `pi` resolves to a pi.CMD batch shim, and batch `%*`
        forwarding destroys multi-line argument values: the system prompt is
        multi-line, so every argument after `--system-prompt` (--tools,
        --extension) was silently cut (probe.txt leak, 2026-10-03). Bypass
        the shim: invoke node on the bundled cli.js directly."""
        if sys.platform == "win32" and executable.lower().endswith((".cmd", ".bat")):
            cli_js = (
                Path(executable).parent
                / "node_modules"
                / "@earendil-works"
                / "pi-coding-agent"
                / "dist"
                / "bundle"
                / "cli.js"
            )
            node = shutil.which("node")
            if node is not None and cli_js.is_file():
                return [node, str(cli_js)]
            # Shim found but layout unknown: keep the shim rather than fail
            # to launch; the consumer's fail-closed checks keep any resulting
            # boundary loss visible.
        return [executable]

    async def close(self) -> None:
        process = self._process
        if process is None:
            return

        logger.info("Stopping Pi RPC process.")
        if process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
            with suppress(BrokenPipeError, ConnectionResetError):
                await process.stdin.wait_closed()

        if process.returncode is None:
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5)
                except TimeoutError:
                    process.kill()
                    await process.wait()

        tasks = tuple(task for task in (self._stdout_task, self._stderr_task) if task is not None)
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5)
            except TimeoutError:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        self._fail_pending(PiRpcError("Pi RPC process closed."))
        self._process = None
        self._stdout_task = None
        self._stderr_task = None
        self._discard_queued_events()

    async def get_state(self) -> dict[str, Any]:
        response = await self._request({"type": "get_state"})
        return _response_data(response)

    async def get_available_models(self) -> list[dict[str, Any]]:
        """List the models Pi can switch to, as raw Model objects."""
        async with self._run_lock:
            await self.start()
            response = await self._request({"type": "get_available_models"})
        data = _response_data(response)
        models = data.get("models")
        if not isinstance(models, list):
            return []
        return [model for model in models if isinstance(model, dict)]

    async def set_model(self, provider: str, model_id: str) -> dict[str, Any]:
        """Switch the model used by subsequent runs; returns the new Model object."""
        async with self._run_lock:
            await self.start()
            response = await self._request(
                {"type": "set_model", "provider": provider, "modelId": model_id}
            )
            model = _response_data(response)
        self.model = f"{provider}/{model_id}"
        return model

    async def abort(self) -> None:
        """Abort the active run and wait for the session to become idle
        (RPC `abort`). Needed because cancelling the client-side await of
        prompt() does NOT stop pi — it keeps streaming the dead run, and
        the next prompt then fails with 'Agent is already processing'.
        Deliberately takes no run lock: the whole point is to interrupt a
        run whose prompt() call was already cancelled (lock released) or
        is still held elsewhere; _request correlates by id either way."""
        await self._request({"type": "abort"})

    async def new_session(self) -> dict[str, Any]:
        async with self._run_lock:
            await self.start()
            await self._request({"type": "new_session"})
            state = await self.get_state()
            session_file = state.get("sessionFile")
            self.session_file = session_file if isinstance(session_file, str) else None
            return state

    async def prompt(self, message: str) -> PiRunResult:
        async with self._run_lock:
            await self.start()
            self._discard_queued_events()
            await self._request({"type": "prompt", "message": message})
            assistant = await self._wait_until_settled()
            _raise_for_assistant_error(assistant)

            state_response = await self._request({"type": "get_state"})
            stats_response = await self._request({"type": "get_session_stats"})
            text = _assistant_text(assistant)
            if not text.strip():
                raise PiRpcError(_empty_text_detail(assistant))

            state = _response_data(state_response)
            stats = _response_data(stats_response)
            model = state.get("model") or {}
            tokens = stats.get("tokens") or {}
            session_file = state.get("sessionFile")
            self.session_file = session_file if isinstance(session_file, str) else None

            return PiRunResult(
                text=text.strip(),
                session_id=str(state.get("sessionId", "")),
                session_file=self.session_file,
                provider=_optional_string(model.get("provider")),
                model_id=_optional_string(model.get("id")),
                input_tokens=_integer(tokens.get("input")),
                output_tokens=_integer(tokens.get("output")),
                cost=float(stats.get("cost") or 0),
            )

    async def _request(self, command: dict[str, Any]) -> dict[str, Any]:
        if not self.is_running:
            if command.get("type") == "get_state":
                raise PiRpcError("Pi RPC process is not running.")
            await self.start()

        await self._ensure_reader_healthy()
        process = self._process
        if process is None or process.stdin is None:
            raise PiRpcError("Pi RPC stdin is unavailable.")

        request_id = uuid4().hex
        payload = {"id": request_id, **command}
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        try:
            encoded = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
            async with self._write_lock:
                process.stdin.write(encoded)
                await process.stdin.drain()
            response = await asyncio.wait_for(future, timeout=self.timeout_seconds)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            await self.close()
            raise PiRpcError(
                f"Pi RPC command timed out and the process was reset: {command.get('type')}"
            ) from exc
        finally:
            self._pending.pop(request_id, None)

        if not response.get("success", False):
            raise PiRpcError(str(response.get("error") or "Unknown Pi RPC error"))
        return response

    async def _wait_until_settled(self) -> dict[str, Any]:
        async def wait() -> dict[str, Any]:
            assistant: dict[str, Any] | None = None
            while True:
                event = await self._events.get()
                if event.get("type") == "message_end":
                    message = event.get("message")
                    if isinstance(message, dict) and message.get("role") == "assistant":
                        assistant = message
                elif event.get("type") == "agent_settled":
                    if assistant is None:
                        raise PiRpcError("Pi settled without a final assistant message.")
                    return assistant
                elif event.get("type") == "process_exited":
                    raise PiRpcError("Pi exited before the agent settled.")
                elif event.get("type") == "reader_failed":
                    raise PiRpcError(str(event.get("error") or "Pi RPC reader failed."))

        try:
            return await asyncio.wait_for(wait(), timeout=self.timeout_seconds)
        except TimeoutError as exc:
            await self.close()
            raise PiRpcError("Pi agent run timed out and the process was reset.") from exc

    async def _read_stdout(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            while line := await process.stdout.readline():
                try:
                    payload = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    logger.warning("Ignored malformed output from Pi RPC.")
                    continue
                request_id = payload.get("id")
                if payload.get("type") == "response" and isinstance(request_id, str):
                    future = self._pending.get(request_id)
                    if future is not None and not future.done():
                        future.set_result(payload)
                else:
                    await self._events.put(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Pi RPC stdout reader failed.")
            error = PiRpcError(f"Pi RPC stdout reader failed: {exc}")
            self._fail_pending(error)
            await self._events.put({"type": "reader_failed", "error": str(error)})
            return

        return_code = await process.wait()
        error = PiRpcError(f"Pi RPC process exited with code {return_code}.")
        self._fail_pending(error)
        await self._events.put({"type": "process_exited", "returncode": return_code})

    async def _read_stderr(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        while line := await process.stderr.readline():
            logger.warning("Pi stderr: %s", line.decode(errors="replace").rstrip())

    async def _ensure_reader_healthy(self) -> None:
        task = self._stdout_task
        if task is None or not task.done():
            return
        error: BaseException | None = None
        if not task.cancelled():
            error = task.exception()
        await self.close()
        detail = f": {error}" if error else ""
        raise PiRpcError(f"Pi RPC stdout reader is not running{detail}.")

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)

    def _discard_queued_events(self) -> None:
        while not self._events.empty():
            try:
                self._events.get_nowait()
            except asyncio.QueueEmpty:
                return


def _subprocess_creationflags() -> int:
    """Hide the Pi console window on Windows.

    Without CREATE_NO_WINDOW, Windows allocates a visible console for the Pi
    subprocess. Closing that console kills Pi, and the next prompt then spawns
    a new window — the observed pop-up/pop-away behavior. Pipes are unaffected.
    """
    if sys.platform == "win32":
        # getattr: subprocess only defines the constant on Windows, but tests
        # simulate win32 on any platform. The value (0x08000000) is fixed by
        # Windows itself, so the fallback is exact.
        return getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return 0


def _empty_text_detail(assistant: dict[str, Any]) -> str:
    stop_reason = assistant.get("stopReason")
    content = assistant.get("content")
    if isinstance(content, list):
        block_types = [str(item.get("type")) for item in content if isinstance(item, dict)]
        shape = ",".join(block_types) or "empty-list"
    else:
        shape = type(content).__name__
    return f"Pi settled without a text response (stopReason={stop_reason!r}, content=[{shape}])."


def _raise_for_assistant_error(assistant: dict[str, Any]) -> None:
    if assistant.get("role") != "assistant":
        raise PiRpcError("Pi final message was not an assistant message.")
    if assistant.get("stopReason") != "error":
        return

    error_message = assistant.get("errorMessage")
    if not isinstance(error_message, str) or not error_message.strip():
        error_message = _assistant_text(assistant) or "Pi reported an unspecified provider error."
    raise PiRpcError(error_message.strip())


def _assistant_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n".join(
        str(item.get("text"))
        for item in content
        if isinstance(item, dict) and item.get("type") == "text" and item.get("text")
    )


def _response_data(response: dict[str, Any]) -> dict[str, Any]:
    data = response.get("data")
    if not isinstance(data, dict):
        raise PiRpcError("Pi RPC response did not contain an object payload.")
    return data


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _integer(value: Any) -> int:
    return int(value) if isinstance(value, int | float) else 0
