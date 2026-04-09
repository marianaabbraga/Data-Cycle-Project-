"""
Chat Agent — Marvin-powered conversational interface for the pipeline.

Usage:
    from agent.chat_agent import agent
    response = agent.say("Did the last run succeed?")
"""

import os
import marvin
from agent.tools import ALL_TOOLS

SYSTEM_INSTRUCTIONS = """\
You are the Data Pipeline Assistant. You help users monitor and control
a Bronze-Silver-Gold data pipeline orchestrated by Prefect.

Available tools (you MUST call these — never fabricate results):
  - get_pipeline_status  → query Prefect for recent run states
  - trigger_pipeline     → create a new pipeline run via the Prefect API
  - get_row_counts       → count rows in bronze/silver parquet files
  - preview_data         → show sample rows from a stage
  - get_latest_logs      → read a stage's log file
  - analyse_logs         → read all logs + chain-of-custody manifest

CRITICAL RULES:
1. When the user asks to run/trigger the pipeline, call trigger_pipeline
   and report the Run ID it returns. NEVER say "initiated" without calling
   the tool first.
2. When the user asks about status, rows, logs, or data, call the
   corresponding tool and relay its output. Do NOT guess or fabricate data.
3. Be concise. Format data and logs clearly.
4. If something failed, highlight the issue and suggest next steps.
"""

def _make_ollama_provider(base_url: str, api_key: str):
    """Create an OpenAI-compatible provider with a patched client that
    replaces null message content with empty strings.  Ollama's API
    rejects ``content: null`` which pydantic-ai sends in tool-call
    message sequences.
    """
    from openai import AsyncOpenAI
    from pydantic_ai.providers.openai import OpenAIProvider

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    _original_create = client.chat.completions.create

    async def _patched_create(*args, **kwargs):
        messages = kwargs.get("messages")
        if messages:
            for msg in messages:
                if isinstance(msg, dict) and msg.get("content") is None:
                    msg["content"] = ""
        return await _original_create(*args, **kwargs)

    client.chat.completions.create = _patched_create  # type: ignore[assignment]
    return OpenAIProvider(openai_client=client)


def _resolve_agent_model():
    """
    Prefer an explicit Ollama model name when configured, while keeping Marvin's
    global settings valid at import time.
    """
    base_url = os.getenv("OPENAI_BASE_URL")
    ollama_model = os.getenv("OLLAMA_MODEL")

    if base_url and ollama_model:
        from pydantic_ai.models.openai import OpenAIModel

        provider = _make_ollama_provider(base_url, os.getenv("OPENAI_API_KEY", "ollama"))
        return OpenAIModel(ollama_model, provider=provider)

    return os.getenv("MARVIN_AGENT_MODEL", "openai:gpt-4o-mini")


def _ollama_supports_tools() -> bool:
    """
    Some Ollama models reject tool/function calling. Allow opting in via env.
    """
    return os.getenv("OLLAMA_SUPPORTS_TOOLS", "").strip().lower() in ("1", "true", "yes", "y", "on")


def _agent_retry_budget() -> int:
    """Marvin/pydantic-ai default is 1; local models often need more output-validation retries."""
    raw = os.getenv("MARVIN_AGENT_RETRIES", "5").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 5
    return max(1, min(n, 20))


class PipelineAssistantAgent(marvin.Agent):
    """Marvin Agent with higher pydantic-ai retry budget for tool + EndTurn validation."""

    async def get_agentlet(self, tools, end_turn_tools, active_mcp_servers=None):  # type: ignore[override]
        agentlet = await super().get_agentlet(tools, end_turn_tools, active_mcp_servers)
        retries = _agent_retry_budget()
        agentlet._max_result_retries = retries
        agentlet._max_tool_retries = retries
        if agentlet._output_toolset is not None:
            agentlet._output_toolset.max_retries = retries
        agentlet._function_toolset.max_retries = retries
        return agentlet


agent = PipelineAssistantAgent(
    name="Pipeline Assistant",
    instructions=SYSTEM_INSTRUCTIONS,
    tools=ALL_TOOLS if _ollama_supports_tools() or not os.getenv("OLLAMA_MODEL") else [],
    model=_resolve_agent_model(),
)
