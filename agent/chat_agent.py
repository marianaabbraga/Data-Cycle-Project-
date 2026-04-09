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

You can:
- Check pipeline status (last runs, next scheduled run)
- Trigger pipeline runs (full or individual stages)
- Show row counts and preview data from bronze/silver outputs
- Read and analyse log files from each stage
- Inspect the chain-of-custody manifest (SHA-256 hashes)

Be concise. When showing data or logs, format them clearly.
If something failed, highlight the issue and suggest next steps.
"""

agent = marvin.Agent(
    name="Pipeline Assistant",
    instructions=SYSTEM_INSTRUCTIONS,
    tools=ALL_TOOLS,
    model=os.getenv("MARVIN_AGENT_MODEL", "openai:gpt-4o-mini"),
)
