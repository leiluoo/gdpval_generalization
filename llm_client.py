"""
Thin wrapper around the claude CLI for use in synthesis scripts.
Uses subprocess to call `claude --print` so the packyapi auth works.
"""

import os
import re
import json
import subprocess
from pathlib import Path

CLAUDE_BIN = os.environ.get(
    "CLAUDE_CODE_EXECPATH",
    "/Users/allenouyang/.vscode/extensions/"
    "anthropic.claude-code-2.1.133-darwin-arm64/resources/native-binary/claude",
)


def call_claude(prompt: str, model: str | None = None) -> str:
    """
    Send a prompt to claude --print, return the text response.
    Raises subprocess.CalledProcessError on non-zero exit.
    """
    cmd = [CLAUDE_BIN, "--print"]
    if model:
        cmd += ["--model", model]

    result = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ},
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"claude CLI error (exit {result.returncode}):\n"
            f"stdout: {result.stdout[:500]}\n"
            f"stderr: {result.stderr[:500]}"
        )

    return result.stdout.strip()


def extract_json(text: str) -> dict:
    """Extract the first JSON object or ```json block from text."""
    m = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        return json.loads(m.group(0))
    raise ValueError(f"No JSON found in response:\n{text[:400]}")
