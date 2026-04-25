"""
Provider abstraction — swap between Groq, Gemini, Anthropic, OpenRouter
via the PROVIDER env var. All providers expose a single `chat()` call.
"""
import os

PROVIDER = os.getenv("PROVIDER", "groq").lower()

# Default models per provider
DEFAULTS = {
    "groq":       "llama-3.3-70b-versatile",
    "gemini":     "gemini-2.0-flash",
    "anthropic":  "claude-sonnet-4-20250514",
    "openrouter": "meta-llama/llama-3.3-70b-instruct:free",
}

MODEL = os.getenv("MODEL") or DEFAULTS.get(PROVIDER, "llama-3.3-70b-versatile")


def _openai_client():
    """Returns an openai.OpenAI client pointed at the right base URL."""
    from openai import OpenAI

    if PROVIDER == "groq":
        return OpenAI(
            api_key=os.environ["GROQ_API_KEY"],
            base_url="https://api.groq.com/openai/v1",
        )
    if PROVIDER == "openrouter":
        return OpenAI(
            api_key=os.environ["OPENROUTER_API_KEY"],
            base_url="https://openrouter.ai/api/v1",
        )
    raise ValueError(f"Provider {PROVIDER!r} is not OpenAI-compatible")


def chat(system: str, user: str, max_tokens: int = 4000) -> str:
    """Single unified chat call. Returns the assistant text."""

    if PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        resp = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return resp.content[0].text

    if PROVIDER == "gemini":
        import google.generativeai as genai
        genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
        model = genai.GenerativeModel(
            model_name=MODEL,
            system_instruction=system,
        )
        resp = model.generate_content(user)
        return resp.text

    # Groq / OpenRouter — both OpenAI-compatible
    client = _openai_client()
    resp = client.chat.completions.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    return resp.choices[0].message.content


def quick(prompt: str, max_tokens: int = 20) -> str:
    """Tiny call for single-word extractions (asset ticker parsing)."""
    return chat("You are a helpful assistant. Be extremely concise.", prompt, max_tokens=max_tokens)
