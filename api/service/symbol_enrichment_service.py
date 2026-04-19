from __future__ import annotations

import hashlib
import json
from uuid import uuid4

from asset_allocation_contracts.ai_chat import AiChatRequest
from asset_allocation_contracts.symbol_enrichment import (
    SymbolEnrichmentResolveRequest,
    SymbolEnrichmentResolveResponse,
    SymbolProfileValues,
)

from api.service.auth import AuthContext
from api.service.openai_responses_gateway import OpenAIResponsesGateway


def _request_fingerprint(payload: SymbolEnrichmentResolveRequest) -> str:
    digest = hashlib.sha256(
        json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return digest[:24]


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return stripped


def _extract_json_object(text: str) -> dict:
    stripped = _strip_code_fences(text)
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("Symbol enrichment resolve did not return a JSON object.")
        payload = json.loads(stripped[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("Symbol enrichment resolve did not return a JSON object.")
    return payload


def _build_prompt(payload: SymbolEnrichmentResolveRequest) -> str:
    requested_fields = ", ".join(payload.requestedFields)
    provider_facts = json.dumps(payload.providerFacts.model_dump(mode="json"), indent=2, sort_keys=True)
    current_profile = json.dumps(
        (payload.currentProfile or SymbolProfileValues()).model_dump(mode="json"),
        indent=2,
        sort_keys=True,
    )
    schema = {
        "symbol": payload.symbol,
        "profile": {field: None for field in payload.requestedFields},
        "model": "<model-name>",
        "confidence": 0.0,
        "sourceFingerprint": _request_fingerprint(payload),
        "warnings": [],
    }
    return (
        "You are enriching symbol metadata for a trading control plane.\n"
        "Return only a JSON object. Do not use markdown code fences.\n"
        "Use null for unknown fields. Do not invent fast-moving market data.\n"
        "Respect provider facts exactly and never change the symbol.\n"
        "Keep issuer_summary_short under 280 characters and factual.\n\n"
        f"Requested fields: {requested_fields}\n"
        f"Overwrite mode: {payload.overwriteMode}\n\n"
        "Provider facts:\n"
        f"{provider_facts}\n\n"
        "Current profile:\n"
        f"{current_profile}\n\n"
        "Return this JSON shape:\n"
        f"{json.dumps(schema, indent=2, sort_keys=True)}"
    )


async def resolve_symbol_profile(
    *,
    gateway: OpenAIResponsesGateway,
    auth_context: AuthContext,
    request_payload: SymbolEnrichmentResolveRequest,
    model_name: str,
) -> SymbolEnrichmentResolveResponse:
    prompt = _build_prompt(request_payload)
    output = await gateway.generate_text_response(
        request_id=str(uuid4()),
        auth_context=auth_context,
        chat_request=AiChatRequest(prompt=prompt),
        attachments=[],
        model_override=model_name,
    )
    raw = _extract_json_object(output)
    if "profile" not in raw:
        raw["profile"] = {}
    raw["symbol"] = request_payload.symbol
    raw.setdefault("model", model_name)
    raw.setdefault("sourceFingerprint", _request_fingerprint(request_payload))
    raw.setdefault("warnings", [])
    validated = SymbolEnrichmentResolveResponse.model_validate(raw)
    if validated.symbol != request_payload.symbol:
        raise ValueError("Resolved symbol did not match the requested symbol.")
    filtered_profile = SymbolProfileValues.model_validate(
        {
            field: getattr(validated.profile, field)
            for field in request_payload.requestedFields
            if getattr(validated.profile, field) is not None
        }
    )
    return validated.model_copy(
        update={
            "symbol": request_payload.symbol,
            "profile": filtered_profile,
        }
    )
