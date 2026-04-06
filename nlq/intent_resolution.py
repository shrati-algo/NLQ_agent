import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Tuple

from nlq.query_logging import derive_intent


TEXT_TYPES = ("char", "text", "varchar")
STOPWORDS = {
    "a", "an", "are", "be", "can", "count", "different", "do", "does", "for",
    "from", "give", "have", "how", "in", "is", "list", "many", "me", "my",
    "name", "of", "show", "tell", "the", "there", "what", "which",
}

PHRASE_SYNONYMS = {
    "semifinished product": [
        "semifinished product",
        "semifinished products",
        "semi finished product",
        "semi finished products",
        "semi-finished product",
        "semi-finished products",
    ],
    "finished product": [
        "finished product",
        "finished products",
    ],
    "raw material": [
        "raw material",
        "raw materials",
    ],
    "material type": [
        "material type",
        "material types",
    ],
}


def resolve_query_context(
    question: str,
    metadata_catalog: Dict[str, Any],
) -> Dict[str, Any]:
    schema_tables = {
        table_name: {
            column_name: column_info["data_type"]
            for column_name, column_info in table_info.get("columns", {}).items()
        }
        for table_name, table_info in metadata_catalog.get("tables", {}).items()
    }
    intent = derive_intent(question, schema_tables)
    candidate_phrases = extract_candidate_phrases(question)
    text_columns = _get_text_columns(metadata_catalog)
    scored_columns = _score_columns(question, candidate_phrases, text_columns)
    top_columns = [item["column"] for item in scored_columns[:5]]

    distinct_samples: Dict[str, List[str]] = {}
    all_matches: List[Dict[str, Any]] = []

    for table_name, column_name in top_columns:
        column_info = metadata_catalog["tables"][table_name]["columns"][column_name]
        values = column_info.get("distinct_values", [])
        distinct_samples[f"{table_name}.{column_name}"] = values[:20]
        all_matches.extend(_score_values(table_name, column_name, candidate_phrases, values))

    all_matches.sort(key=lambda item: item["score"], reverse=True)
    best_match = all_matches[0] if all_matches else None

    return {
        "intent": {
            **intent,
            "candidate_phrases": candidate_phrases,
            "candidate_columns": [item["column"][1] for item in scored_columns[:5]],
        },
        "value_resolution": {
            "candidate_phrases": candidate_phrases,
            "candidate_columns": [
                {
                    "table": table_name,
                    "column": column_name,
                    "score": round(score_info["score"], 3),
                }
                for score_info in scored_columns[:5]
                for table_name, column_name in [score_info["column"]]
            ],
            "matched_table": best_match["table"] if best_match else None,
            "matched_column": best_match["column"] if best_match else None,
            "matched_value": best_match["value"] if best_match else None,
            "matched_phrase": best_match["phrase"] if best_match else None,
            "match_score": round(best_match["score"], 3) if best_match else 0.0,
            "confidence": _confidence_label(best_match["score"]) if best_match else "none",
            "top_matches": [
                {
                    "table": match["table"],
                    "column": match["column"],
                    "value": match["value"],
                    "phrase": match["phrase"],
                    "score": round(match["score"], 3),
                }
                for match in all_matches[:5]
            ],
            "distinct_value_samples": distinct_samples,
        },
    }


def extract_candidate_phrases(question: str) -> List[str]:
    q = question.strip().rstrip("?.! ")
    q_lower = q.lower()
    phrases: List[str] = []

    pattern_matches = [
        re.search(r"how many\s+(.+?)\s+are there(?:\s+in\s+(.+))?$", q_lower),
        re.search(r"which\s+(.+?)\s+(?:is|are|have|has)(?:\s+(.+))?$", q_lower),
        re.search(r"name\s+(.+?)(?:\s+in\s+(.+))?$", q_lower),
    ]

    for match in pattern_matches:
        if not match:
            continue
        for group in match.groups():
            if group:
                phrases.append(group.strip())

    cleaned = _clean_phrase(q_lower)
    if cleaned:
        phrases.append(cleaned)

    deduped: List[str] = []
    for phrase in phrases:
        normalized = _normalize_text(phrase)
        if normalized and normalized not in {_normalize_text(item) for item in deduped}:
            deduped.append(phrase)

    return deduped[:6]


def _clean_phrase(question: str) -> str:
    cleaned = re.sub(
        r"\b(how many|which|what|show|list|give me|tell me|name|count|are there|is there|in my database|in database)\b",
        " ",
        question,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _get_text_columns(metadata_catalog: Dict[str, Any]) -> List[Tuple[str, str]]:
    columns: List[Tuple[str, str]] = []
    for table_name, table_info in metadata_catalog.get("tables", {}).items():
        for column_name, column_info in table_info.get("columns", {}).items():
            if column_info.get("is_text"):
                columns.append((table_name, column_name))
    return columns


def _score_columns(
    question: str,
    candidate_phrases: List[str],
    text_columns: List[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    question_norm = _normalize_text(question)
    scored: List[Dict[str, Any]] = []

    for table_name, column_name in text_columns:
        column_norm = _normalize_text(column_name)
        score = 0.0

        if column_norm in question_norm:
            score += 0.75

        for phrase in candidate_phrases:
            phrase_norm = _normalize_text(phrase)
            score += _token_overlap_score(phrase_norm, column_norm) * 0.7

        score += SequenceMatcher(None, question_norm, column_norm).ratio() * 0.2

        if "type" in column_norm and "type" in question_norm:
            score += 0.25
        if "product" in column_norm and "product" in question_norm:
            score += 0.25
        if "material" in column_norm and "material" in question_norm:
            score += 0.25

        scored.append({"column": (table_name, column_name), "score": score})

    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored


def _score_values(
    table_name: str,
    column_name: str,
    candidate_phrases: List[str],
    values: List[str],
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for phrase in candidate_phrases:
        phrase_variants = {_normalize_text(phrase), *_expand_phrase_variants(phrase)}
        for value in values:
            value_norm = _normalize_text(value)
            best_score = 0.0
            for phrase_variant in phrase_variants:
                best_score = max(best_score, _phrase_value_score(phrase_variant, value_norm))
            if best_score >= 0.45:
                matches.append(
                    {
                        "table": table_name,
                        "column": column_name,
                        "phrase": phrase,
                        "value": value,
                        "score": best_score,
                    }
                )
    return matches


def _expand_phrase_variants(phrase: str) -> List[str]:
    phrase_norm = _normalize_text(phrase)
    variants = {phrase_norm}

    for canonical, aliases in PHRASE_SYNONYMS.items():
        canonical_norm = _normalize_text(canonical)
        alias_norms = {_normalize_text(alias) for alias in aliases}
        if phrase_norm == canonical_norm or phrase_norm in alias_norms:
            variants.add(canonical_norm)
            variants.update(alias_norms)

    return sorted(variants)


def _phrase_value_score(phrase_norm: str, value_norm: str) -> float:
    if not phrase_norm or not value_norm:
        return 0.0
    if phrase_norm == value_norm:
        return 1.0
    if phrase_norm.replace(" ", "") == value_norm.replace(" ", ""):
        return 0.98

    overlap = _token_overlap_score(phrase_norm, value_norm)
    ratio = SequenceMatcher(None, phrase_norm, value_norm).ratio()

    if phrase_norm in value_norm or value_norm in phrase_norm:
        ratio += 0.15

    return min(1.0, overlap * 0.6 + ratio * 0.4)


def _token_overlap_score(left: str, right: str) -> float:
    left_tokens = set(_normalize_tokens(left))
    right_tokens = set(_normalize_tokens(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))


def _normalize_tokens(text: str) -> List[str]:
    text = _normalize_base(text)
    tokens = []
    for token in text.split():
        if token in STOPWORDS:
            continue
        if token.endswith("ies") and len(token) > 3:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 3:
            token = token[:-1]
        tokens.append(token)
    return tokens


def _normalize_text(text: str) -> str:
    base = _normalize_base(text)
    tokens = _normalize_tokens(base)
    return " ".join(tokens) or base


def _normalize_base(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"\bsemi\s+finished\b", "semifinished", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _confidence_label(score: float) -> str:
    if score >= 0.9:
        return "high"
    if score >= 0.7:
        return "medium"
    return "low"
