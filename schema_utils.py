# schema_utils.py
from collections import defaultdict
from typing import Any, Dict, List

def infer_schema(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infer a very simple JSON schema: field -> type name or nested schema
    """
    schema = {}
    for k, v in doc.items():
        if isinstance(v, dict):
            schema[k] = {"type": "object", "schema": infer_schema(v)}
        elif isinstance(v, list):
            # infer type of list elements (take union)
            elem_types = set(type(e).__name__ for e in v if e is not None)
            if not elem_types:
                schema[k] = {"type": "array", "items": "unknown"}
            elif len(elem_types) == 1:
                schema[k] = {"type": "array", "items": elem_types.pop()}
            else:
                schema[k] = {"type": "array", "items": list(elem_types)}
        else:
            schema[k] = {"type": type(v).__name__}
    return schema

def merge_schemas(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two inferred schemas conservatively (union of keys, union of types)
    """
    merged = {}
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        if k not in a:
            merged[k] = b[k]
        elif k not in b:
            merged[k] = a[k]
        else:
            # both exist
            ta, tb = a[k], b[k]
            if ta.get("type") != tb.get("type"):
                # conflicting types -> mark as multiple
                merged[k] = {"type": "multiple", "types": [ta.get("type"), tb.get("type")]}
            elif ta.get("type") == "object":
                merged[k] = {"type": "object", "schema": merge_schemas(ta["schema"], tb["schema"])}
            elif ta.get("type") == "array":
                # merge item types conservatively
                ia, ib = ta.get("items"), tb.get("items")
                if ia == ib:
                    merged[k] = ta
                else:
                    merged[k] = {"type": "array", "items": list({str(ia), str(ib)})}
            else:
                merged[k] = ta
    return merged
