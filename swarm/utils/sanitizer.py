import re
import json
import logging
from typing import Any, Dict, Optional, Type
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

class OutputSanitizer:
    @staticmethod
    def sanitize_json(text: str) -> Dict[str, Any]:
        """
        Extract JSON structure from a raw string text.
        Handles markdown blocks like ```json ... ``` or raw json.
        """
        if not text:
            raise ValueError("Empty input string")

        cleaned = text.strip()
        
        # Regex to match ```json ... ``` or ``` ... ```
        pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
        match = re.search(pattern, cleaned, re.DOTALL)
        
        if match:
            json_str = match.group(1)
        else:
            # Fallback: search for first '{' and last '}'
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start != -1 and end != -1 and end > start:
                json_str = cleaned[start:end+1]
            else:
                json_str = cleaned

        # Basic cleanup: remove invalid escape characters or trailing whitespace
        # Note: raw Python json parser might fail if there are control characters,
        # we can clean those up or attempt standard parsing.
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.debug(f"JSON parsing failed on: {json_str}. Error: {e}")
            # Try to fix simple issues like trailing commas or single quotes
            try:
                # Replace single quotes with double quotes
                # (Caution: handles simple cases, could fail if text contains apostrophes)
                fixed_str = json_str.replace("'", '"')
                return json.loads(fixed_str)
            except Exception:
                raise ValueError(f"Failed to parse text as JSON. Original error: {e}")

    @staticmethod
    def validate_schema(data: Dict[str, Any], schema_class: Type[BaseModel]) -> BaseModel:
        """
        Validate a dictionary against a Pydantic schema class.
        Returns the validated Pydantic model instance.
        """
        try:
            return schema_class.model_validate(data)
        except ValidationError as e:
            logger.warning(f"Schema validation failed for schema {schema_class.__name__}: {e}")
            raise ValueError(f"Schema validation failed: {e}")

    @classmethod
    def sanitize_and_validate(cls, text: str, schema_class: Type[BaseModel]) -> BaseModel:
        """Helper method to clean and validate output in a single call."""
        parsed_dict = cls.sanitize_json(text)
        return cls.validate_schema(parsed_dict, schema_class)
