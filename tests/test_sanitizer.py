import pytest
from pydantic import BaseModel, Field
from swarm.utils.sanitizer import OutputSanitizer

class DummySchema(BaseModel):
    title: str
    rating: int = Field(default=5)
    tags: list[str] = Field(default_factory=list)

def test_sanitizer_extract_markdown_json():
    raw_text = """
    Here is the requested output:
    ```json
    {
        "title": "Clean Code",
        "rating": 5,
        "tags": ["software", "design"]
    }
    ```
    Hope this helps!
    """
    
    parsed = OutputSanitizer.sanitize_json(raw_text)
    assert parsed["title"] == "Clean Code"
    assert parsed["rating"] == 5
    assert "software" in parsed["tags"]

def test_sanitizer_extract_fallback_brackets():
    raw_text = """
    Some introduction text
    {
        "title": "Refactoring",
        "rating": 4,
        "tags": ["refactoring"]
    }
    Trailing text here.
    """
    parsed = OutputSanitizer.sanitize_json(raw_text)
    assert parsed["title"] == "Refactoring"
    assert parsed["rating"] == 4

def test_sanitizer_extract_single_quotes_repair():
    raw_text = "{'title': 'Test Title', 'rating': 3}"
    parsed = OutputSanitizer.sanitize_json(raw_text)
    assert parsed["title"] == "Test Title"
    assert parsed["rating"] == 3

def test_sanitizer_validate_schema_success():
    data = {
        "title": "Design Patterns",
        "rating": 5,
        "tags": ["oop"]
    }
    model = OutputSanitizer.validate_schema(data, DummySchema)
    assert isinstance(model, DummySchema)
    assert model.title == "Design Patterns"
    assert model.rating == 5

def test_sanitizer_validate_schema_missing_fields_defaults():
    # rating and tags have defaults, title is required
    data = {"title": "Design Patterns"}
    model = OutputSanitizer.validate_schema(data, DummySchema)
    assert model.title == "Design Patterns"
    assert model.rating == 5
    assert model.tags == []

def test_sanitizer_validate_schema_failure():
    # Missing required title field
    data = {"rating": 5}
    with pytest.raises(ValueError, match="Schema validation failed"):
        OutputSanitizer.validate_schema(data, DummySchema)

def test_sanitizer_sanitize_and_validate_e2e():
    raw_text = """
    ```json
    {
        "title": "Clean Architecture",
        "rating": 5,
        "tags": ["architecture"]
    }
    ```
    """
    model = OutputSanitizer.sanitize_and_validate(raw_text, DummySchema)
    assert model.title == "Clean Architecture"
    assert model.rating == 5
    assert model.tags == ["architecture"]
