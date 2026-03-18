from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Self

@dataclass(frozen=True)
class PageContent:
    page_number: int
    title: str
    subtitle: str
    content: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        return cls(**data)

@dataclass(frozen=True)
class GuideContent:
    title: str
    author: str
    pages: list[PageContent]
