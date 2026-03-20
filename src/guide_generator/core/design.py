from __future__ import annotations
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Self
import yaml

@dataclass(frozen=True)
class TypeStyle:
    font_family: str
    style: str
    size: int
    color_rgb: tuple[int, int, int]
    line_height: float = 6.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        _fields = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in _fields}
        return cls(**{**filtered, "color_rgb": tuple(data["color_rgb"])})

@dataclass(frozen=True)
class DesignSystem:
    margins: dict[str, int]
    page_format: str
    title_style: TypeStyle
    subtitle_style: TypeStyle
    section_header_style: TypeStyle
    body_style: TypeStyle
    footer_style: TypeStyle
    reference_style: TypeStyle
    color_palette: dict[str, tuple[int, int, int]]

    @classmethod
    def load(cls, path: Path) -> Self:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        topo = raw["typography"]
        palette = {k: tuple(v) for k, v in raw["color_palette"].items()}
        
        return cls(
            margins=raw["layout"]["margins"],
            page_format=raw["layout"]["page_format"],
            title_style=TypeStyle.from_dict(topo["title"]),
            subtitle_style=TypeStyle.from_dict(topo["subtitle"]),
            section_header_style=TypeStyle.from_dict(topo["section_header"]),
            body_style=TypeStyle.from_dict(topo["body"]),
            footer_style=TypeStyle.from_dict(topo["footer"]),
            reference_style=TypeStyle.from_dict(topo["reference"]),
            color_palette=palette
        )
