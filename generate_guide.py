import yaml
from pathlib import Path
from src.guide_generator.core.design import DesignSystem
from src.guide_generator.core.models import PageContent
from src.guide_generator.engine.renderer import GuideRenderer

def generate():
    root = Path(__file__).parent
    content_dir = root / "content" / "guide"
    design_system_path = root / "design-system.yaml"
    output_path = root / "Data_Craftmanship_Guide.pdf"

    # 1. Load Design System
    ds = DesignSystem.load(design_system_path)

    # 2. Load Content
    pages = []
    for yaml_file in sorted(content_dir.glob("*.yaml")):
        with open(yaml_file, "r") as f:
            data = yaml.safe_load(f)
            pages.append(PageContent.from_dict(data))

    # 3. Render PDF
    renderer = GuideRenderer(ds)
    for page in pages:
        renderer.render_page(page)

    # 4. Output
    renderer.output(str(output_path))
    print(f"Success: Guide generated -> {output_path}")

if __name__ == "__main__":
    generate()
