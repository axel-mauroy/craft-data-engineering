from __future__ import annotations
from fpdf import FPDF
from src.guide_generator.core.design import DesignSystem, TypeStyle
from src.guide_generator.core.models import PageContent

class GuideRenderer(FPDF):
    def __init__(self, ds: DesignSystem):
        super().__init__(format=ds.page_format)
        self._ds = ds
        self.set_margins(left=ds.margins["left"], top=ds.margins["top"], right=ds.margins["right"])
        self.set_auto_page_break(auto=True, margin=ds.margins["bottom"])

    def _apply_type_style(self, style: TypeStyle):
        self.set_font(style.font_family, style.style, style.size)
        self.set_text_color(*style.color_rgb)

    def header(self):
        # We don't want a default header on every page that's handled by render_page
        pass

    def footer(self):
        self.set_y(-15)
        self._apply_type_style(self._ds.footer_style)
        
        # LinkedIn on the left, Page number on the right
        self.cell(0, 10, "linkedin.com/in/axel-mauroy-5699509a", link="https://www.linkedin.com/in/axel-mauroy-5699509a")
        self.set_x(self._ds.margins["left"])
        self.cell(0, 10, f"Page {self.page_no()}", align="R")

    def render_page(self, page_content: PageContent):
        self.add_page()
        
        # Title
        self._apply_type_style(self._ds.title_style)
        self.multi_cell(0, 15, page_content.title, align="L")
        self.ln(2)
        
        # Subtitle
        self._apply_type_style(self._ds.subtitle_style)
        self.multi_cell(0, 10, page_content.subtitle, align="L")
        
        # Divider
        self.set_draw_color(*self._ds.color_palette["accent"])
        self.set_line_width(0.5)
        self.line(self.get_x(), self.get_y() + 2, self.get_x() + 50, self.get_y() + 2)
        self.ln(10)
        
        # Content
        for line in page_content.content:
            self._apply_type_style(self._ds.body_style)
            
            # 1. Ensure it looks like a bullet
            text_line = line.strip()
            if not text_line.startswith("-"):
                text_line = f"- {text_line}"
            
            # 2. Automatically bold the part before the first colon (:) if it exists
            # Example: "- Title : Description" -> "- <b>Title :</b> Description"
            if ":" in text_line and "<b>" not in text_line and "**" not in text_line:
                parts = text_line.split(":", 1)
                html_line = f"{parts[0]} :</b>{parts[1]}"
                # Shift the <b> after the bullet dash
                if html_line.startswith("- "):
                    html_line = f"- <b>{html_line[2:]}"
                else:
                    html_line = f"<b>{html_line}"
            else:
                html_line = text_line

            # 3. Handle any remaining ** markers (convert them to <b>)
            while "**" in html_line:
                html_line = html_line.replace("**", "<b>", 1).replace("**", "</b>", 1)
            
            self.write_html(html_line)
            self.ln(self._ds.body_style.line_height / 1.5)
