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

            # 3. Handle markdown links [text](url) -> <a href="url">text</a>
            import re
            link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
            # We wrap the link in a span or just use a to set the color if write_html supports it
            # fpdf2's write_html supports basic <a> tags.
            html_line = re.sub(link_pattern, r'<a href="\2">\1</a>', html_line)

            # 4. Handle any remaining ** markers (convert them to <b>)
            while "**" in html_line:
                html_line = html_line.replace("**", "<b>", 1).replace("**", "</b>", 1)
            
            # Since write_html doesn't automatically blue-ify links, we might need a style
            self.write_html(html_line)
            self.ln(self._ds.body_style.line_height / 1.5)

        # References Footer
        if page_content.references:
            # Disable auto page break temporarily to place the footer at the bottom
            self.set_auto_page_break(False)
            
            self._apply_type_style(self._ds.reference_style)
            ref_text = "Ref : " + ", ".join(page_content.references)
            
            # Calculate height needed for multi_cell
            # Each line is roughly 4-5mm. We'll set Y accordingly.
            h = 5 # line height for references
            wrapped_lines = len(self.multi_cell(0, h, ref_text, dry_run=True, output="LINES"))
            total_h = wrapped_lines * h
            
            self.set_y(-(15 + total_h)) # 15 for the main footer + our height
            self.multi_cell(0, h, ref_text, align="L")
            
            # Restore auto page break
            self.set_auto_page_break(True, margin=self._ds.margins["bottom"])
