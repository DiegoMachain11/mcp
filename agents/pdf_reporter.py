from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from PIL import Image, ImageDraw, ImageFont


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """
    Try several common system fonts before falling back to Pillow's default.
    """
    regular_candidates = [
        "SFNSDisplay.ttf",
        "SFNS.ttf",
        "/System/Library/Fonts/SFNSDisplay.ttf",
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "Helvetica.ttc",
        "Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    bold_candidates = [
        "SFNSDisplay-Bold.ttf",
        "/System/Library/Fonts/SFNSDisplay-Bold.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttc",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "HelveticaBold.ttf",
        "Arial Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    ]

    for candidate in (bold_candidates if bold else regular_candidates):
        try:
            return ImageFont.truetype(candidate, size=size)
        except (OSError, IOError):
            continue

    return ImageFont.load_default()


def _measure_text(font: ImageFont.FreeTypeFont, text: str) -> float:
    if hasattr(font, "getlength"):
        return font.getlength(text)
    return font.getsize(text)[0]


class PdfCanvas:
    def __init__(self, page_size=(1240, 1754), margin=80):
        self.width, self.height = page_size
        self.margin = margin
        self.text_width = self.width - (2 * self.margin)
        self.colors = {
            "title": (18, 62, 105),
            "subtitle": (90, 105, 120),
            "text": (34, 38, 41),
            "accent": (13, 110, 139),
            "tag_bg": (229, 240, 248),
            "tag_text": (18, 62, 105),
            "divider": (210, 215, 220),
        }
        self.fonts = {
            "title": _load_font(64, bold=True),
            "subtitle": _load_font(36),
            "heading": _load_font(40, bold=True),
            "subheading": _load_font(30, bold=True),
            "body": _load_font(26),
            "small": _load_font(22),
        }
        self.pages: List[Image.Image] = []
        self.cursor_y = self.margin
        self._create_page()

    def _create_page(self):
        page = Image.new("RGB", (self.width, self.height), color="white")
        self.pages.append(page)
        self.draw = ImageDraw.Draw(page)
        self.cursor_y = self.margin

    def _ensure_space(self, needed_height: int):
        if self.cursor_y + needed_height > self.height - self.margin:
            self._create_page()

    def add_title(self, text: str):
        self._ensure_space(self.fonts["title"].size + 30)
        self.draw.text(
            (self.margin, self.cursor_y),
            text,
            font=self.fonts["title"],
            fill=self.colors["title"],
        )
        self.cursor_y += self.fonts["title"].size + 20

    def add_subtitle(self, text: str):
        self._ensure_space(self.fonts["subtitle"].size + 10)
        self.draw.text(
            (self.margin, self.cursor_y),
            text,
            font=self.fonts["subtitle"],
            fill=self.colors["subtitle"],
        )
        self.cursor_y += self.fonts["subtitle"].size + 15

    def add_heading(self, text: str):
        self._ensure_space(self.fonts["heading"].size + 20)
        self.draw.text(
            (self.margin, self.cursor_y),
            text,
            font=self.fonts["heading"],
            fill=self.colors["title"],
        )
        self.cursor_y += self.fonts["heading"].size + 12

    def add_subheading(self, text: str):
        self._ensure_space(self.fonts["subheading"].size + 12)
        self.draw.text(
            (self.margin, self.cursor_y),
            text,
            font=self.fonts["subheading"],
            fill=self.colors["accent"],
        )
        self.cursor_y += self.fonts["subheading"].size + 8

    def add_paragraph(self, text: str, font_key: str = "body", spacing: int = 6):
        font = self.fonts[font_key]
        for line in self._wrap_text(text, font):
            height = font.size + spacing
            self._ensure_space(height)
            self.draw.text(
                (self.margin, self.cursor_y),
                line,
                font=font,
                fill=self.colors["text"],
            )
            self.cursor_y += height
        self.cursor_y += spacing

    def add_bullet_list(self, items: Iterable[str], bullet: str = "- "):
        font = self.fonts["body"]
        for item in items:
            for idx, line in enumerate(self._wrap_text(item, font)):
                prefix = bullet if idx == 0 else "  "
                height = font.size + 4
                self._ensure_space(height)
                self.draw.text(
                    (self.margin, self.cursor_y),
                    f"{prefix}{line}",
                    font=font,
                    fill=self.colors["text"],
                )
                self.cursor_y += height
        self.cursor_y += 6

    def add_tag_row(self, tags: Iterable[str]):
        tags = [tag for tag in tags if tag]
        if not tags:
            return

        font = self.fonts["small"]
        padding_x, padding_y = 14, 8
        box_height = font.size + (padding_y * 2)
        x_cursor = self.margin

        for tag in tags:
            text_width = _measure_text(font, tag)
            box_width = text_width + (padding_x * 2)
            if x_cursor + box_width > self.margin + self.text_width:
                self.cursor_y += box_height + 8
                self._ensure_space(box_height + 8)
                x_cursor = self.margin

            xy = [
                (x_cursor, self.cursor_y),
                (x_cursor + box_width, self.cursor_y + box_height),
            ]
            if hasattr(self.draw, "rounded_rectangle"):
                self.draw.rounded_rectangle(
                    xy, radius=16, fill=self.colors["tag_bg"], outline=None
                )
            else:
                self.draw.rectangle(
                    xy, fill=self.colors["tag_bg"], outline=None
                )
            text_position = (
                x_cursor + padding_x,
                self.cursor_y + padding_y - 2,
            )
            self.draw.text(
                text_position,
                tag,
                font=font,
                fill=self.colors["tag_text"],
            )
            x_cursor += box_width + 12

        self.cursor_y += box_height + 12

    def add_divider(self):
        self._ensure_space(30)
        self.draw.line(
            (self.margin, self.cursor_y, self.margin + self.text_width, self.cursor_y),
            fill=self.colors["divider"],
            width=3,
        )
        self.cursor_y += 20

    def _wrap_text(self, text: str, font: ImageFont.FreeTypeFont) -> List[str]:
        if not text:
            return []
        words = text.split()
        lines: List[str] = []
        current_line = ""

        for word in words:
            candidate = f"{current_line} {word}".strip()
            if _measure_text(font, candidate) <= self.text_width:
                current_line = candidate
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word

        if current_line:
            lines.append(current_line)
        return lines

    def save(self, path: Path):
        if not self.pages:
            raise RuntimeError("No pages to save")
        first, rest = self.pages[0], self.pages[1:]
        first.save(
            path,
            format="PDF",
            resolution=200.0,
            save_all=bool(rest),
            append_images=rest,
        )


def generate_master_summary_pdf(
    summary: Dict[str, Any],
    output_dir: Union[Path, str] = "pdfs",
    filename: Optional[str] = None,
    report_date: Optional[datetime] = None,
) -> Path:
    if not summary:
        raise ValueError("Summary payload is empty.")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report_date = report_date or datetime.now()
    farm_code = summary.get("farm_code", "FARM")
    overall = summary.get("final_summary", {}).get("overall_health", "N/A")

    canvas = PdfCanvas()
    canvas.add_title(f"Farm {farm_code}")
    canvas.add_subtitle("Master Performance Summary")
    canvas.add_paragraph(
        f"Report generated on {report_date.strftime('%B %d, %Y at %H:%M')}",
        font_key="small",
    )
    canvas.add_paragraph(f"Overall health rating: {overall}", font_key="small")
    canvas.add_divider()

    overview_text = summary.get("overview")
    if overview_text:
        canvas.add_heading("Pre-Analyzer Overview")
        canvas.add_paragraph(overview_text)

    final_summary = summary.get("final_summary", {})
    exec_summary = final_summary.get("executive_summary")
    if exec_summary:
        canvas.add_heading("Executive Summary")
        canvas.add_paragraph(exec_summary)

    priority_actions = final_summary.get("priority_actions", [])
    if priority_actions:
        canvas.add_subheading("Priority Actions (Next 3 Months)")
        canvas.add_bullet_list(priority_actions)

    urgent_kpis = summary.get("urgent_kpis") or []
    if urgent_kpis:
        canvas.add_subheading("Urgent KPIs")
        canvas.add_tag_row(urgent_kpis)

    domain_overview = final_summary.get("domains_overview", {})
    if domain_overview:
        canvas.add_subheading("Domain Snapshots")
        for domain_name, note in domain_overview.items():
            canvas.add_paragraph(f"{domain_name}: {note}", font_key="small")

    domains = summary.get("domains", {})
    for domain_name in sorted(domains.keys()):
        domain = domains[domain_name]
        canvas.add_divider()
        canvas.add_heading(f"{domain_name} Focus")
        canvas.add_paragraph(domain.get("summary", ""))

        issues = domain.get("issues") or []
        if issues:
            canvas.add_subheading("Key Issues")
            canvas.add_bullet_list(issues)

        recs = domain.get("recommendations") or {}
        order = ["Immediate", "Short", "Medium", "Long"]
        for bucket in order:
            items = recs.get(bucket) or []
            if items:
                canvas.add_subheading(f"{bucket} Actions")
                canvas.add_bullet_list(items)

        kpis = domain.get("kpis_to_plot") or []
        if kpis:
            canvas.add_subheading("KPIs to Monitor")
            canvas.add_tag_row(kpis)

    if not filename:
        timestamp = report_date.strftime("%Y%m%d_%H%M%S")
        sanitized_code = "".join(c for c in farm_code if c.isalnum() or c in ("-", "_"))
        filename = f"{sanitized_code or 'farm'}_master_summary_{timestamp}.pdf"

    pdf_path = output_dir / filename
    canvas.save(pdf_path)
    return pdf_path


def _cli():
    parser = argparse.ArgumentParser(
        description="Render a PDF version of a master summary payload."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=Path("data/sample_master_summary.json"),
        help="Path to the JSON payload from master_summary_agent.",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("pdfs"),
        help="Directory where the PDF report should be stored.",
    )
    parser.add_argument(
        "--filename",
        "-f",
        type=str,
        default=None,
        help="Optional file name for the PDF; defaults to farm + timestamp.",
    )
    args = parser.parse_args()

    with args.input.open("r", encoding="utf-8") as fp:
        summary = json.load(fp)

    output_path = generate_master_summary_pdf(
        summary,
        output_dir=args.output_dir,
        filename=args.filename,
    )
    print(f"PDF saved to {output_path}")


if __name__ == "__main__":
    _cli()
