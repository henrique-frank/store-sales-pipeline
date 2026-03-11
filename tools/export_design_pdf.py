from pathlib import Path

from md2pdf.core import md2pdf


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    md_path = root / "docs" / "design.md"
    pdf_path = root / "docs" / "design.pdf"

    md2pdf(str(pdf_path), md_content=md_path.read_text(encoding="utf-8"))
    print(f"Written {pdf_path}")


if __name__ == "__main__":
    main()

