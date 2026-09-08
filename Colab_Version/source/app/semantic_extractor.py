"""Convert previously fetched HTML into evaluator-ready text."""

import asyncio

from bs4 import BeautifulSoup


async def extract_markdown(raw_html: str) -> str:
    """Return readable search-result text without allowing parser failures to stop a run."""
    if not isinstance(raw_html, str) or not raw_html.strip():
        raise ValueError("raw_html must be a non-empty string")

    soup = BeautifulSoup(raw_html, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    rows = []
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"], recursive=False)
        if not cells:
            continue
        values = []
        for cell in cells:
            text = cell.get_text(" ", strip=True)
            links = [
                f"{anchor.get_text(' ', strip=True) or 'link'} ({anchor['href']})"
                for anchor in cell.find_all("a", href=True)
            ]
            values.append(" ".join(part for part in (text, *links) if part))
        line = " | ".join(values).strip()
        if line:
            rows.append(line)
    return "\n".join(rows) or soup.get_text("\n", strip=True)


async def _run_test() -> None:
    """Run a simple end-to-end fetch and extraction check."""
    try:
        from app.stealth_scraper import fetch_page_content
    except ModuleNotFoundError:
        from stealth_scraper import fetch_page_content
    test_url = "https://libgen.bz/index.php?req=ASHRAE+standard&res=25"
    raw_html = await fetch_page_content(test_url)
    markdown = await extract_markdown(raw_html)

    print("--- Markdown Extraction Successful ---")
    print(markdown[:1500])


if __name__ == "__main__":
    try:
        asyncio.run(_run_test())
    except (ValueError, RuntimeError) as error:
        print(f"Semantic extraction test failed: {error}")
    except Exception as error:
        print(f"Unexpected semantic extraction error: {error}")
