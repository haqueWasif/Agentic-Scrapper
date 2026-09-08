import asyncio
import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")

async def fetch_page_content(url: str) -> str:
    """Fetch a page's HTML using ZenRows Fetch API to bypass anti-bots."""
    if not url or not url.startswith(("http://", "https://")):
        raise ValueError("url must be an absolute HTTP or HTTPS URL")

    if not ZENROWS_API_KEY:
        raise RuntimeError(
            "ZENROWS_API_KEY is not configured. Add it to the environment or a .env file."
        )

    # ZenRows Fetch API endpoint
    api_url = "https://api.zenrows.com/v1/"
    
    # Premium anti-bot parameters passed to the API
    params = {
        "apikey": ZENROWS_API_KEY,
        "url": url,
        "js_render": "true",
        "antibot": "true",
        "premium_proxy": "true"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url, params=params, timeout=120) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    error_text = await response.text()
                    raise RuntimeError(f"ZenRows API Error {response.status}: {error_text}")
        except asyncio.TimeoutError:
            raise RuntimeError(f"Timed out while waiting for ZenRows to fetch {url}")

if __name__ == "__main__":
    test_url = "https://libgen.li/"

    try:
        html = asyncio.run(fetch_page_content(test_url))
        print("--- Fetch Successful ---")
        print(f"Content Length: {len(html)} bytes")
        print(f"Preview: {html[:250]}...")
    except (ValueError, RuntimeError) as error:
        print(f"Scraper test failed: {error}")