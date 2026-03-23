"""
Base scraper class that all state scrapers inherit from.
Provides common functionality for browser automation and data extraction.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, AsyncGenerator
from datetime import datetime
import asyncio
import random

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import settings
from logger import get_logger


@dataclass
class ScraperResult:
    """
    Result of scraping a single inmate.
    
    This dataclass is what scrapers yield. The orchestrator
    converts these to database Inmate objects.
    """
    inmate_id: str
    name: str
    state: str
    facility: Optional[str] = None
    release_date: Optional[datetime] = None
    source_url: Optional[str] = None
    raw_data: dict = field(default_factory=dict)  # Store any extra scraped data
    
    def __post_init__(self):
        # Normalize data
        self.inmate_id = self.inmate_id.strip().upper()
        self.name = self.name.strip().title()
        self.state = self.state.strip().upper()
        if self.facility:
            self.facility = self.facility.strip()


@dataclass
class ScrapeProgress:
    """Progress checkpoint for resumability."""
    letter: str
    page: int
    total_found: int
    last_updated: datetime = field(default_factory=datetime.now)


class BaseScraper(ABC):
    """
    Abstract base class for state inmate database scrapers.
    
    Subclasses must implement:
    - search_by_letter(): Async generator yielding ScraperResult objects
    - STATE: Class attribute with 2-letter state code
    - BASE_URL: Class attribute with the search page URL
    
    Provides:
    - Browser lifecycle management (Playwright)
    - Rate limiting with jitter
    - Retry logic with exponential backoff
    - Progress tracking for resumability
    - Structured logging
    """
    
    # Override these in subclasses
    STATE: str = ""
    BASE_URL: str = ""
    
    def __init__(self):
        self.log = get_logger(f"scraper.{self.STATE.lower()}")
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._progress: Optional[ScrapeProgress] = None
    
    # =========================================================================
    # BROWSER LIFECYCLE
    # =========================================================================
    
    async def __aenter__(self) -> "BaseScraper":
        """Async context manager entry - start browser."""
        await self.start_browser()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - close browser."""
        await self.close_browser()
    
    async def start_browser(self) -> None:
        """Initialize Playwright and browser with stealth settings."""
        self.log.info("Starting browser", state=self.STATE, headless=settings.headless)
        
        self._playwright = await async_playwright().start()
        
        # Launch browser with stealth-friendly settings
        self._browser = await self._playwright.chromium.launch(
            headless=settings.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        
        # Create context with realistic viewport and user agent
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
        )
        
        # Apply stealth scripts to hide automation
        await self._context.add_init_script("""
            // Hide webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Hide automation-related properties
            window.chrome = { runtime: {} };
            
            // Realistic plugins array
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Realistic languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
        
        self._page = await self._context.new_page()
        self._page.set_default_timeout(settings.browser_timeout)
        
        self.log.info("Browser started successfully", state=self.STATE)
    
    async def close_browser(self) -> None:
        """Clean up browser resources."""
        self.log.debug("Closing browser", state=self.STATE)
        
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        
        self.log.info("Browser closed", state=self.STATE)
    
    @property
    def page(self) -> Page:
        """Get the current page, raising if not initialized."""
        if self._page is None:
            raise RuntimeError(
                "Browser not started. Use 'async with scraper:' or call start_browser()"
            )
        return self._page
    
    # =========================================================================
    # ABSTRACT METHODS (implement in subclasses)
    # =========================================================================
    
    @abstractmethod
    async def search_by_letter(
        self,
        letter: str,
        start_page: int = 1,
    ) -> AsyncGenerator[ScraperResult, None]:
        """
        Search for inmates by last name starting letter.
        
        This is the main method subclasses implement. It should:
        1. Navigate to the search page
        2. Enter the search letter
        3. Handle pagination
        4. Yield ScraperResult objects for each inmate found
        
        Args:
            letter: Single letter to search (A-Z)
            start_page: Page to start from (for resumability)
        
        Yields:
            ScraperResult for each inmate found
        """
        yield  # type: ignore (this line makes it a generator)
    
    # =========================================================================
    # MAIN SCRAPING METHODS
    # =========================================================================
    
    async def scrape_all(
        self,
        start_letter: Optional[str] = None,
        start_page: int = 1,
    ) -> AsyncGenerator[ScraperResult, None]:
        """
        Scrape all inmates A-Z.
        
        Args:
            start_letter: Letter to resume from (for resumability)
            start_page: Page to resume from within that letter
        
        Yields:
            ScraperResult for each inmate found
        """
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        
        # Find starting position
        if start_letter:
            start_idx = letters.index(start_letter.upper())
            letters = letters[start_idx:]
        
        total_found = 0
        
        for i, letter in enumerate(letters):
            self.log.info(
                "Starting letter search",
                state=self.STATE,
                letter=letter,
                progress=f"{i + 1}/26"
            )
            
            # Use start_page only for the first letter (resuming)
            page = start_page if i == 0 and start_letter else 1
            
            letter_count = 0
            try:
                async for result in self.search_by_letter(letter, start_page=page):
                    letter_count += 1
                    total_found += 1
                    yield result
                    
                    # Update progress periodically
                    if letter_count % 100 == 0:
                        self.log.info(
                            "Scraping progress",
                            state=self.STATE,
                            letter=letter,
                            count=letter_count,
                            total=total_found
                        )
                
                self.log.info(
                    "Letter complete",
                    state=self.STATE,
                    letter=letter,
                    found=letter_count
                )
                
            except Exception as e:
                self.log.error(
                    "Letter scraping failed",
                    state=self.STATE,
                    letter=letter,
                    error=str(e),
                    found_before_error=letter_count
                )
                # Continue to next letter rather than stopping entirely
                continue
            
            # Delay between letters
            await self.rate_limit()
        
        self.log.info(
            "Scraping complete",
            state=self.STATE,
            total_found=total_found
        )
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    async def rate_limit(self, multiplier: float = 1.0) -> None:
        """
        Add randomized delay to avoid detection.
        
        Args:
            multiplier: Multiply the base delay (e.g., 2.0 for slower)
        """
        base_delay = settings.scraper_request_delay * multiplier
        jitter = base_delay * 0.3  # +/- 30%
        actual_delay = base_delay + random.uniform(-jitter, jitter)
        actual_delay = max(0.5, actual_delay)  # Minimum 0.5s
        
        self.log.debug("Rate limiting", delay=f"{actual_delay:.2f}s")
        await asyncio.sleep(actual_delay)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((PlaywrightTimeout, ConnectionError)),
        before_sleep=lambda retry_state: print(f"Retrying... attempt {retry_state.attempt_number}")
    )
    async def navigate_with_retry(self, url: str) -> None:
        """
        Navigate to URL with retry logic.
        
        Args:
            url: URL to navigate to
        
        Raises:
            PlaywrightTimeout: If navigation fails after retries
        """
        self.log.debug("Navigating", url=url)
        await self.page.goto(url, wait_until="domcontentloaded")
        await self.rate_limit(0.5)  # Small delay after navigation
    
    async def wait_for_results(
        self,
        selector: str,
        timeout: Optional[int] = None,
    ) -> bool:
        """
        Wait for search results to appear.
        
        Args:
            selector: CSS selector for results container
            timeout: Override default timeout (ms)
        
        Returns:
            True if found, False if timeout
        """
        try:
            await self.page.wait_for_selector(
                selector,
                timeout=timeout or settings.browser_timeout
            )
            return True
        except PlaywrightTimeout:
            self.log.warning(
                "Timeout waiting for results",
                state=self.STATE,
                selector=selector
            )
            return False
    
    async def get_page_count(self, pagination_selector: str) -> int:
        """
        Extract total page count from pagination element.
        
        Override in subclass if pagination structure differs.
        
        Args:
            pagination_selector: CSS selector for pagination
        
        Returns:
            Total number of pages (default 1 if not found)
        """
        try:
            pagination = await self.page.query_selector(pagination_selector)
            if pagination:
                text = await pagination.inner_text()
                # Try to extract number like "Page 1 of 5"
                import re
                match = re.search(r'of\s+(\d+)', text, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return 1
    
    async def screenshot(self, name: str) -> None:
        """Take a debug screenshot."""
        path = settings.data_dir / f"debug_{self.STATE}_{name}.png"
        await self.page.screenshot(path=str(path))
        self.log.debug("Screenshot saved", path=str(path))

