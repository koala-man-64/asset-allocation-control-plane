
import asyncio
import logging
from typing import List, Callable, Optional, Set, Any

import pandas as pd

from . import core as mdc

logger = logging.getLogger(__name__)

class ListManager:
    """
    Manages Whitelist and Blacklist for a specific scraper context.
    """
    def __init__(
        self,
        client,
        folder: str = "",
        *,
        auto_flush: bool = True,
        allow_blacklist_updates: bool = True,
    ):
        self.client = client
        self.whitelist_file = f"{folder}/whitelist.csv" if folder else "whitelist.csv"
        self.blacklist_file = f"{folder}/blacklist.csv" if folder else "blacklist.csv"
        
        self.whitelist: Set[str] = set()
        self.blacklist: Set[str] = set()
        self._loaded = False
        self.auto_flush = auto_flush
        self.allow_blacklist_updates = allow_blacklist_updates
        self._dirty_whitelist = False
        self._dirty_blacklist = False

    def load(self):
        """Loads lists from Azure Storage."""
        if not self.client:
             mdc.write_warning("ListManager has no client. Lists will be empty.")
             return

        w_list = mdc.load_ticker_list(self.whitelist_file, client=self.client)
        b_list = mdc.load_ticker_list(self.blacklist_file, client=self.client)
        
        self.whitelist = set(w_list)
        self.blacklist = set(b_list)
        self._loaded = True
        mdc.write_line(f"ListManager loaded: {len(self.whitelist)} whitelisted, {len(self.blacklist)} blacklisted.")

    def is_blacklisted(self, ticker: str) -> bool:
        if not self._loaded: self.load()
        return ticker in self.blacklist

    def is_whitelisted(self, ticker: str) -> bool:
        if not self._loaded: self.load()
        return ticker in self.whitelist

    def add_to_whitelist(self, ticker: str):
        if ticker not in self.whitelist:
            self.whitelist.add(ticker)
            if self.auto_flush:
                mdc.update_csv_set(self.whitelist_file, ticker, client=self.client)
            else:
                self._dirty_whitelist = True
            # If it was in blacklist, maybe remove it? Policy decision: Keep it simple for now.

    def add_to_blacklist(self, ticker: str):
        if not self.allow_blacklist_updates:
            return
        if ticker not in self.blacklist:
            self.blacklist.add(ticker)
            if self.auto_flush:
                mdc.update_csv_set(self.blacklist_file, ticker, client=self.client)
            else:
                self._dirty_blacklist = True

    def flush(self) -> None:
        """
        Persist whitelist/blacklist updates to storage when auto_flush=False.

        This is intended to avoid per-ticker read/modify/write cycles (which are slow and can race under concurrency).
        """
        if self.auto_flush:
            return
        if not self.client:
            mdc.write_warning("ListManager has no client. Cannot flush lists.")
            return
        if not self._loaded:
            # Ensure we don't accidentally discard remote content (best-effort).
            self.load()

        if self._dirty_whitelist:
            df = pd.DataFrame(sorted(self.whitelist), columns=["Symbol"])
            mdc.store_csv(df, self.whitelist_file, client=self.client)
            mdc.write_line(f"Saved {len(df)} symbols to {self.whitelist_file}")
            self._dirty_whitelist = False

        if self._dirty_blacklist:
            df = pd.DataFrame(sorted(self.blacklist), columns=["Symbol"])
            mdc.store_csv(df, self.blacklist_file, client=self.client)
            mdc.write_line(f"Saved {len(df)} symbols to {self.blacklist_file}")
            self._dirty_blacklist = False


class ScraperRunner:
    """
    Generic orchestrator for running async scraping tasks with concurrency control.
    """
    def __init__(self, concurrency: int = 3):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)

    async def run(
        self, 
        symbols: List[str], 
        process_func: Callable[[str], Any], 
        list_manager: Optional[ListManager] = None
    ):
        """
        symbols: List of ticker strings.
        process_func: Async function that takes a ticker and returns a result (or None).
        list_manager: Optional manager to filter symbols before processing.
        """
        
        # 1. Filter
        if list_manager:
            list_manager.load()
            filtered_symbols = [
                s for s in symbols 
                if not list_manager.is_blacklisted(s)
            ]
            if len(filtered_symbols) < len(symbols):
                 mdc.write_line(f"Filtered {len(symbols) - len(filtered_symbols)} blacklisted symbols.")
            symbols = filtered_symbols

        mdc.write_line(f"ScraperRunner starting for {len(symbols)} symbols with concurrency {self.concurrency}...")

        # 2. task wrapper
        async def worker(ticker):
            async with self.semaphore:
                try:
                    # Whitelist check could happen here if we wanted to skip validation logic
                    # relying on the process_func to handle specific logic
                    await process_func(ticker)
                except Exception as e:
                    mdc.write_error(f"Error processing {ticker}: {e}")

        # 3. Execution
        tasks = [worker(sym) for sym in symbols]
        if tasks:
            await asyncio.gather(*tasks)
        
        mdc.write_line("ScraperRunner completed.")
