"""NSE annual report fetcher."""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from typing import Any

import requests


LOGGER = logging.getLogger("dataset_builder")
NSE_HOME = "https://www.nseindia.com"
NSE_REPORTS_API = "https://www.nseindia.com/api/annual-reports?index=equities&symbol={ticker}"


class NSEDATAPipeline:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_HOME,
        }
        LOGGER.info("Initializing NSE session")
        self.session.get(NSE_HOME, headers=self.headers, timeout=10)

    def _fetch_reports(self, ticker: str) -> list[dict[str, Any]]:
        api_url = NSE_REPORTS_API.format(ticker=ticker)
        LOGGER.debug("Fetching NSE annual reports ticker=%s", ticker)
        response = self.session.get(api_url, headers=self.headers, timeout=10)
        if response.status_code != 200:
            raise RuntimeError(f"NSE API failed status={response.status_code}")
        data = response.json().get("data", [])
        LOGGER.debug("NSE reports received ticker=%s count=%d", ticker, len(data))
        return data

    @staticmethod
    def _select_report(data: list[dict[str, Any]], target_year: int) -> tuple[int, str] | None:
        candidates: list[tuple[int, str]] = []
        for report in data:
            to_year_raw = str(report.get("toYr") or "").strip()
            url = str(report.get("fileName") or "").strip()
            if not to_year_raw or not url:
                continue
            try:
                to_year = int(to_year_raw)
            except ValueError:
                continue
            candidates.append((to_year, url))

        if not candidates:
            return None

        candidates.sort(key=lambda item: (abs(item[0] - target_year), -item[0]))
        return candidates[0]

    def download_annual_report(
        self,
        ticker: str,
        target_year: int,
        save_path: Path,
    ) -> tuple[bool, int | None, str | None, str | None]:
        try:
            data = self._fetch_reports(ticker)
        except Exception as exc:
            LOGGER.info("NSE fetch failed ticker=%s error=%s", ticker, exc)
            return False, None, None, f"nse_api:{exc}"

        selected = self._select_report(data, target_year)
        if selected is None:
            LOGGER.info("No annual report found ticker=%s target_year=%s", ticker, target_year)
            return False, None, None, "annual_report_not_found"

        report_year, zip_url = selected
        LOGGER.info("Selected annual report ticker=%s target_year=%s selected_year=%s", ticker, target_year, report_year)
        try:
            zip_resp = self.session.get(zip_url, headers=self.headers, timeout=30)
            if zip_resp.status_code != 200:
                LOGGER.info("Annual report download failed ticker=%s status=%s", ticker, zip_resp.status_code)
                return False, report_year, zip_url, f"zip_download_status:{zip_resp.status_code}"

            with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as archive:
                pdf_name = next((name for name in archive.namelist() if name.lower().endswith(".pdf")), None)
                if not pdf_name:
                    LOGGER.info("Annual report zip missing PDF ticker=%s", ticker)
                    return False, report_year, zip_url, "zip_missing_pdf"
                pdf_bytes = archive.read(pdf_name)
                with save_path.open("wb") as handle:
                    handle.write(pdf_bytes)
        except Exception as exc:
            LOGGER.info("Annual report download error ticker=%s error=%s", ticker, exc)
            return False, report_year, zip_url, f"zip_download:{exc}"

        LOGGER.info("Saved annual report ticker=%s path=%s", ticker, save_path)
        return True, report_year, zip_url, None
