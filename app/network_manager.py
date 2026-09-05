"""Small, stateful network layer for resilient document retrieval."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
import random
import threading
import time
from typing import Any

from curl_cffi import requests as c_requests


@dataclass(frozen=True)
class NetworkSettings:
    proxy_enabled: bool = False
    max_workers: int = 5
    retry_limit: int = 5
    retry_window_seconds: int = 1800
    cooldown_enabled: bool = True
    new_download_min_seconds: float = 2.0
    new_download_max_seconds: float = 6.0
    max_request_retries_per_attempt: int = 2
    max_stall_seconds: int = 60
    max_document_attempts: int = 5
    recovery_round_delay_seconds: float = 5.0
    download_queue_maxsize: int = 50
    validation_workers: int = 2
    stage2_queue_maxsize: int = 20


class NetworkManager:
    """Owns reusable worker sessions, optional proxy rotation, and UI-safe events."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.settings = self._read_settings(project_root / "config" / "settings.yaml")
        self.proxies = self._read_proxies(project_root / "config" / "proxies.txt")
        self.events: Queue[dict[str, Any]] = Queue()
        self._lock = threading.Lock()
        self._sessions: dict[int, Any] = {}
        self._proxy_health = defaultdict(lambda: {"successful_requests": 0, "failed_requests": 0, "last_used": 0.0, "disabled_until": 0.0})
        self._gateway_health = defaultdict(lambda: {
            "gateway_url": "", "success_count": 0, "failure_count": 0,
            "last_failure": None, "score": 0,
        })
        self._last_new_download = 0.0

    @staticmethod
    def _read_settings(path: Path) -> NetworkSettings:
        values: dict[str, str] = {}
        if path.exists():
            section = ""
            for raw in path.read_text(encoding="utf-8").splitlines():
                uncommented = raw.split("#", 1)[0]
                line = uncommented.strip()
                if not line or ":" not in line:
                    continue
                key, value = (part.strip() for part in line.split(":", 1))
                if not value and not raw[:len(raw) - len(raw.lstrip())]:
                    section = key
                    continue
                if section:
                    values[f"{section}.{key}"] = value
        def boolean(name: str, default: bool) -> bool:
            return values.get(name, str(default)).lower() in {"true", "1", "yes"}
        def number(name: str, default: float) -> float:
            try:
                return float(values.get(name, default))
            except ValueError:
                return default
        return NetworkSettings(
            proxy_enabled=boolean("network.proxy_enabled", False),
            max_workers=max(1, min(5, int(number("network.max_workers", 5)))),
            retry_limit=max(1, int(number("network.retry_limit", 5))),
            retry_window_seconds=max(60, int(number("network.retry_window_seconds", 1800))),
            cooldown_enabled=boolean("network.cooldown_enabled", True),
            new_download_min_seconds=max(0, number("network.new_download_min_seconds", 2)),
            new_download_max_seconds=max(0, number("network.new_download_max_seconds", 6)),
            max_request_retries_per_attempt=max(1, int(number("network.max_request_retries_per_attempt", 2))),
            max_stall_seconds=max(1, int(number("network.max_stall_seconds", 60))),
            max_document_attempts=max(1, int(number("network.max_document_attempts", 5))),
            recovery_round_delay_seconds=max(0, number("network.recovery_round_delay_seconds", 5)),
            download_queue_maxsize=max(1, int(number("pipeline.download_queue_maxsize", 50))),
            validation_workers=max(1, int(number("validation.max_workers", 2))),
            stage2_queue_maxsize=max(1, int(number("pipeline.stage2_queue_maxsize", 20))),
        )

    @staticmethod
    def _read_proxies(path: Path) -> list[str]:
        if not path.exists():
            return []
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#")]

    def begin_download(self, worker: int, filename: str) -> str | None:
        with self._lock:
            delay = max(0.0, self._last_new_download + random.uniform(self.settings.new_download_min_seconds, self.settings.new_download_max_seconds) - time.monotonic())
            self._last_new_download = time.monotonic() + delay
            proxy = self._select_proxy()
        if delay:
            time.sleep(delay)
        self.emit(
            "status", worker, filename,
            f"Network Status: Provider: curl_cffi · Worker: {worker}/{self.settings.max_workers} · Proxy: {proxy or 'disabled'}",
            level="DEBUG", category="network",
        )
        return proxy

    def _select_proxy(self) -> str | None:
        if not self.settings.proxy_enabled or not self.proxies:
            return None
        now = time.monotonic()
        candidates = [proxy for proxy in self.proxies if self._proxy_health[proxy]["disabled_until"] <= now]
        if not candidates:
            return None
        proxy = min(candidates, key=lambda item: self._proxy_health[item]["last_used"])
        self._proxy_health[proxy]["last_used"] = now
        return proxy

    def session(self):
        key = threading.get_ident()
        with self._lock:
            if key not in self._sessions:
                self._sessions[key] = c_requests.Session()
            return self._sessions[key]

    @staticmethod
    def request_options(proxy: str | None) -> dict[str, str]:
        return {"proxy": proxy} if proxy else {}

    def result(self, proxy: str | None, success: bool) -> None:
        if not proxy:
            return
        with self._lock:
            health = self._proxy_health[proxy]
            health["successful_requests" if success else "failed_requests"] += 1
            if not success and health["failed_requests"] >= self.settings.retry_limit:
                health["disabled_until"] = time.monotonic() + 300

    def gateway_result(self, gateway_url: str, status_code: int | None = None, *, completed: bool = False) -> None:
        """Persist a small, per-run health score for binary download gateways."""
        with self._lock:
            health = self._gateway_health[gateway_url]
            health["gateway_url"] = gateway_url
            if completed:
                health["success_count"] += 1
                health["score"] += 10 if status_code == 206 else 5 if status_code == 200 else 0
                return

            health["failure_count"] += 1
            health["last_failure"] = time.time()
            health["score"] += {
                500: -20, 502: -20, 503: -20, 504: -30,
            }.get(status_code, 0)

    def gateway_score(self, gateway_url: str) -> int:
        with self._lock:
            return int(self._gateway_health[gateway_url]["score"])

    def cooldown(self, attempt: int) -> None:
        if self.settings.cooldown_enabled:
            time.sleep(min(5 * (2 ** max(0, attempt - 1)), 40))

    def emit(self, kind: str, worker: int, filename: str, message: str = "", **extra: Any) -> None:
        self.events.put({
            "kind": kind, "worker": worker, "filename": filename, "message": message,
            "timestamp": time.time(), **extra,
        })

    def close(self) -> None:
        with self._lock:
            sessions, self._sessions = list(self._sessions.values()), {}
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass
