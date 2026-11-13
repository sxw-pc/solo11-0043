"""Threaded werkzeug lifecycle helpers for mock and API processes."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Thread
import time
from typing import Any

import requests
from werkzeug.serving import make_server

from .. import config
from ..fetcher import check_target


@dataclass
class LocalServer:
    host: str
    port: int
    app: Any
    _server: Any
    _thread: Thread | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start_background(self) -> "LocalServer":
        self._thread = Thread(
            target=self._server.serve_forever,
            name="batchsched-local-server",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def create_local_server(app: Any, host: str, port: int) -> LocalServer:
    if host not in config.ALLOWED_HOSTS:
        raise ValueError("server host must be a loopback address")
    if not 1 <= int(port) <= 65535:
        raise ValueError("port must be between 1 and 65535")
    try:
        server = make_server(host, int(port), app, threaded=True)
    except OSError as exc:
        raise RuntimeError(f"cannot bind {host}:{port}: {exc.__class__.__name__}") from exc
    return LocalServer(host=host, port=int(port), app=app, _server=server)


def serve_forever(app: Any, host: str, port: int) -> None:
    local = create_local_server(app, host, port)
    print(f"batchsched listening on {local.base_url}")
    print("mock site: /api/catalog  read-only API: /api/runs")
    try:
        local._server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        local.stop()


def mock_site_reachable(base_url: str, timeout: float = 0.3) -> bool:
    target = f"{base_url.rstrip('/')}/api/health"
    if not check_target(target).allowed:
        return False
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(
            target,
            timeout=(timeout, timeout),
            allow_redirects=False,
            proxies={"http": None, "https": None},
        )
        if response.status_code != 200:
            return False
        payload = response.json()
        return payload.get("app") == "batchsched-mock-site"
    except (requests.RequestException, ValueError, OSError):
        return False
    finally:
        session.close()


def ensure_mock_site(
    host: str = config.DEFAULT_HOST, port: int = config.DEFAULT_PORT
) -> tuple[LocalServer | None, str]:
    """Start a mock site only if one is not already reachable.

    Returns ``(server_or_none, base_url)``.  A ``None`` server means an existing
    local service was reused and must not be stopped by the caller.
    """

    base_url = f"http://{host}:{port}"
    if mock_site_reachable(base_url):
        return None, base_url
    from .app import create_app

    local = create_local_server(create_app(), host, port).start_background()
    for _ in range(20):
        if mock_site_reachable(base_url):
            return local, base_url
        time.sleep(0.05)
    local.stop()
    raise RuntimeError("mock site failed to become ready")
