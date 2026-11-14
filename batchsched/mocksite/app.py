"""A tiny local-only mock data source used by every demo and test.

It deliberately serves deterministic fixtures plus a few misbehaving
endpoints so retry, timeout and size handling can be exercised without any
external network or real hostnames.
"""

from __future__ import annotations

from pathlib import Path
from threading import Lock
import json
import time

from flask import Flask, Response, jsonify, request

_FIXTURE_DIR = Path(__file__).with_name("fixtures")


def create_app(
    flaky_failures: int = 2,
    slow_delay_seconds: float = 0.15,
    timeout_delay_seconds: float = 3.0,
) -> Flask:
    app = Flask(__name__)
    app.config["FLAKY_FAILURES"] = max(0, int(flaky_failures))
    app.config["SLOW_DELAY_SECONDS"] = max(0.0, float(slow_delay_seconds))
    app.config["TIMEOUT_DELAY_SECONDS"] = max(0.01, float(timeout_delay_seconds))
    app.extensions["mock_state"] = {
        "flaky_requests": 0,
        "concurrency_requests": 0,
        "active": 0,
        "max_active": 0,
    }
    lock = Lock()
    app.extensions["mock_lock"] = lock

    @app.get("/api/health")
    def health():
        return jsonify({"app": "batchsched-mock-site", "status": "ok"})

    @app.get("/api/catalog")
    def catalog():
        return _fixture("catalog.json")

    @app.get("/api/orders")
    def orders():
        return _fixture("orders.json")

    @app.get("/api/orders/enriched")
    def orders_enriched():
        return _fixture("orders_enriched.json")

    @app.get("/api/summary")
    def summary():
        return _fixture("summary.json")

    @app.get("/api/text")
    def text_response():
        return Response("plain text fixture\n", content_type="text/plain; charset=utf-8")

    @app.get("/api/slow")
    def slow():
        time.sleep(app.config["SLOW_DELAY_SECONDS"])
        return jsonify({"dataset": "slow", "delay_seconds": app.config["SLOW_DELAY_SECONDS"]})

    @app.get("/api/concurrency")
    def concurrency():
        delay_ms = min(500, max(0, int(request.args.get("delay_ms", "100"))))
        state = app.extensions["mock_state"]
        with lock:
            state["concurrency_requests"] += 1
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            request_no = state["concurrency_requests"]
        try:
            time.sleep(delay_ms / 1000.0)
            return jsonify({"request": request_no, "delay_ms": delay_ms})
        finally:
            with lock:
                state["active"] -= 1

    @app.get("/api/flaky")
    def flaky():
        state = app.extensions["mock_state"]
        with lock:
            state["flaky_requests"] += 1
            attempt = state["flaky_requests"]
        if attempt <= app.config["FLAKY_FAILURES"]:
            return jsonify({"error": "temporary", "attempt": attempt}), 500
        return jsonify(
            {"dataset": "flaky", "attempt": attempt, "message": "recovered"}
        )

    @app.get("/api/timeout")
    def always_timeout():
        time.sleep(app.config["TIMEOUT_DELAY_SECONDS"])
        return jsonify({"dataset": "timeout", "message": "too late"})

    @app.get("/api/large")
    def large():
        return Response(b"x" * (64 * 1024), content_type="application/octet-stream")

    @app.get("/api/large-json")
    def large_json():
        count = min(500, max(1, int(request.args.get("records", "120"))))
        payload = {
            "dataset": "large-json",
            "records": [
                {"id": index, "label": f"record-{index:04d}", "active": index % 2 == 0}
                for index in range(count)
            ],
        }
        return Response(
            json.dumps(payload, ensure_ascii=False),
            content_type="application/json; charset=utf-8",
        )

    @app.get("/api/empty")
    def empty():
        return Response(b"", status=200, content_type="text/plain")

    @app.get("/api/not-found")
    def not_found():
        return jsonify({"error": "missing"}), 404

    @app.get("/api/server-error")
    def server_error():
        return jsonify({"error": "always broken"}), 500

    @app.post("/__mock/reset")
    def reset():
        with lock:
            state = app.extensions["mock_state"]
            for key in state:
                state[key] = 0
        return jsonify({"status": "reset"})

    @app.get("/__mock/state")
    def state_view():
        with lock:
            return jsonify(dict(app.extensions["mock_state"]))

    return app


def _fixture(name: str) -> Response:
    path = _FIXTURE_DIR / name
    return Response(
        path.read_bytes(),
        status=200,
        content_type="application/json; charset=utf-8",
    )
