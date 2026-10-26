"""Record the README demo from a real, locally running cluster.

Drives the dashboard at http://localhost:3000 with Playwright (Chromium), screenshots it once a
second, and turns the frames into docs/demo/distrocache-demo.gif and .mp4 with ffmpeg. Every
number on screen comes from the live cluster; the caption overlay only annotates each phase.

Phases: five healthy nodes, a write and a read, node3 crashed (SIGKILL) while reads still
succeed, then node3 restarted until it rejoins and reports a rebalance.

    docker compose up --build -d
    pip install playwright && python -m playwright install chromium
    python scripts/record_demo.py
    docker compose down

node3 is killed rather than stopped: a graceful stop deregisters it from the ring, so the
dashboard drops its card instead of showing it Unreachable. The outage is kept under the
coordinator's DEAD_TIMEOUT (30s), after which a dead member is pruned from the ring.

ffmpeg is taken from PATH, or from the imageio-ffmpeg package if it is installed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "demo"
# Tall enough for the whole dashboard, including the membership and rebalance lists, below
# the caption bar.
VIEWPORT = {"width": 1000, "height": 1700}
NODE_COUNT = 5
VICTIM = "node3"

CAPTION_JS = """
(text) => {
  let el = document.getElementById("demo-caption");
  if (!el) {
    el = document.createElement("div");
    el.id = "demo-caption";
    el.style.cssText = [
      "margin:16px 80px 0", "padding:14px 20px", "border-radius:12px",
      "background:#111", "color:#fff", "font:600 22px/1.35 system-ui,sans-serif",
      "white-space:pre-line", "min-height:60px",
    ].join(";");
    document.body.prepend(el);
  }
  el.textContent = text;
}
"""


def api(
    method: str, path: str, api_url: str, body: dict[str, Any] | None = None
) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        api_url + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def find_ffmpeg() -> str:
    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg  # type: ignore[import-not-found]
    except ImportError:
        sys.exit("ffmpeg not found: put it on PATH or `pip install imageio-ffmpeg`")
    return str(imageio_ffmpeg.get_ffmpeg_exe())


class Recorder:
    def __init__(self, page: Page, frames: Path) -> None:
        self.page = page
        self.frames = frames
        self.count = 0

    def caption(self, text: str) -> None:
        self.page.evaluate(CAPTION_JS, text)

    def frame(self) -> None:
        self.count += 1
        self.page.screenshot(path=str(self.frames / f"{self.count:04d}.png"))

    def hold(self, seconds: int) -> None:
        for _ in range(seconds):
            started = time.monotonic()
            self.frame()
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))

    def until(self, done: Callable[[], bool], timeout: int, what: str) -> None:
        """Capture a frame a second until `done()` holds; fail rather than record a lie."""
        for _ in range(timeout):
            started = time.monotonic()
            self.frame()
            if done():
                return
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))
        sys.exit(f"timed out after {timeout}s waiting for: {what}")


def node_badge(page: Page, node: str) -> str | None:
    card = page.locator(".card", has=page.locator("h2", has_text=f"{node}:"))
    if card.count() == 0:
        return None
    return card.first.locator(".badge").inner_text().strip()


def healthy_count(page: Page) -> int:
    return page.locator(".card.ok").count()


def rebalance_rows(page: Page) -> list[str]:
    card = page.locator(".card", has=page.locator("h2", has_text="Recent rebalances"))
    # The dashboard upper-cases these rows with CSS, and inner_text reports the rendered case.
    return [row.strip().lower() for row in card.locator(".card-body p").all_inner_texts()]


def record(dashboard_url: str, api_url: str, frames: Path) -> None:
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport=VIEWPORT)  # type: ignore[arg-type]
        page.goto(dashboard_url)
        page.wait_for_function(f"document.querySelectorAll('.card.ok').length === {NODE_COUNT}")
        rec = Recorder(page, frames)

        rec.caption(f"Local cluster: {NODE_COUNT} cache nodes, all healthy (N=3, R=2, W=2)")
        rec.hold(5)

        status, put = api("POST", "/kv/greeting", api_url, {"value": "hello"})
        assert status == 200, (status, put)
        rec.caption(
            f'POST /kv/greeting {{"value":"hello"}} -> {status}\n'
            f"stored on {', '.join(r.split(':')[0] for r in put['replicas'])} "
            f"({put['acks']} acks)"
        )
        rec.hold(3)
        status, got = api("GET", "/kv/greeting", api_url)
        assert status == 200 and got["value"] == "hello", (status, got)
        rec.caption(
            f'GET /kv/greeting -> {status}: "{got["value"]}" '
            f"(served by {got['node'].split(':')[0]}, {got['acks']} acks)"
        )
        rec.hold(3)

        rec.caption(f"docker compose kill {VICTIM}: crashing a node...")
        rec.frame()
        compose("kill", VICTIM)
        rec.until(
            lambda: node_badge(page, VICTIM) == "Unreachable", 20, f"{VICTIM} shown Unreachable"
        )
        status, got = api("GET", "/kv/greeting", api_url)
        assert status == 200 and got["value"] == "hello", (status, got)
        rec.caption(
            f"{VICTIM} killed: reads still succeed\n"
            f'GET /kv/greeting -> {status}: "{got["value"]}" ({got["acks"]} acks)'
        )
        rec.hold(4)

        before = rebalance_rows(page)
        rec.caption(f"docker compose start {VICTIM}: rejoining...")
        rec.frame()
        compose("start", VICTIM)

        def rejoined() -> bool:
            rows = rebalance_rows(page)
            return (
                node_badge(page, VICTIM) == "Healthy"
                and healthy_count(page) == NODE_COUNT
                and rows != before
                and rows[-1].startswith(VICTIM)
            )

        rec.until(rejoined, 30, f"{VICTIM} healthy with a new rebalance entry")
        rec.caption(f"{VICTIM} rejoined: all {NODE_COUNT} healthy, rebalance reported")
        rec.hold(5)
        browser.close()


def encode(ffmpeg: str, frames: Path, width: int) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pattern = str(frames / "%04d.png")
    gif = OUT_DIR / "distrocache-demo.gif"
    mp4 = OUT_DIR / "distrocache-demo.mp4"
    palette = (
        f"scale={width}:-1:flags=lanczos,split[a][b];"
        "[a]palettegen=max_colors=128:stats_mode=diff[p];"
        "[b][p]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle"
    )
    run = [ffmpeg, "-y", "-loglevel", "error", "-framerate", "1", "-i", pattern]
    subprocess.run([*run, "-filter_complex", palette, "-loop", "0", str(gif)], check=True)
    subprocess.run(
        [
            *run,
            "-vf",
            f"scale={width}:-2:flags=lanczos,format=yuv420p",
            "-r",
            "10",
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-movflags",
            "+faststart",
            str(mp4),
        ],
        check=True,
    )
    for path in (gif, mp4):
        print(f"wrote {path.relative_to(ROOT)} ({path.stat().st_size / 1e6:.2f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dashboard", default="http://localhost:3000")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--width", type=int, default=1000)
    args = parser.parse_args()

    ffmpeg = find_ffmpeg()
    status, health = api("GET", "/health", args.api)
    ok = sum(1 for node in health.get("nodes", {}).values() if node.get("ok"))
    if status != 200 or ok != NODE_COUNT:
        sys.exit(f"need {NODE_COUNT} healthy nodes at {args.api}/health, got {ok}")

    with tempfile.TemporaryDirectory(prefix="distrocache-demo-") as tmp:
        frames = Path(tmp)
        record(args.dashboard, args.api, frames)
        print(f"captured {len(list(frames.glob('*.png')))} frames")
        encode(ffmpeg, frames, args.width)


if __name__ == "__main__":
    main()
