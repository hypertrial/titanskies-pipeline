import functools
import http.server
import threading
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = REPO_ROOT / "site"
pytestmark = pytest.mark.repo_check


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def docs_url():
    if not (SITE_DIR / "index.html").exists():
        pytest.skip("Run make docs-build before checking generated HTML.")
    handler = functools.partial(_QuietHandler, directory=SITE_DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def chromium():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            yield browser
        finally:
            browser.close()


def _has_horizontal_overflow(page):
    return page.evaluate(
        "document.documentElement.scrollWidth > document.documentElement.clientWidth"
    )


def _local_page(chromium, docs_url, *, viewport):
    page = chromium.new_page(viewport=viewport)
    external_requests = []

    def route_request(route):
        if route.request.url.startswith(docs_url):
            route.continue_()
        else:
            external_requests.append(route.request.url)
            route.abort()

    page.route("**/*", route_request)
    return page, external_requests


def test_homepage_desktop_is_usable(chromium, docs_url):
    page, external_requests = _local_page(
        chromium, docs_url, viewport={"width": 1440, "height": 900}
    )
    errors = []
    page.on(
        "console",
        lambda message: (
            errors.append(message.text) if message.type == "error" else None
        ),
    )

    page.goto(docs_url, wait_until="domcontentloaded")

    assert not _has_horizontal_overflow(page)
    assert page.locator("h1", has_text="TitanSkies Pipeline").is_visible()
    assert page.locator(
        ".md-header .md-source[href='https://github.com/hypertrial/titanskies-pipeline']"
    ).is_visible()
    assert not errors
    assert not external_requests
    page.close()


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/getting-started/",
        "/guides/query-the-warehouse/",
        "/reference/data-dictionary/",
        "/concepts/architecture/",
    ],
)
def test_representative_mobile_pages_are_usable(chromium, docs_url, path):
    page, external_requests = _local_page(
        chromium, docs_url, viewport={"width": 390, "height": 844}
    )
    page.goto(f"{docs_url}{path}", wait_until="domcontentloaded")

    assert not _has_horizontal_overflow(page), path
    assert page.locator("h1").is_visible(), path
    assert not external_requests, path
    page.close()
