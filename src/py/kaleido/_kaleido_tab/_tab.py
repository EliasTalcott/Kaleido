from __future__ import annotations

import base64
from typing import TYPE_CHECKING

import logistro

from . import _devtools_utils as _dtools
from . import _js_logger
from ._errors import _raise_error

if TYPE_CHECKING:
    import asyncio
    from pathlib import Path

    import choreographer as choreo

    from kaleido._utils import fig_tools



_TEXT_FORMATS = ("svg", "json")  # eps
_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB keeps messages under Chrome's pipe limit

_logger = logistro.getLogger(__name__)


def _estimate_json_size(spec):
    """Cheaply estimate the JSON byte size of a spec from its data arrays."""
    total = 0
    for trace in spec.get("data", {}).get("data", []):
        for v in trace.values():
            if isinstance(v, dict) and "bdata" in v:
                total += len(v["bdata"])
            elif hasattr(v, "__len__") and not isinstance(v, (str, dict)):
                total += len(v) * 12
    return total


def _subscribe_new(tab: choreo.Tab, event: str) -> asyncio.Future:
    """Create subscription to tab clearing old ones first: helper function."""
    new_future = tab.subscribe_once(event)
    while new_future.done():
        _logger.debug2(f"Clearing an old {event}")
        new_future = tab.subscribe_once(event)
    return new_future


class _KaleidoTab:
    """
    A Kaleido tab is a wrapped choreographer tab providing the functions we need.

    The choreographer tab can be accessed through the `self.tab` attribute.
    """

    tab: choreo.Tab
    """The underlying choreographer tab."""
    js_logger: _js_logger.JavascriptLogger
    """A log for recording javascript."""

    def __init__(self, tab):
        """
        Create a new _KaleidoTab.

        Args:
            tab: the choreographer tab to wrap.

        """
        self.tab = tab
        self.js_logger = _js_logger.JavascriptLogger(self.tab)

    async def navigate(self, url: str | Path = ""):
        """
        Navigate to the kaleidofier script. This is effectively the real initialization.

        Args:
            url: Override the location of the kaleidofier script if necessary.

        """
        # Subscribe to event which will contain javascript engine ID (need it
        # for calling javascript functions)
        javascript_ready = _subscribe_new(self.tab, "Runtime.executionContextCreated")

        # Subscribe to event indicating page ready.
        page_ready = _subscribe_new(self.tab, "Page.loadEventFired")

        # Navigating page. This will trigger the above events.
        _logger.debug2(f"Calling Page.navigate on {self.tab}")
        _raise_error(await self.tab.send_command("Page.navigate", params={"url": url}))

        # Enabling page events (for page_ready- like all events, if already
        # ready, the latest will fire immediately)
        _logger.debug2(f"Calling Page.enable on {self.tab}")
        _raise_error(await self.tab.send_command("Page.enable"))

        # Enabling javascript events (for javascript_ready)
        _logger.debug2(f"Calling Runtime.enable on {self.tab}")
        _raise_error(await self.tab.send_command("Runtime.enable"))

        self._current_js_id = _dtools.get_js_id(await javascript_ready)

        await page_ready  # don't care result, ready is ready

        # this runs *after* page load because running it first thing
        # requires a couple extra lines
        self.js_logger.reset()

    # reload is truly so close to navigate
    async def reload(self):
        """Reload the tab, and set the javascript runtime id."""
        _logger.debug(f"Reloading tab {self.tab} with javascript.")

        javascript_ready = _subscribe_new(self.tab, "Runtime.executionContextCreated")

        page_ready = _subscribe_new(self.tab, "Page.loadEventFired")

        _logger.debug2(f"Calling Page.reload on {self.tab}")
        _raise_error(await self.tab.send_command("Page.reload"))

        self._current_js_id = _dtools.get_js_id(await javascript_ready)

        await page_ready

        self.js_logger.reset()

    async def _calc_fig(
        self,
        spec: fig_tools.Spec,
        *,
        topojson: str | None,
        render_prof,
        stepper,
    ) -> bytes:
        render_prof.profile_log.tick("sending javascript")

        if _estimate_json_size(spec) > _CHUNK_SIZE:
            result = await self._calc_fig_large(
                spec,
                topojson=topojson,
                stepper=stepper,
            )
        else:
            kaleido_js_fn = (
                r"function(spec, ...args)"
                r"{"
                r"return kaleido_scopes.plotly(spec, ...args).then(JSON.stringify);"
                r"}"
            )
            result = await _dtools.exec_js_fn(
                self.tab,
                self._current_js_id,
                kaleido_js_fn,
                spec,
                topojson,
                stepper,
            )
        _raise_error(result)
        render_prof.profile_log.tick("javascript sent")

        _logger.debug2(f"Result of function call: {result}")
        js_response = _dtools.check_kaleido_js_response(result)

        if (response_format := js_response.get("format")) == "pdf":
            render_prof.profile_log.tick("printing pdf")
            img_raw = await _dtools.print_pdf(self.tab)
            render_prof.profile_log.tick("pdf printed")
        else:
            img_raw = js_response["result"]

        if response_format not in _TEXT_FORMATS:
            res = base64.b64decode(img_raw)
        else:
            res = str.encode(img_raw)

        render_prof.data_out_size = len(res)
        render_prof.js_log = self.js_logger.log
        return res


    async def _calc_fig_large(
        self,
        spec: fig_tools.Spec,
        *,
        topojson: str | None,
        stepper,
    ):
        """
        Handle large specs by streaming big bdata arrays to Chrome separately.

        Extracts large base64-encoded data values from the spec, sends the
        lightweight skeleton via a single CDP call, then streams each large
        bdata string in chunks that stay under both the CDP pipe-message limit
        and V8's max string length.
        """
        extractions: list[tuple[int, str, str]] = []
        fig_data = spec.get("data", {}).get("data", [])
        for i, trace in enumerate(fig_data):
            for key, value in list(trace.items()):
                if (
                    isinstance(value, dict)
                    and "bdata" in value
                    and len(value["bdata"]) > _CHUNK_SIZE
                ):
                    extractions.append((i, key, value["bdata"]))
                    trace[key] = {**value, "bdata": ""}

        await _dtools.exec_js_fn(
            self.tab,
            self._current_js_id,
            r"function(spec, ...args)"
            r"{ window.__kaleido_spec = spec;"
            r"  window.__kaleido_args = args; }",
            spec,
            topojson,
            stepper,
        )

        for trace_idx, key, bdata in extractions:
            chunks = [
                bdata[j : j + _CHUNK_SIZE]
                for j in range(0, len(bdata), _CHUNK_SIZE)
            ]
            await _dtools.exec_js_fn(
                self.tab,
                self._current_js_id,
                r"function() { window.__kaleido_bd = []; }",
            )
            for chunk in chunks:
                await _dtools.exec_js_fn(
                    self.tab,
                    self._current_js_id,
                    r"function(c) { window.__kaleido_bd.push(c); }",
                    chunk,
                )
            await _dtools.exec_js_fn(
                self.tab,
                self._current_js_id,
                f"function() {{"
                f"window.__kaleido_spec.data.data[{trace_idx}]"
                f"['{key}'].bdata = window.__kaleido_bd.join('');"
                f"delete window.__kaleido_bd;"
                f"}}",
            )

        return await _dtools.exec_js_fn(
            self.tab,
            self._current_js_id,
            r"function()"
            r"{ var s = window.__kaleido_spec,"
            r"      a = window.__kaleido_args;"
            r"  delete window.__kaleido_spec;"
            r"  delete window.__kaleido_args;"
            r"  return kaleido_scopes.plotly(s, ...a).then(JSON.stringify); }",
        )
