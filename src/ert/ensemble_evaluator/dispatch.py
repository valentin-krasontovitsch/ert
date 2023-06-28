import asyncio
import logging
import time
from collections import OrderedDict, defaultdict

from ert.ensemble_evaluator import identifiers

logger = logging.getLogger(__name__)


class BatchingDispatcher:
    def __init__(self, loop, timeout, max_batch=100):
        self._timeout = timeout
        self._input_timeout = timeout
        self._max_batch = max_batch
        self._input_max_batch = max_batch

        self._LOOKUP_MAP = defaultdict(list)
        self._running = True
        self._buffer = []

        # Schedule task
        self._task = asyncio.ensure_future(self._job(), loop=loop)
        self._task.add_done_callback(self._done_callback)

    def _done_callback(self, *args):
        try:
            failure = self._task.exception()
            if failure is not None:
                logger.warning(f"exception in batcher: {failure}")
            else:
                logger.debug("batcher finished normally")
                return
        except asyncio.CancelledError as ex:
            logger.warning(f"batcher was cancelled: {ex}")

        # call any registered handlers for FAILED. since we don't have
        # an event, pass empty list and let handler decide how to proceed
        funcs = self._LOOKUP_MAP[identifiers.EVTYPE_ENSEMBLE_FAILED]
        asyncio.gather(*[f([]) for f, _ in funcs])

    async def _work(self):
        if len(self._buffer) == 0:
            logger.debug("no events to be processed in queue")
            return

        t0 = time.time()
        batch_of_events_for_processing, self._buffer = (
            self._buffer[: self._max_batch],
            self._buffer[self._max_batch :],
        )
        left_in_queue = len(self._buffer)

        function_to_events_map = OrderedDict()
        for f, event in batch_of_events_for_processing:
            if f not in function_to_events_map:
                function_to_events_map[f] = []
            function_to_events_map[f].append(event)

        def done_logger(_):
            processing_time = time.time()-t0
            logger.debug(
                f"processed {len(batch_of_events_for_processing)} events in "
                f"{processing_time:.6f}s. "
                f"{left_in_queue} left in queue"
            )
            if processing_time >= 10:
                logger.debug(f"adjusting batch size and timeout...")
                self._max_batch = int(self._input_max_batch / 10)
                self._timeout = 10 * self._input_timeout
            else:
                self._max_batch = self._input_max_batch
                self._timeout = self._input_timeout

        events_handling = asyncio.gather(
            *[f(events) for f, events in function_to_events_map.items()]
        )
        events_handling.add_done_callback(done_logger)
        await events_handling

    async def _job(self):
        while self._running:
            await asyncio.sleep(self._timeout)
            await self._work()

        # Make sure no events are lingering
        await self._work()

    async def join(self):
        self._running = False
        try:
            await self._task
        except BaseException:
            # if result is exception it should have been handled by
            # done-handler, but also avoid killing the caller here
            pass

    def register_event_handler(self, event_types, function, batching=True):
        if not isinstance(event_types, set):
            event_types = {event_types}
        for event_type in event_types:
            self._LOOKUP_MAP[event_type].append((function, batching))

    async def handle_event(self, event):
        for function, batching in self._LOOKUP_MAP[event["type"]]:
            if batching:
                if self._task.done():
                    raise asyncio.InvalidStateError(
                        "trying to handle event after batcher is done"
                    )
                self._buffer.append((function, event))
            else:
                await function(event)
