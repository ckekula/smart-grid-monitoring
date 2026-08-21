import time
from collections.abc import Iterator
from itertools import groupby
from operator import itemgetter

SIMULATED_DAY_SECONDS = 60.0
INTERVALS_PER_DAY = 96
INTERVAL_SECONDS = SIMULATED_DAY_SECONDS / INTERVALS_PER_DAY


def simulate_events(events: list[dict]) -> Iterator[dict]:
    """
    Simulate the dataset as a continuous stream.
    One simulated day = one real minute.

    The source contains 15-minute readings, therefore:
        96 intervals/day
        60 seconds/day
        0.625 seconds/interval

    Events sharing the same timestamp are emitted together
    """

    events = sorted(events, key=lambda event: event["timestamp"])

    grouped = groupby(
        events,
        key=itemgetter("timestamp"),
    )

    first_interval = True

    for _, group in grouped:
        interval_events = list(group)

        if not first_interval:
            time.sleep(INTERVAL_SECONDS)

        first_interval = False

        yield from interval_events
