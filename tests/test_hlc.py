from distrocache_common.hlc import HLC, HybridClock


class FakeClock:
    """A wall clock the test drives by hand."""

    def __init__(self) -> None:
        self.ms = 1000

    def __call__(self) -> int:
        return self.ms


def test_ticks_are_strictly_increasing_within_one_millisecond() -> None:
    clock = FakeClock()
    hlc = HybridClock("c", now_ms=clock)

    first = hlc.tick()
    second = hlc.tick()
    third = hlc.tick()

    # The wall clock did not move, so the logical counter orders the three ticks.
    assert first < second < third
    assert (first.physical_ms, first.logical) == (1000, 0)
    assert (second.physical_ms, second.logical) == (1000, 1)
    assert (third.physical_ms, third.logical) == (1000, 2)


def test_advancing_the_wall_clock_resets_the_logical_counter() -> None:
    clock = FakeClock()
    hlc = HybridClock("c", now_ms=clock)

    hlc.tick()
    hlc.tick()  # logical now at 1
    clock.ms = 2000
    later = hlc.tick()

    assert (later.physical_ms, later.logical) == (2000, 0)


def test_a_backwards_wall_clock_still_increases() -> None:
    clock = FakeClock()
    hlc = HybridClock("c", now_ms=clock)

    before = hlc.tick()
    clock.ms = 500  # the OS clock jumped backwards
    after = hlc.tick()

    assert after > before
    # The physical time is held at the last high-water mark; the counter advances.
    assert (after.physical_ms, after.logical) == (1000, 1)


def test_ordering_compares_as_a_tuple() -> None:
    assert HLC(1, 0, "a") < HLC(1, 0, "b")  # coordinator id breaks a tie
    assert HLC(1, 5, "z") < HLC(2, 0, "a")  # physical time dominates the logical counter
    assert HLC(1, 1, "a") < HLC(1, 2, "a")  # then the logical counter
