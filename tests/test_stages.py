from pipeloom.stages import Stage, get_stage_names


def test_stage_values_are_in_expected_order():
    assert Stage.EXTRACT < Stage.VALIDATE < Stage.ENRICH < Stage.AGENT < Stage.TRANSFORM < Stage.LOAD < Stage.MONITOR


def test_stage_values_are_exactly_as_specified():
    assert Stage.EXTRACT == 0
    assert Stage.VALIDATE == 10
    assert Stage.ENRICH == 20
    assert Stage.AGENT == 30
    assert Stage.TRANSFORM == 40
    assert Stage.LOAD == 50
    assert Stage.MONITOR == 1_000_000


def test_get_stage_names_returns_sorted_value_name_pairs():
    names = get_stage_names(Stage)
    assert names[0] == (0, "EXTRACT")
    assert names[-1] == (1_000_000, "MONITOR")
    assert names == sorted(names)
