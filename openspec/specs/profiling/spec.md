# profiling Specification

## Purpose

Runtime and static introspection of a [[pipeline-core]] `Pipeline`'s shape:
per-step call counts, timing, emitted-message counts, and (in blueprint
mode) the accepted/returned types and consume flag of every step — a
picture of the pipeline's structure alongside its runtime numbers.

## Requirements

### Requirement: get_profile_data
`Pipeline.get_profile_data()` SHALL return a dict describing every step
(including sink steps and the synthetic entry point), sorted by stage.

#### Scenario: Every step is represented once
- **WHEN** `get_profile_data()` is called
- **THEN** the returned `"pipeline"` list contains one entry per registered step (`add_handler` steps, `add_sink` steps, the catch-all, and the entry point), sorted ascending by stage

#### Scenario: Each entry reports live counters
- **WHEN** a step has processed messages since the last rebuild
- **THEN** its entry includes `calls`, `time` (cumulative seconds), and `emitted` (count of messages it produced)

#### Scenario: Each entry reports its declared shape
- **WHEN** an entry is produced for a step
- **THEN** it includes `name`, `stage`, `accepts` (type name), `returns` (comma-joined type names), `consume`, and `description` (the step's docstring, or `None`)

#### Scenario: Each entry reports observed outgoing message types
- **WHEN** a step has emitted one or more message types during the run
- **THEN** its `"emits"` list has one item per distinct type actually observed, each with `message_type`, `count`, and the `next` step name it routed to (or `None` if no downstream step would accept it)

### Requirement: format_profile Default Rendering
`Pipeline.format_profile(profile)` SHALL render one line per step —
name, cumulative time, percentage of total time, call count, and emitted
count — grouped under stage banners.

#### Scenario: An empty pipeline reports "No Profile"
- **WHEN** `format_profile(profile)` is called on a profile with an empty `"pipeline"` list
- **THEN** it returns the literal string `"No Profile"`

#### Scenario: Steps are grouped under stage banners
- **WHEN** `format_profile(profile)` is called on a non-empty profile
- **THEN** each stage transition is preceded by a `[ STAGE_NAME = value ]` banner line, followed by that stage's step lines

### Requirement: format_profile Blueprint Rendering
`Pipeline.format_profile(profile, blueprint=True)` SHALL additionally show
each step's accepted/returned types, consume flag, per-call latency, and
docstring inline.

#### Scenario: Blueprint groups by STAGE label
- **WHEN** `format_profile(profile, blueprint=True)` is called
- **THEN** each stage transition is preceded by a `STAGE: <label> - <value>` line

#### Scenario: Blueprint shows type and consume info per step
- **WHEN** an entry is rendered in blueprint mode
- **THEN** its line shows `name(accepts)`, whether it is `consumed`, its `returns` type(s) (or `None`), cumulative time, percentage, call count, per-call latency in ms, and emitted count

#### Scenario: Blueprint includes the step's docstring when present
- **WHEN** a step has a non-empty docstring
- **THEN** it is rendered as an indented line immediately below that step's summary line

### Requirement: set_stage_labels
`Pipeline.set_stage_labels(stage_cls)` SHALL relabel the profiler's stage
banners for a custom `Stage`-shaped class.

#### Scenario: Custom stage constants relabel the profile output
- **WHEN** `set_stage_labels(MyStages)` is called with a class exposing its own int constants
- **THEN** subsequent `get_profile_data()`/`format_profile()` calls use `MyStages`' names for stage banners instead of the default `Stage` names
