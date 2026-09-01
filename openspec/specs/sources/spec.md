# sources Specification

## Purpose

A `Source` reads raw input and yields it into a pipeline as `RawRecord`
messages, feeding [[pipeline-core]]'s `run()`.

## Requirements

### Requirement: Source Protocol
The system SHALL define `Source` as a structural `Protocol` with one async
method, `get_records() -> AsyncIterator[RawRecord]`.

#### Scenario: Any object with a matching get_records is a valid Source
- **WHEN** an object exposes an async `get_records()` yielding `RawRecord`s
- **THEN** it satisfies `Source` and can be passed to `run()` without inheriting from anything

### Requirement: IterableSource
`IterableSource(records)` SHALL wrap an in-memory iterable of dicts as a
`Source` — useful for tests and small batch jobs.

#### Scenario: Each dict becomes a RawRecord
- **WHEN** `IterableSource([{"name": "ada"}]).get_records()` is iterated
- **THEN** it yields `RawRecord(value={"name": "ada"}, time=<current time>)`

### Requirement: JSONLinesSource
`JSONLinesSource(path)` SHALL read a JSON Lines file, one JSON object per
non-blank line, yielding a `RawRecord` per line.

#### Scenario: Each JSON line becomes a RawRecord
- **WHEN** `JSONLinesSource(path).get_records()` is iterated over a file with one JSON object per line
- **THEN** it yields one `RawRecord` per line, `value` set to the parsed object

#### Scenario: Blank lines are skipped
- **WHEN** the file contains a blank (or whitespace-only) line
- **THEN** no `RawRecord` is yielded for that line

### Requirement: CSVSource
`CSVSource(path)` SHALL read a CSV file via `csv.DictReader`, yielding a
`RawRecord` per row keyed by the header row.

#### Scenario: Each CSV row becomes a RawRecord
- **WHEN** `CSVSource(path).get_records()` is iterated over a CSV file with a header row
- **THEN** it yields one `RawRecord` per data row, `value` set to a dict keyed by the header
