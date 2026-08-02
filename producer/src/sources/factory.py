from sources.generator_source import GeneratorSource
from sources.jdbc import JDBCPollingSource

_SOURCE_TYPES = {
    "generator": lambda entry: GeneratorSource(
        events_per_second=entry.get("events_per_second", 10),
    ),
    "jdbc": lambda entry: JDBCPollingSource(
        name=entry["name"],
        dialect=entry["dialect"],
        connection=entry["connection"],
        query=entry["query"],
        watermark_column=entry["watermark_column"],
        watermark_initial=entry["watermark_initial"],
        poll_interval_seconds=entry.get("poll_interval_seconds", 5),
        state_file=entry["state_file"],
        fetch_size=entry.get("fetch_size", 1000),
    ),
}


def build_source(entry: dict):
    factory = _SOURCE_TYPES.get(entry["type"])

    if factory is None:
        raise ValueError(f"Unknown source type '{entry['type']}'. Known types: {list(_SOURCE_TYPES)}")

    return factory(entry)
