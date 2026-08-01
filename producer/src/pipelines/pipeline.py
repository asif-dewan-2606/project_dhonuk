from dataclasses import dataclass

from publishers.base import Publisher
from sources.base import Source


@dataclass(slots=True)
class Pipeline:
    name: str
    source: Source
    publisher: Publisher