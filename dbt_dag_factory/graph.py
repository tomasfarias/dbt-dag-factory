"""Module contains the ManifestGraph class."""
from __future__ import annotations

import typing


class ManifestGraph:
    """A graph made from a dbt manifest.json file."""

    def __init__(self, manifest: dict[str, typing.Any]):
        """Initialize a ManifestGraph."""
        self.manifest = manifest

    def get_node(self, node_id: str) -> typing.Optional[dict[str, typing.Any]]:
        """Return a dbt node from the manifest."""
        return self.manifest["nodes"].get(node_id, None)

    def root_models(self) -> typing.Generator[str, None, None]:
        """Return all models without parents."""
        for model, parents in self.manifest["parent_map"].items():
            if len(parents) == 0:
                yield model

    def get_first_children(self, model: str):
        """Return the models first children if found in children_map."""
        return self.manifest["child_map"].get(model, [])

    def traverse(self, root: str) -> typing.Generator[tuple[str, str], None, None]:
        """Traverse through the manifest graph in a breath-first fashion."""
        visited = []
        to_visit = [root]

        while to_visit:
            current = to_visit.pop(0)
            children = self.get_first_children(current)

            for child in children:
                yield (current, child)

                if child not in visited:
                    visited.append(child)
                    to_visit.append(child)
