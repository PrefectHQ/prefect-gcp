"""Discovers all blocks and generates a list of them in the docs."""

from pathlib import Path
from textwrap import dedent

import mkdocs_gen_files
from prefect.blocks.core import Block
from prefect.utilities.importtools import to_qualified_name

COLLECTION_NAME = "prefect_gcp"


def inheritors(klass):
    """
    Finds all classes that inherit from a given class.
    Taken from: https://stackoverflow.com/questions/5881873
    """
    subclasses = set()
    work = [klass]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


blocks = sorted(inheritors(Block), key=to_qualified_name)
module_blocks = {}
for block in blocks:
    block_path = to_qualified_name(block)
    if not block_path.startswith(COLLECTION_NAME):
        continue
    name_components = block_path.split(".")
    collection_name = name_components[0]
    module_nesting = tuple(name_components[1:-1])
    block_name = name_components[-1]
    if module_nesting not in module_blocks:
        module_blocks[module_nesting] = []
    module_blocks[module_nesting].append(block_name)
docs_blocks_list_path = Path("blocks_list.md")

with mkdocs_gen_files.open(docs_blocks_list_path, "w") as generated_file:
    generated_file.write("# Blocks List\n")
    generated_file.write(
        "Below is a list of Blocks available for registration in `prefect-gcp`.\n\n"
    )
    generated_file.write(
        "Note, to use the `load` method on Blocks, you must already have a block document "  # noqa
        "[saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) "  # noqa
        "or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).\n"
    )
    for module_nesting, block_names in module_blocks.items():
        module_path = " ".join(module_nesting)
        module_title = module_path.replace("_", " ").title()
        generated_file.write(f"## {module_title} Module\n")
        for block_name in block_names:
            generated_file.write(
                f"#### [{block_name}][{COLLECTION_NAME}.{module_path}.{block_name}]\n"
            )
        generated_file.write(
            dedent(
                f"""
                To register blocks in this module:
                ```bash
                prefect block register -m prefect_gcp.{module_path}
                ```

                To load the {block_name}:
                ```python
                from prefect import flow
                from {COLLECTION_NAME}.{module_path} import {block_name}

                @flow
                def my_flow():
                    my_block = {block_name}.load("MY_BLOCK_NAME")

                my_flow()
                ```
                </details>
                """
            )
        )
