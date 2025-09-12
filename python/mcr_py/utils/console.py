from rich import print
from rich.tree import Tree
from typing_extensions import Any


def print_tree_from_any(data: Any, root_name: str = ":root:") -> None:
    """
    Prints a tree structure from the given data, which can be a dictionary or a list.

    :param data: Any - The data to be converted into a tree structure.
                     It can be a dictionary or a list.
    :param root_name: str - The name of the root node in the tree. Defaults to ":root:".
    :raises ValueError: If the data type is neither a dictionary nor a list.
    """
    if isinstance(data, dict):
        tree = _build_tree_from_dict(data, parent=Tree(root_name))
    elif isinstance(data, list):
        tree = _build_tree_from_list(data, parent=Tree(root_name))
    else:
        msg = f"Cannot print tree from type {type(data)}"
        raise ValueError(msg)
    print(tree)


def _build_tree_from_dict(data, parent=None):
    """
    Recursively builds a tree structure from a dictionary.

    :param data: dict - The dictionary to convert into a tree structure.
    :param parent: Tree - The parent Tree node to which the current dictionary entries will be added.
                         If None, a new root Tree node will be created.
    :returns: Tree - The parent Tree node with the added structure from the dictionary.
    """
    if parent is None:
        parent = Tree(":root:")
    for key, value in data.items():
        if isinstance(value, dict):
            subtree = parent.add(f"{key}")
            _build_tree_from_dict(value, parent=subtree)
        elif isinstance(value, list):
            subtree = parent.add(f"{key}")
            _build_tree_from_list(value, parent=subtree)
        else:
            parent.add(f"{key}: {value}")

    return parent


def _build_tree_from_list(data, parent=None):
    """
    Recursively builds a tree structure from a list.

    :param data: list - The list to convert into a tree structure.
    :param parent: Tree - The parent Tree node to which the current list items will be added.
                         If None, a new root Tree node will be created.
    :returns: Tree - The parent Tree node with the added structure from the list.
    """
    if parent is None:
        parent = Tree(":root:")
    for item in data:
        if isinstance(item, dict):
            item_subtree = parent.add("")
            _build_tree_from_dict(item, parent=item_subtree)
        elif isinstance(item, list):
            item_subtree = parent.add("")
            _build_tree_from_list(item, parent=item_subtree)
        else:
            parent.add(str(item))

    return parent
