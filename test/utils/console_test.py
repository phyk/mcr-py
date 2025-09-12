import pytest
from mcr_py.utils.console import (
    print_tree_from_any,
)  # Replace 'your_module' with the actual module name


def test_print_tree_from_dict(capfd) -> None:
    data = {
        "key1": "value1",
        "key2": {"subkey1": "subvalue1", "subkey2": "subvalue2"},
        "key3": ["item1", "item2"],
    }

    print_tree_from_any(data)

    captured = capfd.readouterr()
    expected_output = (
        ":root:\n"
        "├── key1: value1\n"
        "├── key2\n"
        "│   ├── subkey1: subvalue1\n"
        "│   └── subkey2: subvalue2\n"
        "└── key3\n"
        "    ├── item1\n"
        "    └── item2\n"
    )

    assert captured.out == expected_output


def test_print_tree_from_list(capfd) -> None:
    data = ["item1", {"key1": "value1"}, ["item2", "item3"]]

    print_tree_from_any(data)

    captured = capfd.readouterr()
    expected_output = (
        ":root:\n├── item1\n├── \n│   └── key1: value1\n└── \n    ├── item2\n    └── item3\n"
    )

    assert captured.out == expected_output


def test_invalid_type() -> None:
    with pytest.raises(ValueError, match="Cannot print tree from type <class 'int'>"):
        print_tree_from_any(123)


def test_empty_dict(capfd) -> None:
    data = {}

    print_tree_from_any(data)

    captured = capfd.readouterr()
    expected_output = ":root:\n"

    assert captured.out == expected_output


def test_empty_list(capfd) -> None:
    data = []

    print_tree_from_any(data)

    captured = capfd.readouterr()
    expected_output = ":root:\n"

    assert captured.out == expected_output
