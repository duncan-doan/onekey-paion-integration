"""Static checks on function_app.py that the Azure Python worker enforces.

The worker refuses a binding annotation such as func.Out[list[str]] ("invalid
non-type annotation"), and when one function fails to load, none load: the
portal then shows no functions and no timer runs. These tests read the source,
so they need no azure-functions package.
"""

import ast
from pathlib import Path

FUNCTION_APP = Path(__file__).resolve().parent.parent / "function_app.py"
BUILTIN_GENERICS = {"list", "dict", "tuple", "set", "frozenset", "type"}


def out_annotations(source: str) -> list[ast.Subscript]:
    return [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "Out"
    ]


def builtin_generic_outs(source: str) -> list[str]:
    return [
        f"line {node.lineno}: {ast.unparse(node)}"
        for node in out_annotations(source)
        if isinstance(node.slice, ast.Subscript)
        and isinstance(node.slice.value, ast.Name)
        and node.slice.value.id in BUILTIN_GENERICS
    ]


def test_out_bindings_use_typing_generics():
    source = FUNCTION_APP.read_text(encoding="utf-8")
    assert out_annotations(source), "expected the queue output bindings"
    assert builtin_generic_outs(source) == []


def test_the_check_catches_a_builtin_generic():
    source = "def f(work: func.Out[list[str]], ok: func.Out[typing.List[str]]): pass"
    assert builtin_generic_outs(source) == ["line 1: func.Out[list[str]]"]
