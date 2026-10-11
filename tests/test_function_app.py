"""Static checks on function_app.py that the Azure Python worker enforces.

The worker refuses a binding annotation such as func.Out[list[str]] ("invalid
non-type annotation"), and when one function fails to load, none load: the
portal then shows no functions and no timer runs. These tests read the source,
so they need no azure-functions package. They also check the queue wiring:
each worker's poison output must be the queue Azure itself poisons into.
"""

import ast
from pathlib import Path

from integration.queue_sync import QUEUES, poison_queue

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


def queue_bindings(source: str) -> dict[str, dict[str, str]]:
    """{function: {arg_name: queue_name}} from the queue trigger and output decorators."""
    found: dict[str, dict[str, str]] = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr in {"queue_trigger", "queue_output"}
            ):
                continue
            kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in decorator.keywords}
            found.setdefault(node.name, {})[kwargs["arg_name"]] = kwargs["queue_name"]
    return found


def test_each_worker_poisons_into_the_queue_azure_uses():
    bindings = queue_bindings(FUNCTION_APP.read_text(encoding="utf-8"))
    workers = {name: queues for name, queues in bindings.items() if "message" in queues}
    assert sorted(queues["message"] for queues in workers.values()) == sorted(QUEUES.values())
    for name, queues in workers.items():
        assert queues["poison"] == poison_queue(queues["message"]), name


def test_timers_and_the_retry_worker_fill_the_right_queues():
    bindings = queue_bindings(FUNCTION_APP.read_text(encoding="utf-8"))
    assert bindings["scheduled_onekey_full"]["work"] == QUEUES["full"]
    assert bindings["scheduled_onekey_delta"]["work"] == QUEUES["retry"]
    assert bindings["scheduled_onekey_retry_worker"]["delta"] == QUEUES["delta"]


def test_full_and_delta_workers_continue_on_their_own_queue():
    # A part that spent its time budget queues the next part where it came from.
    bindings = queue_bindings(FUNCTION_APP.read_text(encoding="utf-8"))
    assert bindings["scheduled_onekey_full_worker"]["more"] == QUEUES["full"]
    assert bindings["scheduled_onekey_delta_worker"]["more"] == QUEUES["delta"]
    assert "more" not in bindings["scheduled_onekey_retry_worker"]


def test_every_binding_is_a_parameter_of_its_function():
    # A binding whose arg_name the function does not take stops the worker
    # from loading, and with it every function in the app.
    tree = ast.parse(FUNCTION_APP.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        params = {arg.arg for arg in node.args.args}
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                for keyword in decorator.keywords:
                    if keyword.arg == "arg_name":
                        assert ast.literal_eval(keyword.value) in params, node.name
