"""The only process which evaluates customer Python source.

This runner is deliberately tiny. Docker/OCI constraints, rather than this
module, are the enforcement boundary. The checks below make failures clear and
provide defense in depth if a permitted runtime capability is accidentally
broadened.
"""

import ast
import builtins
import json
import math
import os
import signal
import sys

ALLOWED_IMPORTS = {
    "collections", "datetime", "decimal", "functools", "itertools",
    "json", "math", "operator", "re", "statistics", "string", "typing",
}


class SandboxFailure(Exception):
    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = {key: value for key, value in details.items() if value is not None}


def write_frame(status, **values):
    sys.__stdout__.write(json.dumps({"status": status, **values}, separators=(",", ":"), ensure_ascii=False))
    sys.__stdout__.flush()


def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    root = name.split(".", 1)[0]
    if level != 0 or root not in ALLOWED_IMPORTS:
        raise SandboxFailure("CODE_IMPORT_FORBIDDEN", "Python import is not in the sandbox allowlist")
    return builtins.__import__(name, globals, locals, fromlist, level)


def validate_source(source):
    try:
        tree = ast.parse(source, filename="workflow_code.py", mode="exec")
    except SyntaxError as error:
        raise SandboxFailure("CODE_SYNTAX_ERROR", "Python source has invalid syntax", source_line=error.lineno, source_column=error.offset)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
            root = (module or "").split(".", 1)[0]
            level = getattr(node, "level", 0)
            if level != 0 or root not in ALLOWED_IMPORTS:
                raise SandboxFailure("CODE_IMPORT_FORBIDDEN", "Python import is not in the sandbox allowlist", source_line=node.lineno, source_column=node.col_offset + 1)
    entries = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main"]
    if len(entries) != 1:
        raise SandboxFailure("CODE_ENTRYPOINT_INVALID", "Python source must define exactly one main(inputs)")
    entry = entries[0]
    if (len(entry.args.posonlyargs) != 0 or len(entry.args.args) != 1 or entry.args.args[0].arg != "inputs"
            or entry.args.vararg is not None or entry.args.kwonlyargs or entry.args.kwarg is not None or entry.args.defaults):
        raise SandboxFailure("CODE_ENTRYPOINT_INVALID", "main must have exactly one inputs parameter", source_line=entry.lineno, source_column=entry.col_offset + 1)


def safe_builtins():
    names = ("abs", "all", "any", "bool", "dict", "enumerate", "float", "int", "isinstance", "len", "list", "max", "min", "range", "reversed", "round", "set", "sorted", "str", "sum", "tuple", "zip")
    return {name: getattr(builtins, name) for name in names} | {"__import__": guarded_import}


def execute(frame):
    source = frame.get("source_code")
    inputs = frame.get("inputs")
    if not isinstance(source, str) or not isinstance(inputs, dict):
        raise SandboxFailure("SANDBOX_PROTOCOL_ERROR", "sandbox request is invalid")
    validate_source(source)
    namespace = {"__builtins__": safe_builtins(), "math": math}
    timeout_ms = int(os.environ.get("EVALFROG_SANDBOX_TIMEOUT_MS", "30000"))
    def deadline(_signal, _frame):
        raise SandboxFailure("SANDBOX_EXECUTION_TIMEOUT", "sandbox execution exceeded the fixed timeout")
    previous = signal.signal(signal.SIGALRM, deadline)
    signal.setitimer(signal.ITIMER_REAL, timeout_ms / 1000)
    try:
        try:
            exec(compile(source, "workflow_code.py", "exec"), namespace, namespace)
            output = namespace["main"](inputs)
        except SandboxFailure:
            raise
        except MemoryError:
            raise SandboxFailure("SANDBOX_MEMORY_LIMIT_EXCEEDED", "sandbox memory limit was exceeded")
        except Exception as error:
            raise SandboxFailure("CODE_RUNTIME_ERROR", "Python code raised an exception", source_line=user_source_line(error))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    if not isinstance(output, dict):
        raise SandboxFailure("CODE_OUTPUT_NOT_OBJECT", "main(inputs) must return a JSON object")
    try:
        json.dumps(output, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError):
        raise SandboxFailure("CODE_OUTPUT_NOT_OBJECT", "main(inputs) output is not JSON serializable")
    return output


def user_source_line(error):
    trace = getattr(error, "__traceback__", None)
    line = None
    while trace is not None:
        if trace.tb_frame.f_code.co_filename == "workflow_code.py":
            line = trace.tb_lineno
        trace = trace.tb_next
    return line


def main():
    try:
        frame = json.loads(sys.stdin.readline())
        write_frame("ok", output=execute(frame))
    except SandboxFailure as error:
        write_frame("error", code=error.code, message=error.message, details=error.details)
    except Exception:
        write_frame("error", code="SANDBOX_PROTOCOL_ERROR", message="sandbox runner failed", details={})


main()
