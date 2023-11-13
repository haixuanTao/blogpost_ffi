import blogpost_ffi
import os
import pyarrow as pa
import time
import tracing
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

pa.array([])
value = [1] * 100_000_000

os.system("clear")


# Default benchmark

print(f"---Default Implementation---")

start_time = time.time()

array = blogpost_ffi.create_list(value)

print(f"default: {time.time() - start_time:.3f}s")

assert len(array) == 100_000_000, "Not enough data"
assert array[-1] == 1, "data value is wrong"

print(f"------")
print()

# PyBytes benchmark

print(f"---PyBytes Implementation---")

value_bytes = bytes(value)

start_time = time.time()

array = blogpost_ffi.create_list_bytes(value_bytes)

print(f"bytes: {time.time() - start_time:.3f}s")

assert len(array) == 100_000_000, "Not enough data"
assert array[-1] == 1, "Value is wrong"

print(f"------")
print()

# Arrow benchmark

print(f"---Arrow Implementation---")

value_arrow = pa.array(value, type=pa.uint8())

start_time = time.time()

array = blogpost_ffi.create_list_arrow(value_arrow)

print(f"arrow: {time.time() - start_time:.3f}s")

assert len(array) == 100_000_000, "Not enough data"
assert array[-1].as_py() == 1, f"Value is wrong. Expected: 1, got: {array[-1]}"

print(f"------")
print()

# Debugging eyre

print(f"---Eyre error---")

# Try:
# array = blogpost_ffi.create_list_arrow(1)
#
## This error panics the whole program and is therefore uncatchable.
ERROR_WITHOUT_EYRE = """
thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: PyErr { type: <class 'TypeError'>, value: TypeError('Expected instance of pyarrow.lib.Array, got builtins.int'), traceback: None }', src/lib.rs:45:62
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
Traceback (most recent call last):
  File "/home/peter/Documents/work/blogpost_ffi/test_script.py", line 79, in <module>
    array = blogpost_ffi.create_list_arrow(1)
pyo3_runtime.PanicException: called `Result::unwrap()` on an `Err` value: PyErr { type: <class 'TypeError'>, value: TypeError('Expected instance of pyarrow.lib.Array, got builtins.int'), traceback: None }
"""

try:
    array = blogpost_ffi.create_list_arrow_eyre(1)
except Exception as e:
    print(f"eyre says: {e}")
print(f"------")
print()

# Eyre default
print(f"---Eyre no traceback---")


def abc():
    assert False, "I have no idea what is wrong"


try:
    array = blogpost_ffi.call_func_eyre(abc)
except Exception as e:
    print(f"eyre no traceback says: {e}")

print(f"------")
print()

# Eyre traceback
print(f"---Eyre traceback---")


def abc():
    assert False, "I have no idea what is wrong"


try:
    array = blogpost_ffi.call_func_eyre_traceback(abc)
except Exception as e:
    print(f"eyre traceback says: {e}")

print(f"------")
print()


# Unbounded Memory Growth
print(f"---Memory Growth---")

print(f"-->Open a Memory analyzer")
array = blogpost_ffi.unbounded_memory_growth()

print(f"------")
print()

# Unbounded Memory Growth
print(f"---Memory Growth---")

print(f"-->Open a Memory analyzer")
array = blogpost_ffi.bounded_memory_growth()

print(f"------")
print()

# GIL Lock
print(f"---GIL Lock---")

array = blogpost_ffi.gil_lock()

print(f"------")
print()

# GIL unlock
print(f"---GIL Lock---")

array = blogpost_ffi.gil_unlock()

print(f"------")
print()

# Global tracing
print(f"---Global tracing---")


def abc(cx):
    propagator = TraceContextTextMapPropagator()
    context = propagator.extract(carrier=cx)

    with tracing.tracer.start_as_current_span(
        name="Python_span", context=context
    ) as child_span:
        child_span.add_event("in Python!")
        output = {}
        tracing.propagator.inject(output)
        time.sleep(2)
    return output


array = blogpost_ffi.global_tracing(abc)

print(f"------")
print()
