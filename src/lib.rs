use std::{
    collections::HashMap,
    ptr::NonNull,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use eyre::{Context, ContextCompat, Result};

use opentelemetry::{
    global, propagation::Extractor, sdk::propagation::TraceContextPropagator, trace::Tracer,
    Context as OtelContext,
};

use pyo3::{prelude::*, types::PyBytes};
/// Formats the sum of two numbers as string.

#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn create_list(a: Vec<&PyAny>) -> PyResult<Vec<&PyAny>> {
    // ... Imagine some work here...
    Ok(a)
}

/// Create a list from a value
#[pyfunction]
fn create_list_bytes<'a>(py: Python<'a>, a: &'a PyBytes) -> PyResult<&'a PyBytes> {
    let s = a.as_bytes();

    // ... Imagine some work here...

    let output = PyBytes::new_with(py, s.len(), |bytes| {
        bytes.copy_from_slice(s);
        Ok(())
    })?;
    Ok(output)
}

/// Create a list from a value
#[pyfunction]
fn create_list_arrow(py: Python, a: &PyAny) -> PyResult<Py<PyAny>> {
    // ... Imagine some work here...

    let arraydata = arrow::array::ArrayData::from_pyarrow(a).unwrap();

    let buffer = arraydata.buffers()[0].as_slice();

    // ... Imagine some work here, similar to PyBytes...

    // Zero Copy Buffer reference counted
    let arc_s = Arc::new(buffer.to_vec());
    let ptr = NonNull::new(arc_s.as_ptr() as *mut _).unwrap();
    let raw_buffer =
        unsafe { arrow::buffer::Buffer::from_custom_allocation(ptr, 100_000_000, arc_s) };
    let output = arrow::array::ArrayData::try_new(
        arrow::datatypes::DataType::UInt8,
        100_000_000,
        None,
        0,
        vec![raw_buffer],
        vec![],
    )
    .unwrap();

    output.to_pyarrow(py)
}

/// Create a list from a value
#[pyfunction]
fn create_list_eyre(py: Python, a: u8) -> Result<Py<PyAny>> {
    // ... Imagine some work here...
    let s = vec![a; 100_000_000];

    let arc_s = Arc::new(s);

    let ptr = NonNull::new(arc_s.as_ptr() as *mut _).context("could not create ptr")?;

    let _raw_buffer =
        unsafe { arrow::buffer::Buffer::from_custom_allocation(ptr, 100_000_000, arc_s) };
    let output = arrow::array::ArrayData::try_new(
        arrow::datatypes::DataType::UInt8,
        100_000_000,
        None,
        0,
        vec![], // This should not be empty
        vec![],
    )
    .context("could not create array")?;

    output
        .to_pyarrow(py)
        .context("Could not convert to pyarrow type")
}

/// Create a list from a value
#[pyfunction]
fn call_func_eyre(py: Python, func: Py<PyAny>) -> Result<()> {
    // ... Imagine some work here...

    let _call_python = func.call0(py).context("function called failed")?;
    Ok(())
}

fn traceback(err: pyo3::PyErr) -> eyre::Report {
    let traceback = Python::with_gil(|py| err.traceback(py).and_then(|t| t.format().ok()));
    if let Some(traceback) = traceback {
        eyre::eyre!("{traceback}\n{err}")
    } else {
        eyre::eyre!("{err}")
    }
}

/// Create a list from a value
#[pyfunction]
fn call_func_eyre_traceback(py: Python, func: Py<PyAny>) -> Result<()> {
    // ... Imagine some work here...

    let _call_python = func
        .call0(py)
        .map_err(traceback) // this will gives you python traceback.
        .context("function called failed")?;
    Ok(())
}

/// Unbounded memory growth
#[pyfunction]
fn unbounded_memory_growth(py: Python) -> Result<()> {
    for _ in 0..10 {
        let a: Vec<u8> = vec![0; 40_000_000];
        let _ = PyBytes::new(py, &a);

        std::thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

#[pyfunction]
fn bounded_memory_growth(py: Python) -> Result<()> {
    py.allow_threads(|| {
        for _ in 0..10 {
            let a: Vec<u8> = vec![0; 40_000_000];
            Python::with_gil(|py| {
                let _bytes = PyBytes::new(py, &a);

                std::thread::sleep(Duration::from_secs(1));
            });
        }
    });

    // or
    for _ in 0..10 {
        let a: Vec<u8> = vec![0; 40_000_000];
        let pool = unsafe { py.new_pool() };
        let py = pool.python();

        let _bytes = PyBytes::new(py, &a);

        std::thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

/// Function GIL Lock
#[pyfunction]
fn gil_lock() {
    let start_time = Instant::now();
    std::thread::spawn(move || {
        Python::with_gil(|_py| {
            println!(
                "This threaded print was printed after {:#?}",
                &start_time.elapsed()
            )
        });
    });

    std::thread::sleep(Duration::from_secs(10));
}

/// No gil lock
#[pyfunction]
fn gil_unlock() {
    let start_time = Instant::now();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(10));
    });

    Python::with_gil(|_py| println!("1. This was printed after {:#?}", &start_time.elapsed()));

    // or

    let start_time = Instant::now();
    std::thread::spawn(move || {
        Python::with_gil(|_py| println!("2. This was printed after {:#?}", &start_time.elapsed()));
    });
    Python::with_gil(|py| {
        py.allow_threads(|| {
            std::thread::sleep(Duration::from_secs(10));
        })
    });
}

struct MetadataMap<'a>(HashMap<&'a str, &'a str>);

impl<'a> Extractor for MetadataMap<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).cloned()
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0.keys().cloned().collect()
    }
}

fn serialize_context(context: &OtelContext) -> String {
    let mut map = HashMap::new();
    global::get_text_map_propagator(|propagator| propagator.inject_context(context, &mut map));
    let mut string_context = String::new();
    for (k, v) in map.iter() {
        string_context.push_str(k);
        string_context.push(';');
        string_context.push_str(v);
        string_context.push('\n');
    }
    string_context
}

fn deserialize_context(string_context: &str) -> OtelContext {
    let mut map = MetadataMap(HashMap::new());
    for s in string_context.split('\n') {
        let mut values = s.split(';');
        let key = values.next().unwrap();
        let value = values.next().unwrap_or("");
        map.0.insert(key, value);
    }
    global::get_text_map_propagator(|prop| prop.extract(&map))
}
/// No gil lock
#[pyfunction]
fn global_tracing(py: Python, func: Py<PyAny>) {
    // global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    global::set_text_map_propagator(TraceContextPropagator::new());

    let _tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint("172.17.0.1:6831")
        .with_service_name("rust_ffi")
        .install_simple()
        .unwrap();

    let tracer = global::tracer("test");

    let _ = tracer.in_span("parent_python_work", |cx| -> Result<()> {
        std::thread::sleep(Duration::from_secs(1));

        let output = func
            .call1(py, (serialize_context(&cx),))
            .map_err(traceback) // this will gives you python traceback.
            .context("function called failed")?;
        let out_context: String = output.extract(py).unwrap();
        let out_context = deserialize_context(&out_context);

        std::thread::sleep(Duration::from_secs(1));

        let span = tracer.start_with_context("after_python_work", &out_context);

        Ok(())
    });
}

/// A Python module implemented in Rust.
#[pymodule]
fn blogpost_ffi(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(create_list, m)?)?;
    m.add_function(wrap_pyfunction!(create_list_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(create_list_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(create_list_eyre, m)?)?;
    m.add_function(wrap_pyfunction!(call_func_eyre, m)?)?;
    m.add_function(wrap_pyfunction!(call_func_eyre_traceback, m)?)?;
    m.add_function(wrap_pyfunction!(unbounded_memory_growth, m)?)?;
    m.add_function(wrap_pyfunction!(bounded_memory_growth, m)?)?;
    m.add_function(wrap_pyfunction!(gil_lock, m)?)?;
    m.add_function(wrap_pyfunction!(gil_unlock, m)?)?;
    m.add_function(wrap_pyfunction!(global_tracing, m)?)?;
    Ok(())
}
