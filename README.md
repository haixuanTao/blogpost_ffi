# Reproduction step

```bash
mkdir blogpost_ffi
maturin init # pyo3
maturin develop
python -c "import blogpost_ffi; blogpost_ffi.sum_as_string(1,1)"
```