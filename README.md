# Rust-Python Demo course

This repo is the underlying implementation for the followed blogpost: https://dora.carsmos.ai/blog/rust-python

# Development of this repository

```bash
# For tracing
docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest

# For compiling rust library
pip install -r requirements.txt
maturin develop --release

# For testing
python test_script.py
```
