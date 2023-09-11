# Reproduction step

```bash
# For tracing
docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest

# For compiling rust library
maturin develop --release

# For testing
python test_script.py
```
