from pathlib import Path

path = Path('data/task3_demo.bin')
path.parent.mkdir(parents=True, exist_ok=True)
path.write_bytes(b'A'*(4*1024*1024) + b'A'*(4*1024*1024) + b'Unique ending\n')
print('Created', path, 'bytes:', path.stat().st_size)
