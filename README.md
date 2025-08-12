# GenBank_WGS_analysis
Download, sketch, and enumerate unique hashes of GenBank WGS

Coded with assistance from ChatGPT 5 Pro.

Restartable, fault-tolerant, polite crawler/downloader/sketcher for
`https://ftp.ncbi.nlm.nih.gov/genbank/wgs/`.

## Quick start
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Install sourmash from conda-forge or pipx
# conda install -c conda-forge sourmash
# or: pipx install sourmash
edit config.yaml
./run.sh
```
