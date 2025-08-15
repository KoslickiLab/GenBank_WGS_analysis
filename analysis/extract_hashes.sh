#!/bin/bash
python /scratch/dmk333_new/GenBank_WGS_analysis/analysis/wgs_sketch_union_ChatGPT.py extract --input /mnt/ramdisk/wgs_sketches --out /mnt/ramdisk/extracted_hashes --processes 128 --partition-bits 10 --partition-mode low --max-in-flight 200
