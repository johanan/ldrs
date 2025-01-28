#!/bin/bash

if [ ! -f "train-00000-of-00001-bd8cf5be2ae7fd23.parquet" ]; then
  wget https://huggingface.co/datasets/RF0000/pubmed-kinase-abstract/resolve/main/data/train-00000-of-00001-bd8cf5be2ae7fd23.parquet
else
  echo "File already exists"
fi
