# Build the Docker image
docker build -t wikidata_datadump_textification .

# Run the Docker container with the environment variable and volume mounting
docker run -it \
        -v $(pwd)/datadump:/app/datadump \
        -v $(pwd)/csvfiles:/app/csvfiles \
        -v $(pwd)/sqlitedbs:/app/sqlitedbs \
        -v $HOME/.cache/huggingface/hub:/root/.cache/huggingface/hub \
        -e WIKIMEDIA_TOKEN=$WIKIMEDIA_TOKEN \
        -e N_COMPLETE=1000000 \
        -e EMBED=True \
        -e EMBED_BATCHSIZE=65536 \
        -e CHUNKSIZE=32 \
        -e PIPELINE=item \
        -e COLLECTION='testwikidata' \
        --gpus all \
        wikidata_datadump_textification

# batchsize     time    chunksize
# 128           1m45s   100
# 1024          1m44s   100
# 4096          1m44s   100
# 32768         1m43s   100
# 65536         1m44s   100
# 65536         1m38s   128
# 65536         1m37s   256

# Stay under 8000 bytes chunksize must be 32