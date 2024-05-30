# Build the Docker image
docker build -t wikidata_datadump_textification .

# Run the Docker container with the environment variable and volume mounting
docker run -it \
        -v /home/jofr/Research/Wikidata:/app/datadump \
        -v $(pwd)/csvfiles:/app/csvfiles \
        -v $(pwd)/sqlitedbs:/app/sqlitedbs \
        -e WIKIMEDIA_TOKEN=$WIKIMEDIA_TOKEN \
        -e N_COMPLETE=100 \
        -e EMBED=False \
        wikidata_datadump_textification

