# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Upgrade the pip version to the most recent version
RUN pip install --upgrade pip

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cu118
#  torchvision torchaudio


# Copy the rest of the application code into the container
COPY ./wikidata_datadump_item_textification.py ./pipeline.py
COPY ./post_process_embed_df.py ./post_process_embed_df.py

# ARG FUNCTION_DIR="/var/task"
# RUN mkdir -p ${FUNCTION_DIR}
# COPY summarize.py ${FUNCTION_DIR}
# COPY --from=model /tmp/model ${FUNCTION_DIR}/model

# Create a volume to store the output CSV files
VOLUME /app/csvfiles
VOLUME /app/datadump
VOLUME /app/sqlitedbs

# Set the environment variable inside the Docker container
ENV WIKIMEDIA_TOKEN=$WIKIMEDIA_TOKEN
ENV N_COMPLETE=$N_COMPLETE
ENV EMBED=$EMBED
ENV EMBED_BATCHSIZE=$EMBED_BATCHSIZE
ENV PIPELINE=$PIPELINE
ENV CHUNKSIZE=$CHUNKSIZE
ENV COLLECTION_NAME=COLLECTION_NAME

# Run the Python script
CMD ["python", "pipeline.py"]
# CMD ["python", "post_process_embed_df.py"]