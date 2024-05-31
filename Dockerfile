# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Upgrade the pip version to the most recent version
RUN pip install --upgrade pip

# setup user to avoid root vs user conflicts
# RUN adduser -D dockeruser
RUN useradd -ms /bin/bash dockeruser
USER dockeruser

# Set the working directory in the container
WORKDIR /home/dockeruser

# Copy the requirements file into the container
COPY --chown=dockeruser:dockeruser requirements.txt requirements.txt

# Install the dependencies
RUN pip install --user --no-cache-dir -r requirements.txt

# Set path to include local user bin
ENV PATH="/home/dockeruser/.local/bin:${PATH}"

# Copy the rest of the application code into the container
COPY --chown=dockeruser:dockeruser ./wikidata_datadump_textification.py ./wikidata_datadump_textification.py
COPY --chown=dockeruser:dockeruser ./post_process_embed_df.py ./post_process_embed_df.py

# Create a volume to store the output CSV files
VOLUME /app/csvfiles

# Set the environment variable inside the Docker container
ENV WIKIMEDIA_TOKEN=$WIKIMEDIA_TOKEN
ENV N_COMPLETE=$N_COMPLETE
ENV EMBED=$EMBED
ENV EMBED_BATCHSIZE=$EMBED_BATCHSIZE

# Run the Python script
CMD ["python", "wikidata_datadump_textification.py"]
# CMD ["python", "post_process_embed_df.py"]