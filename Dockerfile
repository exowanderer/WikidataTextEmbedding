# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY ./wikidata_datadump_textification.py ./wikidata_datadump_textification.py
# COPY ./sqlitedbs/wikidata_qid_pid_labels.db ./sqlitedbs/wikidata_qid_pid_labels.db

# Create a volume to store the output CSV files
VOLUME /app/csvfiles

# Set the environment variable inside the Docker container
ENV WIKIMEDIA_TOKEN=$WIKIMEDIA_TOKEN
ENV N_COMPLETE=$N_COMPLETE
ENV EMBED=$EMBED

# Run the Python script
CMD ["python", "wikidata_datadump_textification.py"]