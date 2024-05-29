# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Create a volume to store the output CSV files
VOLUME /app/output

# Run the Python script
CMD ["python", "wikidata_datadump_textification.py"]

# CMD ["python", "post_process_embed_df.py"]
