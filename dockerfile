FROM python:3.11

# Set Python to run in unbuffered mode so logs appear immediately
ENV PYTHONUNBUFFERED=1

# Copy the packages file into the build
WORKDIR /app
COPY ./ /app/

# Run the install using the packages manifest file
RUN pip install --no-cache-dir -r requirements.txt

# Args come from the SPCS job spec (e.g. --full, object names)
ENTRYPOINT ["python", "-u", "load_salesforce.py"]