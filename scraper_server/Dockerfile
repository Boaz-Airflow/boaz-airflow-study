FROM python:3.10-slim 

RUN useradd -r -s /bin/bash jd_scraper

# set app config option
ENV FastAPI_ENV=production


# Set a directory for the app
WORKDIR /app

# Install Python dependencies
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r ./requirements.txt

RUN pip install uvicorn

# Make static directory
RUN if [ ! -d "/static" ]; then mkdir /static; fi

# Copy the rest of the application's code
COPY . .

EXPOSE 8000
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["python", "server.py"]