FROM python:3.9.18
WORKDIR /code

# Update pip
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .

CMD ["python", "./app.py"]
