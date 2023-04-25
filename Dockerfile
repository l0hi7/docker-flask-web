# Basic flask container

FROM python:3.11


COPY . .
WORKDIR /app/
RUN pip install --no-cache-dir -r requirements.txt



ENTRYPOINT [ "python" ]

CMD ["app.py" ]
