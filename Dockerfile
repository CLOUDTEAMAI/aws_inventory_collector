FROM python:3.9

ENV AWS_STS_REGIONAL_ENDPOINTS="regional"

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]

CMD ["main.py"]