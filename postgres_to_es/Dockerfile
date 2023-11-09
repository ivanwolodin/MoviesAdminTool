FROM python:3.12

WORKDIR /opt/app

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip --no-cache-dir && pip install -r requirements.txt --no-cache-dir 

COPY . .

# CMD [ "python", "-u", "main.py"]
RUN chmod +x /opt/app/entrypoint.sh
ENTRYPOINT ["/opt/app/entrypoint.sh"]