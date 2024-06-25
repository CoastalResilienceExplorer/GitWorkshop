FROM ghcr.io/osgeo/gdal:ubuntu-full-latest
WORKDIR /app
RUN apt update && apt install -y \
    python3-pip 
    
COPY ./requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt --break-system-packages
COPY . /app