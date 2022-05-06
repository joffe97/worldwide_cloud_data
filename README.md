# World-wide cloud data

### Documentation
https://www.overleaf.com/read/prhxhpqvphwy

### Satelites

##### Himawari Satellite Data:
* https://registry.opendata.aws/noaa-himawari/
* https://noaa-himawari8.s3.amazonaws.com/index.html

##### NOAA GOES Satellite Data:
* https://registry.opendata.aws/noaa-goes/
* https://www.star.nesdis.noaa.gov/GOES/

##### EUMETSAT Satellite Data:
* https://data.eumetsat.int/data/map/EO:EUM:DAT:MSG:CLM (Needs account but “data policy” field says free and unrestricted)

### How to run:

```bash
# Build the image
docker build wwclouds/ -t wwclouds

# Run the image
docker run -it wwclouds:latest

# Wait until the program is finished (this might take some time)

# Get the container id by using the following command
docker container ls

# Copy the products into a local directory. 
# Note that the correct container id must be used.
docker cp <container_id>:/usr/src/wwclouds/data/products .
```
