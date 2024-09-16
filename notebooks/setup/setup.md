#Create docker volume

docker volume create spark-data

#Copy data from local machine to volume has been created

docker run --rm -v spark-data:/data -v /d/Big_data/customer-analytics/data:/source alpine sh -c "cp -R /source/* /data/"

#Start container spark
docker-compose up -d


#Delete docker images
#1. Check containers are running 

docker ps

#2.  Stop the containers
docker stop <containerID>

#3.  Delete the containers
docker rm <containerID>

#4.   Delete the images
docker rmi <imageID> --force
#4.1.  Delete the images useless
docker image prune -a


#5.    Delete the volumes
docker volume rm <volumeName>

#6.     Delete the networks
docker network rm <networkName>

#7. Check again
docker images






