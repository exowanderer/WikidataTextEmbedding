docker image rm -f $(docker image ls -aq)
docker container rm -f $(docker container ls -aq)
docker volume rm -f $(docker volume ls -aq)
docker network rm -f $(docker network ls -aq)

docker image prune -f
docker container prune -f
docker network prune -f
docker volume prune -f
docker system prune -f

docker system info
docker system df
