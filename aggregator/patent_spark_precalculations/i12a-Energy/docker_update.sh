#!/bin/bash

# Get the version number from user input
version=$1

# Delete all Docker images with the same name and different version
docker images | grep "docker-registry.services.opix.ai/aggregator" | grep -v "$version" | awk '{print $1 ":" $2}' | xargs -r docker rmi -f

# Activate Poetry virtual environment
source $(poetry env info --path)/bin/activate

# Freeze dependencies using pip
pip freeze > requirements.txt

# Add changes to git
git add .

# Run pre-commit hooks
pre-commit run

# Commit changes with the given version number
git commit -m "new commit with version $version"

# Build the Docker image with the given version number
docker build --file docker/aggregator.Dockerfile -t docker-registry.services.opix.ai/aggregator:$version .

# Push the Docker image to the registry
docker push docker-registry.services.opix.ai/aggregator:$version
