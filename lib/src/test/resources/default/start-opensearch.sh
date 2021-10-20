#!/bin/bash

# Generate the certificates used in the HTTPS tests.
# Copy these into the docker image to make available to client (Java class in this repo) and
# elastic server (docker container started in test setup). Volume mounting is unsolved issue.

OS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}"/ )" >/dev/null 2>&1 && pwd )"
cd $OS_DIR/..
OS_DIR=$(pwd)
export PATH=/usr/share/elasticsearch/jdk/bin/:$PATH

if [[ -z "${IP_ADDRESS}" ]]; then
    IP_ADDRESS=$(hostname -I)
fi

echo
echo "Replacing the ip address in the ${OS_DIR}/config/ssl/instances.yml file with ${IP_ADDRESS}"
sed -i "s/ipAddress/${IP_ADDRESS}/g" ${OS_DIR}/config/instances.yml

echo
echo "Starting OpenSearch ..."
/usr/share/opensearch/opensearch-docker-entrypoint.sh
