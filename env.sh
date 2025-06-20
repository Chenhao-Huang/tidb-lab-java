  #!/bin/sh

  ## You only need to change this part
  export TIDB_HOST=${LAB:SERVERLESS_CLUSTER_HOST}
  export TIDB_PORT=${LAB:SERVERLESS_CLUSTER_PORT}
  export TIDB_USER='${LAB:SERVERLESS_CLUSTER_USERNAME}'
  export TIDB_PASSWORD='${LAB:SERVERLESS_CLUSTER_PASSWORD}'
  export TIDB_DB_NAME=${LAB:SERVERLESS_CLUSTER_DATABASE_NAME}
  export USE_SSL='true'

  ## Do not change the following part
  jdbc_url="jdbc:mysql://${TIDB_HOST}:${TIDB_PORT}/${TIDB_DB_NAME}"
  if [ 'true' = "${USE_SSL}" ]; then
    jdbc_url="${jdbc_url}?sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3"
  fi

  export TIDB_JDBC_URL=${jdbc_url}