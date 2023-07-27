airflow connections add postgres-server \
                --conn-type postgres \
                --conn-host postgres-server \
                --conn-port 1234 \
                --conn-login boaz \
                --conn-password qwer1234!

PGPASSWORD=qwer1234! psql -h postgres-server -p 1234 -U boaz -d boaz -f create_table.sql

# --conn-description CONN_DESCRIPTION
#                     Connection description, optional when adding a connection
# --conn-extra CONN_EXTRA
#                     Connection `Extra` field, optional when adding a connection
# --conn-host CONN_HOST
#                     Connection host, optional when adding a connection
# --conn-json CONN_JSON
#                     Connection JSON, required to add a connection using JSON representation
# --conn-login CONN_LOGIN
#                     Connection login, optional when adding a connection
# --conn-password CONN_PASSWORD
#                     Connection password, optional when adding a connection
# --conn-port CONN_PORT
#                     Connection port, optional when adding a connection
# --conn-schema CONN_SCHEMA
#                     Connection schema, optional when adding a connection
# --conn-type CONN_TYPE
#                     Connection type, required to add a connection without conn_uri
# --conn-uri CONN_URI   Connection URI, required to add a connection without conn_type