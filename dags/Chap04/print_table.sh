PGPASSWORD=qwer1234! psql -h postgres-server -p 1234 -U boaz -d boaz -c "select * from boaz;"
PGPASSWORD=qwer1234! psql -h postgres-server -p 1234 -U boaz -d boaz -f check.sql