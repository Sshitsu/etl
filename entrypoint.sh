wait_for_db(){
  until nc -z "$DB_HOST" "$DB_PORT"; do
    echo "Waiting for Postgres at $DB_HOST:$DB_PORT..."
    sleep 1
  done
}


case "$1" in
  json2db)
    wait_for_db
    java -jar etl-app.jar --source json --action db --file /data/input.json
    ;;
  json2csv)
    java -jar etl-app.jar --source json --action csv --file /data/input.json --out /data/output.csv
    ;;
  api2db)
    wait_for_db
    java -jar etl-app.jar --source api --action db
    ;;
  api2csv)
    java -jar etl-app.jar --source api --action csv --out /data/output.csv
    ;;
  *)
    echo "Usage: $0 {json2db|json2csv|api2db|api2csv}"
    exit 1
    ;;
esac