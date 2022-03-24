#!/bin/sh

TARGET=incoming/
processed=processed/
failed=failed/
partition_column=Region

inotifywait -m $TARGET -e create -e moved_to -e close_write |
while read path action file; do
   if [ $action == "MOVED_TO" ] || [ $action == "CLOSE_WRITE,CLOSE" ]; 
   then
      input_path="/data/"$path$file
      file_type=${file#*.}"toparquet"
      output_path="/data/output/"
      dag_run_id=$(curl -s -X POST http://demeter.jheeta.ca:8080/api/v1/dags/poc_etl_demo/dagRuns -H 'content-type: application/json' --user "admin:admin" -d "{\"conf\": { \"input_path\": \"$input_path\", \"output_path\": \"$output_path\", \"file_type\": \"$file_type\", \"partition_column\": \"$partition_column\" }}" | jq '.dag_run_id' | tr -d '"')
      status=$(curl -s -X GET http://demeter.jheeta.ca:8080/api/v1/dags/poc_etl_demo/dagRuns/$dag_run_id -H 'content-type: application/json' --user "admin:admin" | jq '.state' | tr -d '"' )
      echo  "File" $path$file "has been" $status "output folder will be" $output_path "partioned by" $partition_column "and the unique dag run id is" $dag_run_id
      while [ $status = "running" ] || [ $status = 'queued' ]
      do
         status=$(curl -s -X GET http://demeter.jheeta.ca:8080/api/v1/dags/poc_etl_demo/dagRuns/$dag_run_id -H 'content-type: application/json' --user "admin:admin" | jq '.state' | tr -d '"') 
      done
      if [ $status == "success" ];
      then
         mv $path$file $processed${file%%.*}"_"$dag_run_id"."${file#*.}
         echo "File" $path$file "has successfully processed and has been moved" $processed${file%%.*}"_"$dag_run_id"."${file#*.}
      elif [ $status == "failed" ];
      then
         mv $path$file $failed${file%%.*}"_"$dag_run_id"."${file#*.}
         echo "File" $path$file "has failed to process and has been moved" $processed${file%%.*}"_"$dag_run_id"."${file#*.}
      fi
   fi
done