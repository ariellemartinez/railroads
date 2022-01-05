#!/bin/bash
cd /www/scripts/railroads
source env/bin/activate

for f in tables/*; do
#   echo "File -> $f"
  echo $(echo $f | sed -e "s/^csv\///" -e 's/\.csv$//'); 
  sqlite-utils insert Transportation.db $(echo $f | sed -e "s/^csv\///" -e 's/\.csv$//') $f --csv
  sqlite-utils drop-table /www/datasette-environment/'Transportation.db' $(echo $f | sed -e "s/^csv\///" -e 's/\.csv$//') --ignore
  sqlite-utils insert /www/datasette-environment/'Transportation.db' $(echo $f | sed -e "s/^csv\///" -e 's/\.csv$//') $f --csv
done
