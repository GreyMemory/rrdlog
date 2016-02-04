rsync -a --progress root@104.131.64.201:/var/lib/vici/log ./
rsync -a --progress root@104.131.64.201:/home/equalit/rrd/http_response_ratio.csv ./csv
rsync -a --progress root@104.131.64.201:/home/equalit/http_response_historical_last_month.csv ./csv
rsync -a --progress root@104.131.64.201:/home/equalit/http_response_historical.csv ./csv

