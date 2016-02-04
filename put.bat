rsync -a --progress build root@104.131.64.201:/opt/vici/java
rsync -a --progress logger.sh root@104.131.64.201:/opt/vici/java
ssh root@104.131.64.201 'chmod +x /opt/vici/java/logger.sh'