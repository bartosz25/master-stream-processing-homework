#!/bin/bash
kafka-topics.sh --create  --bootstrap-server localhost:9092 --topic raw_data --partitions 5 --replication-factor 1  

echo ">> TOPICS (raw_data) were correctly created" 
exit;
