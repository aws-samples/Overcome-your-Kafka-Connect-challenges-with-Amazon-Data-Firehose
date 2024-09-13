## Overcome your Kafka Connect challenges with Amazon Data Firehose

This repository contains a python script, get_latest_offsets.py, that will connect to an Amazon MSK cluster using the iam-sasl-signer library to enable IAM authentication for non-Java Kafka clients, in order to scan and output the latest offset positions in the provided consumer-group, per-partition, on the Kafka topic you provide. 

Running the script requires the following parameters be provided in the command line: 
- broker-list : a comma-separated list of representing the bootstrap server strings for your Kafka brokers
- topic-name : name of the topic on the Kafka cluster 
- consumer-group-id : ID of the consumer group you would like to query the latest-offset positions from
- aws-region : AWS region that the MSK cluster resides in

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

