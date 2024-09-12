import argparse
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from datetime import datetime
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider():
    def __init__(self, aws_region):
        self.aws_region = aws_region

    def token(self):
        try:
            oauth2_token, _ = MSKAuthTokenProvider.generate_auth_token(
                self.aws_region
            )
            return oauth2_token
        except Exception as e:
            print(f"Failed to generate token: {e}")
            return None

def main(broker_list, topic_name, consumer_group_id, aws_region):
        try:

                token_provider = MSKTokenProvider(aws_region)

                # Create a Kafka consumer to fetch partition information
                consumer = KafkaConsumer(
                        bootstrap_servers=broker_list, 
                        group_id=consumer_group_id, 
                        api_version=(2, 6, 0),
                        client_id=socket.gethostname(),
                        security_protocol='SASL_SSL',
                        sasl_mechanism='OAUTHBEARER',
                        sasl_oauth_token_provider=token_provider  # Pass the token callback method
                )

                # Get partition information for the topic
                partitions = consumer.partitions_for_topic(topic_name)

                if partitions is None:
                        print("Topic {} does not exist or has no partitions.".format(topic_name))
                else:
                        for partition in partitions:
                                tp = TopicPartition(topic_name, partition)

                                # Fetch the latest offset for the partition
                                end_offset = consumer.end_offsets([tp])[tp]

                                # Check if the end_offset is valid
                                if end_offset > 0:
                                        #Assign the consumer to the partition and seek to the latest offset - 1
                                        consumer.assign([tp])
                                        consumer.seek(tp, end_offset - 1)

                                        # Poll to get the latest message
                                        records = consumer.poll(timeout_ms=5000)

                                        # Extract the message
                                        partition_records = records.get(tp, [])
                                        if partition_records:
                                                msg = partition_records[0]
                                                timestamp = datetime.fromtimestamp(msg.timestamp / 1000.0)
                                                print("Partition: {}, Latest Offset: {}, Timestamp: {}".format(partition, msg.offset, timestamp))
                                        else:
                                                print("Partition: {}, No messages found at the latest offset.".format(partition))
                                else:
                                         print("Partition: {}, No messages found in the partition.".format(partition))
                # Close the consumer
                consumer.close()

        except KafkaError as e:
                print("Kafka error: {}".format(e))
        except Exception as e:
                print("An error occurred: {}".format(e))

if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Get the latest offset and timestamp for each partition of a Kafka topic.")
        parser.add_argument("--broker-list", required=True, help="Comma-separated list of Kafka brokers")
        parser.add_argument("--topic-name", required=True, help="Name of the Kafka topic")
        parser.add_argument("--consumer-group-id", required=True, help="Consumer group ID")
        parser.add_argument("--aws-region", required=True, help="AWS Region MSK Cluster resides in")

        args = parser.parse_args()

        main(args.broker_list, args.topic_name, args.consumer_group_id, args.aws_region)
