const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "c_consumer1",
  brokers: ["localhost:9092", "localhost:9093"],
});

// Consuming
const consumingTopic1 = async () => {
  try {

    const consumer = kafka.consumer({ groupId: "test-group3" });
    await consumer.connect();

    await consumer.subscribe({
      topics: ['animals'],
      fromBeginning: false,
    });
    consumer.logger().info("Starting consumer1");
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          offset: message.offset,
          topic,
          key: message.key.toString(),
          value: message.value.toString(),
        });
      },
    });
  } catch (error) {
    throw new Error(error.message);
  }
};
consumingTopic1();