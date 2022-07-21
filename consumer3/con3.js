const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "c_consumer1",
  brokers: ["localhost:9092", "localhost:9093"],
});

// Consuming
const consumingTopic1 = async () => {
  try {

    const consumer3 = kafka.consumer({ groupId: "test-group2" });
    await consumer3.connect();

    await consumer3.subscribe({
      topics: ['animals', 'birds', 'age'],
      fromBeginning: false,
    });
    consumer3.logger().info("Starting consumer3");
    await consumer3.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
        }) => {
            for (let message of batch.messages) {
                console.log({
                    topic: batch.topic,
                    message: {
                        offset: message.offset,
                        // key: message.key.toString(),
                        value: message.value.toString(),
                    }
                })
    
                resolveOffset(message.offset)
                await heartbeat()
            }
        },
    })
  } catch (error) {
    throw new Error(error.message);
  }
};
consumingTopic1();