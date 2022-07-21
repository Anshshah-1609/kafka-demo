const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "c_consumer2",
    brokers: ["localhost:9092"],
});
const consumingTopic2 = async () => {
    try {
  
      const consumer2 = kafka.consumer({ groupId: 'test-group'});
      await consumer2.connect();
  
      await consumer2.subscribe({
        topics: ['animals', 'birds'],
        fromBeginning: false,
      });
      consumer2.logger().info("Starting consumer2");
      await consumer2.run({
        eachMessage: async ({ topic, message }) => {
          console.log({
            offset: message.offset,
            topic,
            // key: message.key.toString(),
            value: message.value.toString()
          })
        }
      })
    } catch (error) {
      throw new Error(error.message);
    }
  } 
  consumingTopic2();
  