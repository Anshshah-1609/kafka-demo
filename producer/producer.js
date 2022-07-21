const { Kafka, Partitioners } = require("kafkajs");
const Chance = require('chance')
const chance = new Chance();

const kafka = new Kafka({
  clientId: "c_producer",
  brokers: ["localhost:9092", "localhost:9093",  "localhost:9094"],
});
const producer = kafka.producer();

const produceAnimalMessage = async () => {
  const value = chance.animal();
  
  console.log(value);
  try {
    await producer.send({
        topic: 'animals',
        messages: [
          { 
            key: 'name',
            value
          }
        ]
      })
  } catch (error) {
    console.log({ message: error.message });
  }
}

const produceAgeMessage = async () => {
  const value = chance.age();

  console.log(value);
  try {
    await producer.send({
      topic: 'age',
      messages: [
        {
          key: 'description',
          value: value.toString()
        }
      ]
    });
  } catch (error) {
    console.log({ message: error.message });
  }
}

const producing = async () => {
  // Producing
  console.log("----->  Producer...");
    await producer.connect();
    setInterval(async () => {
      produceAnimalMessage();
      produceAgeMessage();
    }, 10000);

    // admin
    const admin = kafka.admin()
    await admin.connect();

    const topics = await admin.listTopics();
    const groups = await admin.listGroups();
    console.log(topics);
    console.log(groups);

    await admin.disconnect();

  }
producing().catch(console.error);
