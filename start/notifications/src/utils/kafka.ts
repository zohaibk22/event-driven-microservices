import { Kafka } from "kafkajs";

const brokers = ["0.0.0.0:9092"];
const topics = ["message-created"] as const;
const kafka = new Kafka({
  brokers,
  clientId: "notifications-service",
});

const consumer = kafka.consumer({
  groupId: "notification-service",
});

const messageCreatedHandler = (data) => {
  console.log("got a new message", JSON.stringify(data, null, 2));
};
const topicsToSubscribe: Record<(typeof topics)[number], Function> = {
  "message-created": messageCreatedHandler,
};

export const connectConsumer = async () => {
  await consumer.connect();
  console.log("connected to consumer");

  for (let i = 0; i < topics.length; i++) {
    await consumer.subscribe({
      topic: topics[i],
      fromBeginning: true,
    });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message || !message.value) {
        return;
      }
      const data = JSON.parse(message.value.toString());

      const handler = topicsToSubscribe[topic];
      if (handler) {
        handler(data);
      }
    },
  });
};

export const disconnectConsumer = async () => {
  await consumer.disconnect();
  console.log("disconnected from consumer");
};
