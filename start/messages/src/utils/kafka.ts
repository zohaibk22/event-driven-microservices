import { Kafka } from "kafkajs";

const brokers = ["0.0.0.0:9092"];

const kafka = new Kafka({
  clientId: "messages-app",
  brokers,
});

const producer = kafka.producer();

//need to create a function to connect to kafka

export async function connectToProducer() {
  await producer.connect();
  console.log("Producer connected");
  //returns void... nothing returned
}

//need to create a function to disconnect from kafka

export async function disconnectFromProducer() {
  console.log("Producer disconnected");
  return producer.disconnect();
}

const topics = ["message-created"] as const;
export async function sendMessage(
  topic: (typeof topics)[number],
  message: any
) {
  return producer.send({
    topic,
    messages: [
      {
        value: message,
      },
    ],
  });
}
