import { disconnect } from "process";
import { connectToDb } from "./utils/db";
import { connectConsumer, disconnectConsumer } from "./utils/kafka";
import { createServer } from "./utils/server";

async function gracefulShutdown(app: Awaited<ReturnType<typeof createServer>>) {
  console.log("Graceful shutdown");

  await app.close();

  await disconnectConsumer();

  process.exit(0);
}

async function main() {
  const app = createServer();

  await connectToDb();

  await connectConsumer();

  await app.listen({
    port: 4000,
    host: "0.0.0.0",
  });

  const signals = ["SIGINT", "SIGTERM", "SIGQUIT"];

  for (let i = 0; i < signals.length; i++) {
    process.on(signals[i], () => gracefulShutdown(app));
  }

  console.log("Notification service ready at http://localhost:4000");
}

main();
