import { TetherClient } from "tether-ts";
import { createInterface } from "node:readline/promises";

const client = new TetherClient();
client.connect("ws://localhost:8080/tether");

client.subscribe("getMessages", { room: "1" }, (messages) => {
	console.log("Received messages", messages);
});

const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
});

while (true) {
  const message = await rl.question("Enter a message");
  if (message) {
    const result = await client.sendMutation("createMessage", { room: "1", message: message });
    console.log("Mutation result", result);
  }
}