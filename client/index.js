const redis = require("redis");
const client = redis.createClient();

client.on("error", function(error) {
  console.error(error);
});

for (let i = 0; i < 100; i++) {
  client.set("key", `key${i}`, redis.print);
}

