const redis = require("redis");
const client = redis.createClient();

client.on("error", function(error) {
  console.error(error);
});

let start = Date.now();
for (let i = 0; i < 100000; i++) {
  client.set(`key${i}`, `value${i}`);
}

client.set(`key`, `value`, () => {
  let diff = Date.now() - start;

  console.log(`Took ${diff}ms`)
});
