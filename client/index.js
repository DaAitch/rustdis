const redis = require('redis');
const client = redis.createClient();

client.on("error", function(error) {
  console.error(error);
});

let start = Date.now();
for (let i = 0; i < 10000; i++) {
  client.set(`key${i}`, `value${i}`);
  client.get(`key${i}`, (err, reply) => {
    console.log(reply)
  })
}

client.set(`key`, `value`, () => {
  let diff = Date.now() - start;

  console.log(`Took ${diff}ms`)
});
