const { kafka } = require('./client');
 
const group = process.argv[2];
 
async function init() {
  const consumer = kafka.consumer({ groupId: group });
 
  console.log('consumer connecting...');
  await consumer.connect();
  console.log('consumer connected!');
 
  console.log('consumer subscribing...');
  await consumer.subscribe({ topics: ['rider-updates'], fromBeginning: true });
  console.log('consumer subscribed');
 
  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `${group}: [${topic}]: PART:[${partition}]:`,
        message.value.toString()
      );
    },
  });
}
 
init();
