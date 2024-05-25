const { kafka } = require('./client');
const readline = require('readline');
 
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});
 
async function init() {
  const producer = kafka.producer();
 
  console.log('producer connecting...');
  await producer.connect();
  console.log('producer connected!');
 
  rl.setPrompt('> ');
  rl.prompt();
 
  rl.on('line', async function (line) {
    const [riderName, riderLocation] = line.split(' ');
 
    console.log('sending messages...');
    await producer.send({
      topic: 'rider-updates',
      messages: [
        {
          partition: riderLocation.toLowerCase() == 'north' ? 0 : 1,
          key: 'location-update',
          value: JSON.stringify({ name: riderName, location: riderLocation }),
        },
      ],
    });
    console.log('messages sent!');
  }).on('close', async () => {
    console.log('producer disconnecting...');
    await producer.disconnect();
    console.log('producer disconnected!');
  });
}
 
init();
