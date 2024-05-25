const { kafka } = require('./client');
 
async function init() {
  const admin = kafka.admin();
  console.log('admin connecting...');
  await admin.connect();
  console.log('admin connected!');
 
  console.log('topics creating...');
  await admin.createTopics({
    topics: [
      {
        topic: 'rider-updates',
        numPartitions: 2,
      },
    ],
  });
  console.log('topics created! [rider-updates]');
 
  console.log('admin disconnecting...');
  await admin.disconnect();
  console.log('admin disconnected!');
}
 
init();