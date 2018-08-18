const mongodb= require('mongodb')
const url = 'mongodb://localhost:27017'
const customers = require('./customer-data.json')
const customerAddresses = require('./customer-address-data.json')
const async = require('async')


let tasks = []
const recLimit = parseInt(process.argv[2], 10)
mongodb.MongoClient.connect(url,{ useNewUrlParser: true }, (error, db) => {
    const dbo = db.db('edx-course-db');
  if (error) return process.exit(1)

  //Read cutomer file
  customers.forEach((customer, index) => {

    customers[index] = Object.assign(customer, customerAddresses[index])
    

    if (index % recLimit == 0) {
      const startIdx = index
      const endIdx = (startIdx+recLimit > customers.length) ? customers.length-1 : startIdx+recLimit
      tasks.push((done) => {
        console.log(`Processing ${startIdx}-${endIdx} out of ${customers.length}`)
        dbo.collection('customers').insertMany(customers.slice(startIdx, endIdx), (error, results) => {
          done(error, results)
        })
      })
    } 
  })
  console.log(`Launching ${tasks.length} parallel task(s)`)
  const startTime = Date.now()
  async.parallel(tasks, (error, results) => {
    if (error) console.error(error)
    const endTime = Date.now()
    console.log(`Execution time: ${endTime-startTime}`)    
    db.close()
  })
})