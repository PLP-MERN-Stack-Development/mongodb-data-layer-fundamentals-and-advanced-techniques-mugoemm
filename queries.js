// queries.js - MongoDB assignment queries using Node.js

const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB server');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    console.log('\n📖 All Fiction books:');
    await collection.find({ genre: 'Fiction' }).forEach(book => console.log(book));

    console.log('\n📖 Books published after 1950:');
    await collection.find({ published_year: { $gt: 1950 } }).forEach(book => console.log(book));

    console.log('\n📖 In-stock books:');
    await collection.find({ in_stock: true }).forEach(book => console.log(book));

    console.log('\n📖 Books by George Orwell:');
    await collection.find({ author: 'George Orwell' }).forEach(book => console.log(book));

    console.log('\n📖 In-stock books (title, author, price):');
    await collection.find({ in_stock: true }, { projection: { title: 1, author: 1, price: 1, _id: 0 } })
      .forEach(book => console.log(book));

    console.log('\n💰 Update price of "1984" to 12.99');
    await collection.updateOne({ title: '1984' }, { $set: { price: 12.99 } });

    console.log('\n📖 Books sorted by price ascending:');
    await collection.find().sort({ price: 1 }).forEach(book => console.log(book));

    console.log('\n📖 Books sorted by price descending:');
    await collection.find().sort({ price: -1 }).forEach(book => console.log(book));

    console.log('\n📊 Average price by genre:');
    await collection.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' } } }
    ]).forEach(result => console.log(result));

    console.log('\n📊 Author with most books:');
    await collection.aggregate([
      { $group: { _id: '$author', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).forEach(result => console.log(result));

    console.log('\n📊 Books grouped by decade:');
    await collection.aggregate([
      {
        $group: {
          _id: { $subtract: ['$published_year', { $mod: ['$published_year', 10] }] },
          count: { $sum: 1 }
        }
      }
    ]).forEach(result => console.log(result));

    console.log('\n🏷 Creating index on title...');
    await collection.createIndex({ title: 1 });

    console.log('🏷 Creating compound index on author + published_year...');
    await collection.createIndex({ author: 1, published_year: 1 });

    console.log('\n🔍 Explain query for "1984":');
    const explain = await collection.find({ title: '1984' }).explain('executionStats');
    console.log(explain);

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
    console.log('\nConnection closed');
  }
}

runQueries();
