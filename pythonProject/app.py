from flask import Flask, request, jsonify
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer
import json

app = Flask(__name__)
client = MongoClient('localhost', 27017)
db = client.shop
purchases = db.purchases

producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('purchase_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')


@app.route('/buy', methods=['POST'])
def buy_item():
    data = request.json
    producer.send('purchase_topic', json.dumps(data).encode('utf-8'))
    return jsonify({'message': f'{data["item_name"]} purchased successfully'}), 200


@app.route('/purchased_items', methods=['GET'])
def get_purchased_items():
    purchased_items = list(purchases.find({}, {'_id': 0}))
    return jsonify(purchased_items), 200


def consume_messages():
    for message in consumer:
        item = json.loads(message.value.decode('utf-8'))
        purchases.insert_one(item)


if __name__ == '__main__':
    from threading import Thread

    Thread(target=consume_messages).start()
    app.run(port=5000)
