from flask import Flask, request, render_template, jsonify
import requests
import json
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092')


@app.route('/buy', methods=['POST'])
def buy_item():
    data = request.form.to_dict()
    response = requests.post('http://localhost:5000/buy', json=data)
    item_name = request.form['item_name']
    item = {'item_name': item_name, 'purchased': False}
    producer.send('purchased_topic', json.dumps(item).encode('utf-8'))
    if response.status_code == 200:
        message = f'{data["item_name"]} purchased successfully'
        return render_template('success.html', message=message)
    else:
        return jsonify({'error': 'failed to purchase item'}), 500


@app.route('/items', methods=['GET'])
def get_all_items():
    response = requests.get('http://localhost:5000/items')
    items = response.json()
    return render_template('items.html', items=items)


@app.route('/purchased_items', methods=['GET'])
def get_purchased_items():
    response = requests.get('http://localhost:5000/purchased_items')
    purchased_items = response.json()
    return render_template('purchased_items.html', items=purchased_items)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(port=5001, debug=True)
