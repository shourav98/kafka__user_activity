from flask import Flask, request, jsonify, session
from kafka import KafkaProducer
import json
from datetime import datetime

# Initialize Kafka Producers
producer_project1 = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(2, 7, 0),
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer_project2 = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(2, 7, 0),
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def create_app(producer):
    app = Flask(__name__)
    app.secret_key = "your_secret_key"  # Set a secret key for session management

    @app.route('/')
    def home():
        if 'username' in session:
            return jsonify({'message': f'Logged in as {session["username"]}'})
        return jsonify({'message': 'You are not logged in'})

    @app.route('/register', methods=['POST'])
    def register():
        data = request.get_json()
        if data and 'username' in data and 'password' in data:
            username = data['username']
            ip_address = request.remote_addr  # Capture user's IP address
            # Here, you could add additional validation if required
            session['username'] = username
            # Send user activity message to Kafka
            producer.send('user_activity', {'event_type': 'register', 'username': username, 'ip_address': ip_address, 'timestamp': str(datetime.now())})
            producer.flush()  # Ensure all messages are sent
            return jsonify({'message': 'Registration successful'})
        else:
            return jsonify({'message': 'Username and password are required'}), 400

    @app.route('/login', methods=['POST'])
    def login():
        data = request.get_json()
        if data and 'username' in data and 'password' in data:
            username = data['username']
            ip_address = request.remote_addr  # Capture user's IP address
            # Here, you could add additional validation if required
            session['username'] = username
            # Send user activity message to Kafka
            producer.send('user_activity', {'event_type': 'login', 'username': username, 'ip_address': ip_address, 'timestamp': str(datetime.now())})
            producer.flush()  # Ensure all messages are sent
            return jsonify({'message': 'Login successful'})
        else:
            return jsonify({'message': 'Username and password are required'}), 400

    @app.route('/logout', methods=['POST'])
    def logout():
        if 'username' in session:
            username = session.pop('username')
            ip_address = request.remote_addr  # Capture user's IP address
            # Send user activity message to Kafka
            producer.send('user_activity', {'event_type': 'logout', 'username': username, 'ip_address': ip_address, 'timestamp': str(datetime.now())})
            producer.flush()  # Ensure all messages are sent
            return jsonify({'message': 'Logout successful'})
        else:
            return jsonify({'message': 'You are not logged in'}), 401

    return app

if __name__ == '__main__':
    # Create and run two separate Flask applications for Project 1 and Project 2
    app_project1 = create_app(producer_project1)
    app_project1.run(debug=True, port=5000)

    app_project2 = create_app(producer_project2)
    app_project2.run(debug=True, port=5001)
