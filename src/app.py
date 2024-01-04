from flask import Flask, request, jsonify, send_from_directory
import json
import shutil
import time
import os
from dotenv import load_dotenv
from threading import Thread
from modbus_client import service_read as modbus_read
from update_config import update_now as update_config_now

load_dotenv()

app = Flask(__name__)
Thread(target=update_config_now).start()


stop_thread = False
thread = None

def background_task():
    global stop_thread
    while not stop_thread:
        modbus_read()
        interval = (float(os.getenv('INTERVAL'))/1000)
        #app.logger.info(f"interval:{interval} s")
        time.sleep(interval)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def readfile(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def updatefile(source_file, destination_file):
    try:
        shutil.copyfile(source_file, destination_file)
        os.remove(source_file)
    except Exception as e:
        print(f"Error: {e}")


@app.route('/api/modbus/start', methods=['POST'])
def start_thread():
    global stop_thread, thread
    if thread is None or not thread.is_alive() or stop_thread:
        stop_thread = False
        thread = Thread(target=background_task)
        thread.start()
        return jsonify({"message": "Starting the service..."}), 200
    else:
        return jsonify({"message": "Service is already running."}), 400

@app.route('/api/modbus/stop', methods=['POST'])
def stop_thread():
    global stop_thread
    if thread and thread.is_alive() and not stop_thread:
        stop_thread = True
        return jsonify({"message": "Stopping the service..."}), 200
    else:
        return jsonify({"message": "Service is not running."}), 400

@app.route('/api/modbus/status')
def status_thread():
    global stop_thread
    if thread and thread.is_alive() and not stop_thread:
        return jsonify({"message": "The service is run"}), 200
    else:
        return jsonify({"message": "The service is stop"}), 200

@app.route('/')
def hello_geek():
    return '<h1>Hello from Agen Service</h2>'

@app.route('/api/config/update', methods=['POST'])
def update_config():
    destination_file = f'{os.getcwd()}/modbus_meta.json'
    source_file=  f'{os.getcwd()}/modbus_meta_release.json'
    try:
        shutil.copyfile(source_file, destination_file)
        os.remove(source_file)
        return jsonify({"message": "Successfully updated config file."}), 200
    except Exception as e:
        return jsonify({"message":{e}}), 400    
    
@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return 'No file part'

    file = request.files['file']

    if file.filename == '':
        return 'No selected file'

    if file and file.filename.endswith('.json'):
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)

        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

        return jsonify({'filename': file.filename, 'json_content': json_data})
    else:
        return 'Invalid file format. Please upload a JSON file.'

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

# Run the app
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=4321)
