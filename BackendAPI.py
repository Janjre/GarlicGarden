import eventlet

eventlet.monkey_patch()
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
import redis
import json

import time
import random
from datetime import datetime, timedelta
import threading
import psutil

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# make app
app = Flask(
    __name__,
    static_url_path="",
    static_folder="web/static",
    template_folder="web/templates",
)


# Update the SocketIO initialization
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    message_queue="redis://localhost:6379/0",  # Adjust if your Redis is elsewhere
    logger=True,
    engineio_logger=True,
)

##set starting variables
sensor_data = {"moisture": 0}
pump_state = {"pump": False}
max_data_points = 100


registered_streams = []
registered_names = []


class Stream:
    def __init__(self, channel, id, name, colour):
        self.channel = channel
        self.id = id
        self.name = name
        self.colour = colour
        registered_streams.append(self)
        registered_names.append(id)

    def listener(self):
        pubsub = redis_client.pubsub()
        pubsub.subscribe(self.channel)
        for message in pubsub.listen():
            if message["type"] == "message":
                new_point = json.loads(message["data"])
                socketio.emit(self.id, new_point, room=self.id)


# cpu_temp
# cpu_usage
# humidity
# temperature
# moisture

# Stream(
#     channel="cpu_temp_channel", id="cpu_temp", name="CPU temperature", colour="#FF0000"
# )
Stream(channel="cpu_usage_channel", id="cpu_usage", name="CPU usage", colour="#00FF00")
Stream(channel="humidity_channel", id="humidity", name="Humidity", colour="#0000FF")
Stream(
    channel="temperature_channel",
    id="temperature",
    name="Temperature",
    colour="#FFFF00",
)
Stream(channel="moisture_channel", id="moisture", name="Moisture", colour="#00FFFF")


def get_cpu_temperature():
    """Read CPU temperature on Linux systems"""
    try:
        # Try the most common temperature source first
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            temp = float(f.read().strip()) / 1000.0  # Convert millidegrees to degrees
            return temp
    except:
        try:
            # Try using sensors command if available
            import subprocess

            output = subprocess.check_output(["sensors"], universal_newlines=True)
            for line in output.split("\n"):
                if "Core 0" in line and "°C" in line:
                    return float(line.split("+")[1].split("°C")[0].strip())
        except:
            # Return a placeholder if we can't get the temperature
            return None  # Placeholder


def cpu_usage_publisher():
    """Publish CPU usage percentage every 5 seconds"""
    while True:
        try:
            usage = psutil.cpu_percent(interval=1)
            data_point = {"value": usage, "date": time.time()}
            key = "cpu_usage_data"
            stored_data = redis_client.get(key)
            usage_data = json.loads(stored_data) if stored_data else []
            usage_data.append(data_point)
            redis_client.set(key, json.dumps(usage_data))
            redis_client.publish("cpu_usage_channel", json.dumps(data_point))
        except Exception as e:
            print(f"Error publishing CPU usage: {e}")
        time.sleep(4)  # 1s for cpu_percent + 4s = 5s total


def cpu_temp_publisher():
    """Publish CPU temperature every 5 seconds"""
    while True:
        try:
            # Get CPU temperature
            temp = get_cpu_temperature()

            # Create data point
            data_point = {"value": temp, "date": time.time()}

            # Store in Redis
            key = "cpu_temp_data"
            stored_data = redis_client.get(key)
            temp_data = json.loads(stored_data) if stored_data else []
            temp_data.append(data_point)
            redis_client.set(key, json.dumps(temp_data))

            # Publish to channel
            redis_client.publish("cpu_temp_channel", json.dumps(data_point))
        except Exception as e:
            print(f"Error publishing CPU temp: {e}")

        # Wait 5 seconds
        time.sleep(5)


@socketio.on("subscribe")
def handle_subscribe(data):
    stream = data.get("stream")

    if stream in [s.id for s in registered_streams]:
        join_room(stream)
        # Send the last N points immediately from Redis
        key = f"{stream}_data"
        data_json = redis_client.get(key)
        if data_json:
            arr = json.loads(data_json)
            # Send last 100 points (or fewer if not enough)
            emit(stream, arr[-100:])
        else:
            emit(stream, [])


@socketio.on("unsubscribe")
def handle_unsubscribe(data):
    stream = data.get("stream")
    if stream in [s.id for s in registered_streams]:
        leave_room(stream)


# Add a new endpoint to list all available streams
@app.route("/streams", methods=["GET"])
def get_streams():
    # Define available streams with friendly names and colors
    available_streams = []
    for stream in registered_streams:
        available_streams.append(
            {"id": stream.id, "name": stream.name, "colour": stream.colour}
        )
    return jsonify(available_streams)


# publish endpoints for IoT devices
@app.route("/publish/<stream>", methods=["POST"])
def publish_data(stream):
    if stream not in registered_names:
        return jsonify({"error": "Invalid stream"}), 400

    try:
        # Get data from request
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Ensure it has required fields
        if "value" not in data:
            return jsonify({"error": "Missing 'value' field"}), 400

        # Add timestamp if not provided
        if "date" not in data:
            data["date"] = time.time()

        # Store in Redis
        key = f"{stream}_data"
        stored_data = redis_client.get(key)
        stream_data = json.loads(stored_data) if stored_data else []
        stream_data.append(data)
        redis_client.set(key, json.dumps(stream_data))

        # Publish to channel
        redis_client.publish(f"{stream}_channel", json.dumps(data))

        return jsonify({"status": "published"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/data/<stream>", methods=["GET"])
def get_data(stream):
    key = f"{stream}_data"
    data_json = redis_client.get(key)
    if data_json:
        return jsonify(json.loads(data_json))
    else:
        return jsonify([]), 404


# Add a simple simulator endpoint for development
@app.route("/simulate", methods=["GET"])
def simulate_data():
    """Generate fake data for development"""
    import random

    humidity = {"value": random.randint(0, 50), "date": time.time()}
    temperature = {"value": random.randint(50, 100), "date": time.time()}

    # Store and publish humidity
    redis_client.publish("humidity_channel", json.dumps(humidity))
    h_data = redis_client.get("humidity_data")
    h_list = json.loads(h_data) if h_data else []
    h_list.append(humidity)
    redis_client.set("humidity_data", json.dumps(h_list))

    # Store and publish temperature
    redis_client.publish("temperature_channel", json.dumps(temperature))
    t_data = redis_client.get("temperature_data")
    t_list = json.loads(t_data) if t_data else []
    t_list.append(temperature)
    redis_client.set("temperature_data", json.dumps(t_list))

    return jsonify(
        {
            "status": "simulated data sent",
            "humidity": humidity,
            "temperature": temperature,
        }
    )


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/sensor", methods=["POST"])
def receive_sensor():
    data = request.json
    sensor_data["moisture"] = data["moisture"]
    return jsonify({"status": "ok"})


@app.route("/status", methods=["GET"])
def get_status():
    return jsonify(
        {**sensor_data, **pump_state}
    )  # right now unpacking is unnecercery, but if it has more properties later, it will still work


@app.route("/pump", methods=["GET"])
def get_pump():
    return jsonify(pump_state)


@app.route("/pump", methods=["POST"])
def set_pump():
    data = request.json
    pump_state["pump"] = data.get(
        "pump", False
    )  # if data["pump"] = something then it is that thing but if it doesn't exist it is False
    return jsonify({"status": "updated to " + str(pump_state["pump"])})


@app.route("/button", methods=["POST"])
def button():
    data = request.json
    print(data)
    return jsonify({"status", "ok"})


if __name__ == "__main__":
    for stream in registered_streams:
        threading.Thread(target=stream.listener, daemon=True).start()

    # Start CPU temperature publisher
    # threading.Thread(target=cpu_usage_pubsub_listener, daemon=True).start()
    threading.Thread(target=cpu_usage_publisher, daemon=True).start()

    # Run the SocketIO app instead of the Flask app
    socketio.run(app, host="0.0.0.0", port=5555, debug=True)
