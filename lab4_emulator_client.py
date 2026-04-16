from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time, json, csv

ENDPOINT    = "a1beiz43zcgzks-ats.iot.us-east-2.amazonaws.com"
ROOT_CA     = "/Users/luisgarciaalvarez/Documents/URBANA/iot/lab4/certs/AmazonRootCA1.pem"
CERTS_DIR   = "/Users/luisgarciaalvarez/Documents/URBANA/iot/lab4/certs"
DATA_FOLDER = "/Users/luisgarciaalvarez/Documents/URBANA/iot/lab4/vehicle_data"
NUM_DEVICES = 5
TOPIC_PUB   = "cars/emission"
TOPIC_SUB   = "cars/result/Car_{:04d}"

clients   = []
data_rows = {}

def create_client(device_id):
    cert_path = f"{CERTS_DIR}/Car_{device_id:04d}"
    client = AWSIoTMQTTClient(f"Car_{device_id:04d}")
    client.configureEndpoint(ENDPOINT, 8883)
    client.configureCredentials(
        ROOT_CA,
        f"{cert_path}/private.key",
        f"{cert_path}/cert.pem"
    )
    client.configureAutoReconnectBackoffTime(1, 32, 20)
    client.configureOfflinePublishQueueing(-1)
    client.configureDrainingFrequency(2)
    client.configureConnectDisconnectTimeout(10)
    client.configureMQTTOperationTimeout(5)
    return client

def load_vehicle_data(device_id):
    rows = []
    path = f"{DATA_FOLDER}/vehicle{device_id}.csv"
    try:
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        print(f"  Cargadas {len(rows)} filas de {path}")
    except FileNotFoundError:
        print(f"  [ERROR] No se encontró {path}")
    return rows

def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode("utf-8"))
    print(f"[RESULTADO] Dispositivo {userdata}: {payload}")

def send_one_message(device_id, row_index):
    client = clients[device_id]
    rows = data_rows.get(device_id, [])
    if not rows:
        return
    row = rows[row_index % len(rows)]
    payload = json.dumps({
        "device_id":   f"Car_{device_id:04d}",
        "timestamp":   time.time(),
        "vehicle_CO2": float(row.get("CO2", 0)),
        "data":        row
    })
    client.publish(TOPIC_PUB, payload, 1)
    print(f"[SEND] Car_{device_id:04d} → CO2: {row.get('CO2', '?')}")

def main():
    global clients, data_rows
    print("Iniciando emulador...")

    for i in range(NUM_DEVICES):
        print(f"Conectando Car_{i:04d}...")
        c = create_client(i)
        c.connect()

        did = i  # captura correcta para el lambda
        result_topic = TOPIC_SUB.format(i)
        c.subscribeAsync(
            result_topic, 1,
            ackCallback=None,
            messageCallback=lambda c, u, m, d=did: on_message(c, d, m)
        )

        clients.append(c)
        data_rows[i] = load_vehicle_data(i)
        print(f"Conectado: Car_{i:04d} | suscrito a: {result_topic}")
        time.sleep(0.5)

    print("\n¡Listo! Presiona 's' + Enter para enviar un mensaje de cada carro.")
    print("Presiona 'q' + Enter para salir.\n")

    row_index = 0
    while True:
        cmd = input()
        if cmd == "s":
            for i in range(NUM_DEVICES):
                send_one_message(i, row_index)
            row_index += 1
        elif cmd == "q":
            break

    for c in clients:
        c.disconnect()
    print("Desconectado.")

if __name__ == "__main__":
    main()