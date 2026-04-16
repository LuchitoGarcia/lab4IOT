import time
import json
import traceback
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    IoTCoreMessage,
    QOS,
    PublishToIoTCoreRequest,
    SubscribeToIoTCoreRequest
)

SUBSCRIBE_TOPIC = "cars/emission"
TIMEOUT = 10

ipc_client = awsiot.greengrasscoreipc.connect()

# Almacena el CO2 maximo por dispositivo
co2_max_per_device = {}


class StreamHandler(client.SubscribeToIoTCoreStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: IoTCoreMessage) -> None:
        try:
            payload   = json.loads(event.message.payload.decode("utf-8"))
            device_id = payload.get("device_id", "unknown")
            co2_value = float(payload.get("vehicle_CO2", 0.0))

            # Calcular maximo CO2 para este dispositivo
            prev_max = co2_max_per_device.get(device_id, 0.0)
            new_max  = max(prev_max, co2_value)
            co2_max_per_device[device_id] = new_max

            print(f"[RECEIVED] {device_id} | CO2: {co2_value:.2f} | Max: {new_max:.2f}")

            # Publicar resultado SOLO al topic de ese dispositivo (privacidad)
            result_topic = f"cars/result/{device_id}"
            result = json.dumps({
                "device_id":   device_id,
                "current_CO2": co2_value,
                "max_CO2":     new_max,
                "timestamp":   time.time()
            })

            pub_request = PublishToIoTCoreRequest(
                topic_name=result_topic,
                qos=QOS.AT_LEAST_ONCE,
                payload=result.encode("utf-8")
            )
            ipc_client.new_publish_to_iot_core().activate(pub_request).result(timeout=TIMEOUT)
            print(f"[PUBLISHED] resultado -> {result_topic}")

        except Exception:
            traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        print(f"[ERROR] Stream error: {error}")
        return True

    def on_stream_closed(self) -> None:
        print("[INFO] Stream cerrado")


def main():
    print("Iniciando process_emission.py...")
    request = SubscribeToIoTCoreRequest(
        topic_name=SUBSCRIBE_TOPIC,
        qos=QOS.AT_LEAST_ONCE
    )
    handler   = StreamHandler()
    operation = ipc_client.new_subscribe_to_iot_core(handler)
    operation.activate(request).result(timeout=TIMEOUT)
    print(f"Suscrito a topic: {SUBSCRIBE_TOPIC}")

    while True:
        time.sleep(5)


if __name__ == "__main__":
    main()
