# create_things.py
import boto3
import json
import os

REGION = "us-east-2"          # cambia a tu región
POLICY_NAME = "lab4"  # nombre de la policy que creaste
GROUP_NAME  = "Lab4Cars"       # nombre del ThingGroup que crearás

client = boto3.client("iot", region_name=REGION)

# Primero crea el ThingGroup (solo una vez)
def create_group():
    try:
        client.create_thing_group(thingGroupName=GROUP_NAME)
        print(f"Grupo '{GROUP_NAME}' creado")
    except client.exceptions.ResourceAlreadyExistsException:
        print(f"Grupo '{GROUP_NAME}' ya existe")

def create_thing(thing_name):
    # 1. Crear el thing
    client.create_thing(thingName=thing_name)

    # 2. Crear certificado
    cert = client.create_keys_and_certificate(setAsActive=True)
    cert_arn  = cert["certificateArn"]
    cert_id   = cert["certificateId"]

    # 3. Guardar certificados localmente
    os.makedirs(f"certs/{thing_name}", exist_ok=True)
    with open(f"certs/{thing_name}/cert.pem", "w") as f:
        f.write(cert["certificatePem"])
    with open(f"certs/{thing_name}/private.key", "w") as f:
        f.write(cert["keyPair"]["PrivateKey"])

    # 4. Adjuntar policy al certificado
    client.attach_policy(policyName=POLICY_NAME, target=cert_arn)

    # 5. Adjuntar certificado al thing
    client.attach_thing_principal(thingName=thing_name, principal=cert_arn)

    # 6. Añadir al grupo
    client.add_thing_to_thing_group(
        thingGroupName=GROUP_NAME,
        thingName=thing_name
    )

    print(f"Creado: {thing_name} | cert_id: {cert_id[:8]}...")
    return cert_id

if __name__ == "__main__":
    NUM_THINGS = 10  # empieza con 10, luego sube
    create_group()
    for i in range(NUM_THINGS):
        create_thing(f"Car_{i:04d}")
    print("Listo!")