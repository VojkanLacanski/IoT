import socket
import json
import sys
import signal
import threading
import time
import random
import paho.mqtt.client as mqtt

MULTICAST_ADDR = "239.255.255.250"   # SSDP standardna multicast adresa
MULTICAST_PORT = 1900                # SSDP standardni port
MQTT_BROKER = "127.0.0.1"            # Adresa MQTT brokera
MQTT_PORT = 1883                     # Port MQTT brokera
TEMA_STANJA_AKTUATORA = "kupatilo/aktuatori/stanje"

INTERVAL_OBJAVE_SEC = 5
INTERVAL_OBJAVE_AKTUATORA_SEC = 5

TEMP_MIN = 5
TEMP_MAX = 60

VLAGA_MIN = 0
VLAGA_MAX = 100

stanja_aktuatora = {
    "bojler": "off",
    "ventilator": "off",
    "svetlo": "off"
}

state_lock = threading.Lock()

config = {}     # Ucitani JSON fajl uredjaja
sock   = None   # UDP socket
mqtt_client = None  # MQTT client


# SSDP funkcije
def ucitaj_config(putanja_fajla):
    # Ucitava JSON fajl uredjaja i vraca ga kao Python recnik
    with open(putanja_fajla, "r", encoding="utf-8") as f:
        return json.load(f)


def uzmi_grupu():
    # Vraca grupu uredjaja iz ucitane konfiguracije
    return config['opis_uredjaja']['grupa']


def uzmi_id():
    # Vraca ID uredjaja iz ucitane konfiguracije
    return config["opis_uredjaja"]["identifikacija"]["id"]


def uzmi_tip():
    # Vraca tip uredjaja (UPnP URN string) iz konfiguracije
    return config["opis_uredjaja"]["identifikacija"]["tip"]


def uzmi_port():
    # Vraca port na kom ovaj uredjaj slusa unicast poruke
    return config["opis_uredjaja"]["identifikacija"]["port"]


def uzmi_ime_uredjaja():
    """
    Iz ID-ja tipa id:kupatilo-bojler-senzor
    izvlaci ime uredjaja: bojler, ventilator ili svetlo
    """
    delovi = uzmi_id().split("-")

    if len(delovi) >= 2:
        return delovi[1]

    return "nepoznato"


def uzmi_stanje():
    # Vraca trenutno stanje uredjaja kao JSON string
    return json.dumps(config["stanje_uredjaja"])


def posalji_ssdp(nts_vrednost):
    """
    Salje SSDP NOTIFY poruku na multicast adresu

    Parametar nts_vrednost moze biti:
        "ssdp:alive"  — uredjaj se oglasava da je aktivan
        "ssdp:byebye" — uredjaj se odjavljuje pre gasenja
    """
    poruka = (
        "NOTIFY * HTTP/1.1\r\n"
        f"HOST: {MULTICAST_ADDR}:{MULTICAST_PORT}\r\n"
        f"NT: {uzmi_tip()}\r\n"
        f"NTS: {nts_vrednost}\r\n"
        f"USN: {uzmi_id()}\r\n"
        f"GRUPA: {uzmi_grupu()}\r\n"
        f"PORT: {uzmi_port()}\r\n"
        "\r\n"
    )

    odrediste = (MULTICAST_ADDR, MULTICAST_PORT)

    sock.sendto(poruka.encode(), odrediste)
    print(f"[SSDP] Poruka poslata: {nts_vrednost} | ID: {uzmi_id()} | Port: {uzmi_port()}")


def obradi_poruku(poruka_bytes, adresa_posiljaoca):
    """
    Poziva se kad stigne UDP poruka od kontrolera

    Kontroler moze poslati:
      STATUS <device_id> — trazi trenutno stanje ovog uredjaja

    Uredjaj odgovara sa:
      STATUS-REPLY USN:<id> STATE:<json_stanje>
    """
    tekst = poruka_bytes.decode(errors="ignore")

    if "STATUS" in tekst:
        # Kontroler trazi stanje uredjaja — saljemo trenutno stanje kao JSON
        odgovor = f"STATUS-REPLY USN:{uzmi_id()} STATE:{uzmi_stanje()}"
        sock.sendto(odgovor.encode(), adresa_posiljaoca)
        print(f"[STATUS] Odgovorili smo kontroleru {adresa_posiljaoca}: {uzmi_stanje()}")

    else:
        print(f"[UNKNOWN] Nepoznata poruka od {adresa_posiljaoca}: {tekst[:60]}")


# Petlja slusanja
def slusaj():
    # Beskonacna petlja koja ceka UDP poruke od kontrolera
    print(f"[LISTEN] Uredjaj {uzmi_id()} slusa na portu {uzmi_port()}...")
    while True:
        try:
            # Cekamo poruku (blokira nit dok ne stigne nesto)
            podaci, adresa = sock.recvfrom(4096)
            obradi_poruku(podaci, adresa)
        except OSError:
            # Socket je zatvoren (desava se pri gasenju) — izlazimo iz petlje
            break


# Obrada CTRL+C signala
def prekid(sig, frame):
    """
    Poziva se kad korisnik pritisne CTRL+C.
    Salje BYEBYE, zatvara socket i gasi program.
    """
    print(f"\n[EXIT] Gasim uredjaj: {uzmi_id()}")
    try:
        posalji_ssdp("ssdp:byebye")
    except Exception:
        pass
    sock.close()
    sys.exit(0)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Uredjaj povezan na MQTT broker")

        # Informacija da li je uredjaj AKTUATOR ili SENZOR
        podgrupa = config["opis_uredjaja"]["podgrupa"]
        ime_uredjaja = uzmi_ime_uredjaja()

        if podgrupa == "Aktuator":
            tema_set = f"kupatilo/{ime_uredjaja}/set"
            client.subscribe(tema_set, qos=1)
            print(f"[MQTT] Pretplacen na temu: {tema_set}")
            objavi_stanje_aktuatora(client)

            pocetno_stanje = config["stanje_uredjaja"]["status"].upper()
            client.publish(tema_set, pocetno_stanje, qos=1)
    else:
        print(f"[MQTT] Neuspesna konekcija, kod: {rc}")


def on_message(client, userdata, msg):
    tema = msg.topic
    poruka = msg.payload.decode("utf-8").upper()
    print(f"[MQTT] Tema: {tema} | Poruka: {poruka}")

    podgrupa = config["opis_uredjaja"]["podgrupa"]

    if podgrupa != "Aktuator":
        return

    if poruka not in ["ON", "OFF"]:
        return
    
    ime_uredjaja = uzmi_ime_uredjaja()

    config["stanje_uredjaja"]["status"] = poruka.lower()

    with state_lock:
        stanja_aktuatora[ime_uredjaja] = poruka.lower()

    print(f"[ACTION] Novo stanje je: {poruka.lower()}")

    objavi_stanje_aktuatora(client)


def objavi_stanje_aktuatora(client=None):
    if config["opis_uredjaja"]["podgrupa"] != "Aktuator":
        return

    klijent = client if client is not None else mqtt_client

    if klijent is None:
        return

    ime_uredjaja = uzmi_ime_uredjaja()
    status = config["stanje_uredjaja"].get("status", "off")

    poruka = json.dumps({ime_uredjaja: status})

    klijent.publish(TEMA_STANJA_AKTUATORA, poruka, qos=1)


def periodicno_objavljuj_stanje_aktuatora():
    while True:
        if config["opis_uredjaja"]["podgrupa"] == "Aktuator":
            objavi_stanje_aktuatora()

        time.sleep(INTERVAL_OBJAVE_AKTUATORA_SEC)


def on_connect_stanja_aktuatora(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(TEMA_STANJA_AKTUATORA, qos=1)
    else:
        print(f"[MQTT] Neuspesna konekcija za slusanje aktuatora, kod: {rc}")


def on_message_stanja_aktuatora(client, userdata, msg):
    poruka = msg.payload.decode("utf-8")

    try:
        podaci = json.loads(poruka)

        with state_lock:
            for uredjaj in ["bojler", "ventilator", "svetlo"]:
                if uredjaj in podaci:
                    vrednost = str(podaci[uredjaj]).lower()

                    if vrednost in ["on", "off"]:
                        stanja_aktuatora[uredjaj] = vrednost

    except json.JSONDecodeError:
        print("[FAILED] Neispravan JSON na temi stanja aktuatora")


def slusaj_stanja_aktuatora():
    client_id = f"{uzmi_id()}-slusac-aktuatora"

    client = mqtt.Client(client_id=client_id)
    client.on_connect = on_connect_stanja_aktuatora
    client.on_message = on_message_stanja_aktuatora

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


#-----POMOCNE FUNKCIJE ZA SIMULACIJU-----
def ogranicenje(vrednost, minimum, maksimum):
    return max(minimum, min(vrednost, maksimum))


def simuliraj_temperaturu():
    trenutna = config["stanje_uredjaja"]["temperatura_vode"]

    if stanja_aktuatora["bojler"] == "on":
        nova = trenutna + random.randint(1, 5)
    else:
        nova = trenutna - random.randint(1, 5)

    config["stanje_uredjaja"]["temperatura_vode"] = ogranicenje(nova, TEMP_MIN, TEMP_MAX)


def simuliraj_vlaznost():
    trenutna = config["stanje_uredjaja"]["vlaznost_vazduha"]

    if stanja_aktuatora["ventilator"] == "on":
        nova = trenutna - random.randint(1, 5)
    else:
        nova = trenutna + random.randint(1, 5)

    config["stanje_uredjaja"]["vlaznost_vazduha"] = ogranicenje(nova, VLAGA_MIN, VLAGA_MAX)


def simuliraj_pokret():
    detektovan = random.random() < 0.4

    config["stanje_uredjaja"]["pokret_detektovan"] = detektovan


def publisher():
    # Posebna funkcija za niti koje ce predstavljati senzore
    podgrupa = config["opis_uredjaja"]["podgrupa"]
    ime_uredjaja = uzmi_ime_uredjaja()

    tema_stanje = f"kupatilo/{ime_uredjaja}/stanje"

    while True:
        if podgrupa == "Senzor":
            with state_lock:
                if "vlaznost_vazduha" in config["stanje_uredjaja"]:
                    simuliraj_vlaznost()
                    qos = 0

                elif "pokret_detektovan" in config["stanje_uredjaja"]:
                    simuliraj_pokret()
                    qos = 1

                elif "temperatura_vode" in config["stanje_uredjaja"]:
                    simuliraj_temperaturu()
                    qos = 1

                stanje_str = json.dumps(config["stanje_uredjaja"])

            mqtt_client.publish(tema_stanje, stanje_str, qos=qos)
            print(f"[MQTT] Objavljujem na {tema_stanje}: {stanje_str}")

        time.sleep(INTERVAL_OBJAVE_SEC)


# Main
def main():
    global config, sock, mqtt_client

    # Proveravamo da li je korisnik dao ime JSON fajla kao argument
    if len(sys.argv) != 2:
        print("Primer poziva:   python uredjaj.py <ime_uredjaja>.json")
        sys.exit(1)

    # Ucitavamo konfiguraciju iz JSON fajla
    config = ucitaj_config(sys.argv[1])
    print(f"[INIT] Ucitan uredjaj: {uzmi_id()} ({uzmi_tip()})")
    print(f"[INIT] Koristim port: {uzmi_port()}")

    # Kreiramo UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Postavljamo TTL za multicast (2 = lokalna mreza)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    sock.bind(("", uzmi_port()))

    # Registrujemo handler za CTRL+C
    signal.signal(signal.SIGINT, prekid)

    # Oglasavamo se na mrezi
    posalji_ssdp("ssdp:alive")

    # Pokrecemo slusanje u pozadinskoj niti (da main thread ne bude blokiran)
    nit = threading.Thread(target=slusaj, daemon=True)
    nit.start()

    # Inicijalizacija MQTT modula na strani uredjaja
    mqtt_client = mqtt.Client(client_id=uzmi_id())
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Senzorska nit
    podgrupa = config["opis_uredjaja"]["podgrupa"]

    if podgrupa == "Senzor":
        nit_slusac_aktuatora = threading.Thread(target=slusaj_stanja_aktuatora, daemon=True)
        nit_slusac_aktuatora.start()

        nit_senzor = threading.Thread(target=publisher, daemon=True)
        nit_senzor.start()

    elif podgrupa == "Aktuator":
        nit_aktuator = threading.Thread(target=periodicno_objavljuj_stanje_aktuatora, daemon=True)
        nit_aktuator.start()

    # Glavni thread ceka dok program ne bude ugasen (CTRL+C)
    print(f"[RUN] Uredjaj {uzmi_id()} je aktivan. Pritisnite CTRL+C za gasenje.")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
