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

config = {}     # Ucitani JSON fajl uredjaja
sock   = None   # UDP socket
mqtt_client = None  # MQTT client


#SSDP funkcije
def ucitaj_config(putanja_fajla):
    """Ucitava JSON fajl uredjaja i vraca ga kao Python recnik."""
    with open(putanja_fajla, "r", encoding="utf-8") as f:
        return json.load(f)


def uzmi_grupu():
    """Vraca grupu uredjaja iz ucitane konfiguracije."""
    return config['opis_uredjaja']['grupa']


def uzmi_id():
    """Vraca ID uredjaja iz ucitane konfiguracije."""
    return config["opis_uredjaja"]["identifikacija"]["id"]


def uzmi_tip():
    """Vraca tip uredjaja (UPnP URN string) iz konfiguracije."""
    return config["opis_uredjaja"]["identifikacija"]["tip"]


def uzmi_port():
    """Vraca port na kom ovaj uredjaj slusa unicast poruke."""
    return config["opis_uredjaja"]["identifikacija"]["port"]


def uzmi_stanje():
    """Vraca trenutno stanje uredjaja kao JSON string."""
    return json.dumps(config["stanje_uredjaja"])


def posalji_ssdp(nts_vrednost):
    """
    Salje SSDP NOTIFY poruku na multicast adresu.

    Parametar nts_vrednost moze biti:
        "ssdp:alive"  — uredjaj se oglasava da je aktivan
        "ssdp:byebye" — uredjaj se odjavljuje pre gasenja

    Poruka sada sadrzi i PORT liniju!
    Kontroler ce procitati ovaj port i koristiti ga
    kada bude slao STATUS upite ovom uredjaju.
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
    Poziva se kad stigne UDP poruka od kontrolera.

    Kontroler moze poslati:
      STATUS <device_id> — trazi trenutno stanje ovog uredjaja

    Uredjaj odgovara sa:
      STATUS-REPLY USN:<id> STATE:<json_stanje>

    Odgovaramo na adresu sa koje je stigao upit (adresa_posiljaoca).
    To je adresa socket-a kontrolera koji je poslao STATUS.
    Kontroler ce primiti odgovor na istom socket-u na koji je poslao upit.
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
    """
    Beskonacna petlja koja ceka UDP poruke od kontrolera.
    Pokrece se u posebnoj niti da ne blokira glavni program.
    Uredjaj slusa SAMO na SVOM portu.
    Zbog toga vise uredjaja moze istovremeno da radi bez konflikta.
    """
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
        uredjaj_id = uzmi_id()

        # Izvlacimo ime uredjaja iz id-a
        delovi = uredjaj_id.split("-")
        ime_uredjaja = "nepoznato"
        if len(delovi) >= 2:
            ime_uredjaja = delovi[1]

        if podgrupa == "Aktuator":
            tema_set = f"kupatilo/{ime_uredjaja}/set"
            client.subscribe(tema_set)
            print(f"[MQTT] Pretplacen na temu: {tema_set}")
    else:
        print(f"[MQTT] Neuspesna konekcija, kod: {rc}")


def on_message(client, userdata, msg):
    tema = msg.topic
    poruka = msg.payload.decode("utf-8")
    print(f"[MQTT] Tema: {tema} | Poruka: {poruka}")

    # Samo aktuatori vrse promenu stanja
    if poruka.upper() in ["ON", "OFF"]:
        config["stanje_uredjaja"]["status"] = poruka.lower()
        print(f"[ACTION] Novo stanje je: {poruka.lower()}")


def publisher():
    # Posebna funkcija za niti koje ce predstavljati senzore
    podgrupa = config["opis_uredjaja"]["podgrupa"]
    uredjaj_id = uzmi_id()

    delovi = uredjaj_id.split("-")
    ime_uredjaja = "nepoznato"
    if len(delovi) >= 2:
        ime_uredjaja = delovi[1]

    tema_stanje = f"kupatilo/{ime_uredjaja}/stanje"

    while True:
        if podgrupa == "Senzor":
            # Simulacija promene vlage
            if "vlaznost_vazduha" in config["stanje_uredjaja"]:
                trenutna = config["stanje_uredjaja"]["vlaznost_vazduha"]
                nova = trenutna + random.randint(-5, 5)
                config["stanje_uredjaja"]["vlaznost_vazduha"] = max(0, min(nova, 100))
            elif "pokret_detektovan" in config["stanje_uredjaja"]:
                trenutna = config["stanje_uredjaja"]["pokret_detektovan"]
                nova = not(trenutna)
                config["stanje_uredjaja"]["pokret_detektovan"] = nova
            elif "temperatura_vode" in config["stanje_uredjaja"]:
                trenutna = config["stanje_uredjaja"]["temperatura_vode"]
                nova = trenutna + random.randint(-5, 5)
                config["stanje_uredjaja"]["temperatura_vode"] = max(0, min(nova, 100))

            stanje_str = json.dumps(config["stanje_uredjaja"])
            mqtt_client.publish(tema_stanje, stanje_str)
            print(f"[MQTT] Objavljujem na {tema_stanje}: {stanje_str}")
        time.sleep(5)


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

    """
    Vezujemo socket za SOPSTVENI port (ocitan iz JSON-a).
    Zahvaljujuci ovome, svaki uredjaj slusa na drugom portu.
    """
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
    nit_senzor = threading.Thread(target=publisher, daemon=True)
    nit_senzor.start()

    # Glavni thread ceka dok program ne bude ugasen (CTRL+C)
    print(f"[RUN] Uredjaj {uzmi_id()} je aktivan. Pritisnite CTRL+C za gasenje.")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
