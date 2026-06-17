import socket
import json
import threading
import time
import paho.mqtt.client as mqtt

MULTICAST_ADDR = "239.255.255.250"  # SSDP standardna multicast adresa
MULTICAST_PORT = 1900               # SSDP standradni port
MQTT_BROKER = "127.0.0.1"            # Adresa MQTT brokera
MQTT_PORT = 1883                     # Port MQTT brokera

KONTROLER_STATUS_PORT = 1902

INTERVAL_STATUS_SEC = 5  # Svakih 5 sekundi saljemo STATUS svim uredjajima
MAX_PROMASAJA = 3  # Posle 3 uzastopna neodgovorena proglasavamo uredjaj nedostupnim

VREME_GASENJA_SVETLA_SEC = 10
pocetak_bez_pokreta = None
svetlo_trenutno = "off"

"""
Lista dozvoljenih tipova uredjaja
Samo uredjaji ciji tip se nalazi ovde ce biti prihvaceni
Ako zelimo da dozvolimo novi tip, samo dodamo string u ovu listu
"""
DOZVOLJENI_TIPOVI = [
    "urn:schemas-upnp-org:device:WaterHeaterActuator:1",
    "urn:schemas-upnp-org:device:TemperatureSensor:1",
    "urn:schemas-upnp-org:device:LightActuator:1",
    "urn:schemas-upnp-org:device:MotionSensor:1",
    "urn:schemas-upnp-org:device:FanActuator:1",
    "urn:schemas-upnp-org:device:HumiditySensor:1",
]

uredjaji = {}

# Zakljucavanje — vise niti pristupa recniku, ovo sprecava konflikte
lock = threading.Lock()

mqtt_client = None  # MQTT client


# Callback funkcija
def promena_stanja(uredjaj_id, staro_stanje, novo_stanje):
    # Ova funkcija se poziva automatski kada se stanje nekog uredjaja promeni

    print(f"\n[CALLBACK] Promena stanja: {uredjaj_id}")
    print(f"  Staro: {staro_stanje}")
    print(f"  Novo:  {novo_stanje}\n")


# Azuriranje stanja uredjaja
def azuriraj_stanje(uredjaj_id, novo_stanje_recnik):
    """
    Azurira stanje uredjaja u internoj listi
    Ako je stanje zaista promenjeno, poziva callback
    """
    with lock:  # Zakljucavamo dok menjamo recnik (bezbednost niti)
        if uredjaj_id not in uredjaji:
            return  # Uredjaj nije registrovan, ignorisemo

        staro = uredjaji[uredjaj_id]["stanje"]  # Pamtimo staro stanje

        # Proveravamo da li je stanje zaista promenjeno
        if staro != novo_stanje_recnik:
            uredjaji[uredjaj_id]["stanje"] = novo_stanje_recnik  # Postavljamo novo stanje
            threading.Thread(target=promena_stanja, args=(uredjaj_id, dict(staro), dict(novo_stanje_recnik)), daemon=True).start()


# Registracija novog uredjaja
def registruj_uredjaj(uredjaj_id, tip, grupa, ip, port):
    # Dodaje novi uredjaj u internu listu ili reaktivira vec postojeci

    with lock:
        if uredjaj_id not in uredjaji:
            # Novi uredjaj — dodajemo ga u recnik sa pocetnim vrednostima
            uredjaji[uredjaj_id] = {
                "id":        uredjaj_id,
                "tip":       tip,
                "grupa":     grupa,
                "ip":        ip,
                "port":      port,   # cuva se port za slanje STATUS upita
                "stanje":    {},     # Stanje ce biti popunjeno prvim STATUS odgovorom
                "aktivan":   True,
                "promasaji": 0
            }
            print(f"[+] Registrovan novi uredjaj: {uredjaj_id} | Tip: {tip} | IP: {ip} | Port: {port}")
        else:
            # Vec postoji — samo ga reaktiviramo (npr. ako je doslo do restarta)
            uredjaji[uredjaj_id]["aktivan"]   = True
            uredjaji[uredjaj_id]["promasaji"] = 0
            uredjaji[uredjaj_id]["ip"]        = ip
            uredjaji[uredjaj_id]["port"]      = port   # osvezavamo port u slucaju promene
            print(f"[~] Reaktiviran uredjaj: {uredjaj_id}")


# Deregistracija (BYEBYE)
def deregistruj_uredjaj(uredjaj_id):
    # Oznacava uredjaj kao neaktivan kada posalje BYEBYE poruku.
    global mqtt_client

    with lock:
        if uredjaj_id in uredjaji:
            uredjaji[uredjaj_id]["aktivan"] = False
            print(f"[-] Uredjaj se ugasio (byebye): {uredjaj_id}")

            delovi = uredjaj_id.split("-")
            if len(delovi) >= 3:
                ime_uredjaja = f"{delovi[1]}-{delovi[2]}"
            else:
                ime_uredjaja = delovi[1] if len(delovi) > 1 else "nepoznato"

            poruka_offline = json.dumps({"uredjaj": ime_uredjaja, "stanje": "offline"})
            if mqtt_client:
                mqtt_client.publish("kupatilo/obavestenja", poruka_offline)


# Parsiranje SSDP NOTIFY poruke
def parsiraj_notify(tekst):
    """
    Iz teksta SSDP NOTIFY poruke izvlaci kljucne vrednosti
    Vraca recnik sa kljucevima: NT, NTS, USN, GRUPA, PORT
    ili None ako parsiranje nije uspelo
    """
    rezultat = {}

    for linija in tekst.split("\r\n"):
        if ":" in linija:
            # partition(":") deli samo na prvoj dvotacki
            # (koristimo ovo umesto split(":") jer vrednost moze sadrzati ":")
            kljuc, _, vrednost = linija.partition(":")
            rezultat[kljuc.strip().upper()] = vrednost.strip()

    # Proveravamo da li postoje obavezna polja
    if "NT" in rezultat and "NTS" in rezultat and "USN" in rezultat:
        return rezultat

    return None


# Slusanje na multicast (SSDP Listener)
def slusaj_multicast():
    """
    Slusa na SSDP multicast adresi za ALIVE i BYEBYE poruke od uredjaja
    Ova funkcija radi u posebnoj niti, u beskonacnoj petlji
    """
    # Kreiramo UDP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # SO_REUSEADDR dozvoljava vise programa da slusa na istom portu
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Vezujemo socket za SSDP port (svi mrezni interfejsi)
    s.bind(("", MULTICAST_PORT))

    # Prijavljivanje na multicast grupu — ovo je obavezno da bi OS
    # prosledjivao multicast pakete ovom socketu
    adresa = socket.inet_aton(MULTICAST_ADDR) + socket.inet_aton("0.0.0.0")
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, adresa)

    print(f"[LISTEN] Slusam SSDP na {MULTICAST_ADDR}:{MULTICAST_PORT} ...")

    while True:
        # Cekamo dolazni UDP paket (blokira dok ne stigne nesto)
        # ip je IP adresa posiljaoca (adresa uredjaja koji se oglasava)
        podaci, (ip, _) = s.recvfrom(4096)
        tekst = podaci.decode(errors="ignore")

        # Proveravamo da li je NOTIFY poruka (ALIVE ili BYEBYE)
        if "NOTIFY" not in tekst:
            continue  # Nije SSDP NOTIFY — preskacemo

        # Parsiramo zaglavlja
        zaglavlja = parsiraj_notify(tekst)
        if zaglavlja is None:
            continue  # Neispravna poruka — preskacemo

        tip = zaglavlja.get("NT", "")
        nts = zaglavlja.get("NTS", "")
        uid = zaglavlja.get("USN", "")
        grupa = zaglavlja.get("GRUPA", "NEPOZNATA")

        try:
            port = int(zaglavlja.get("PORT", "1901"))
        except ValueError:
            continue

        # Kontrola pristupa
        # Ako tip uredjaja nije na listi dozvoljenih, odbijamo ga
        if tip not in DOZVOLJENI_TIPOVI:
            print(f"[BLOCKED] Odbijen uredjaj tipa '{tip}' | ID: {uid}")
            continue

        # Obrada na osnovu NTS vrednosti
        if "alive" in nts:
            # Uredjaj se oglasava kao aktivan — registrujemo ga sa svim podacima
            registruj_uredjaj(uid, tip, grupa, ip, port)

        elif "byebye" in nts:
            # Uredjaj se gasi — oznacavamo ga kao neaktivan
            deregistruj_uredjaj(uid)


# Periodicno slanje STATUS upita i primanje odgovora
def periodicni_status():
    """
    Svakih 5 sekundi salje STATUS upit svim aktivnim uredjajima
    i ODMAH ceka odgovor od svakog

    Ako uredjaj ne odgovori u roku od 2 sekunde -> timeout -> brojimo promasaj
    Ako uredjaj ne odgovori 3 puta zaredom -> proglasavamo ga nedostupnim
    """
    # Kreiramo UDP socket za slanje STATUS upita
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.bind(("", KONTROLER_STATUS_PORT))

    # Timeout od 2 sekunde: ako uredjaj ne odgovori, recvfrom baca socket.timeout
    s.settimeout(2.0)

    while True:
        time.sleep(INTERVAL_STATUS_SEC)  # Cekamo pre svakog kruga

        with lock:
            uredjaji_kopija = dict(uredjaji)

        # Prolazimo kroz sve registrovane uredjaje
        for uredjaj_id, info in uredjaji_kopija.items():

            if not info["aktivan"]:
                continue  # Preskacemo vec neaktivne uredjaje

            ip = info["ip"]    # IP adresa uredjaja
            port = info["port"]  # Port na kom uredjaj slusa (iz JSON-a, prenet kroz NOTIFY)

            # Saljemo STATUS upit direktno uredjaju (unicast)
            poruka = f"STATUS {uredjaj_id}"
            try:
                s.sendto(poruka.encode(), (ip, port))
                print(f"[STATUS] Poslat upit za {uredjaj_id} na {ip}:{port}")
            except Exception as e:
                print(f"[WARNING] Nije mogao da se posalje STATUS za {uredjaj_id}: {e}")
                continue  # Prelazimo na sledeci uredjaj

            # Cekamo odgovor od uredjaja
            try:
                podaci, _ = s.recvfrom(4096)
                tekst = podaci.decode(errors="ignore")

                # Proveravamo da li je odgovor STATUS-REPLY od pravog uredjaja
                if "STATUS-REPLY" not in tekst or uredjaj_id not in tekst:
                    continue

                # Parsiramo JSON stanje iz odgovora
                # Format odgovora: "STATUS-REPLY USN:id:... STATE:{...json...}"
                state_deo = tekst.split("STATE:")[1]   # Sve iza "STATE:"
                novo_stanje = json.loads(state_deo)      # Parsiramo JSON string u recnik

                # Resetujemo brojac promasaja jer smo dobili validan odgovor
                with lock:
                    if uredjaj_id in uredjaji:
                        uredjaji[uredjaj_id]["promasaji"] = 0

                # Azuriramo stanje — ako se promenilo, poziva se callback
                azuriraj_stanje(uredjaj_id, novo_stanje)

            except (socket.timeout, ConnectionResetError) as e:
                # Uredjaj nije odgovorio u roku od 2 sekunde
                with lock:
                    if uredjaj_id in uredjaji and uredjaji[uredjaj_id]["aktivan"]:

                        # Povecavamo brojac promasaja
                        uredjaji[uredjaj_id]["promasaji"] += 1
                        promasaji = uredjaji[uredjaj_id]["promasaji"]

                        print(f"[TIMEOUT] {uredjaj_id} nije odgovorio ({promasaji}/{MAX_PROMASAJA})")

                        # Ako smo premašili limit — proglasavamo uredjaj nedostupnim
                        if promasaji >= MAX_PROMASAJA:
                            uredjaji[uredjaj_id]["aktivan"] = False
                            print(f"[STATUS] Uredjaj proglašen nedostupnim: {uredjaj_id}")

                            delovi = uredjaj_id.split("-")
                            if len(delovi) >= 3:
                                ime_uredjaja = f"{delovi[1]}-{delovi[2]}"
                            else:
                                ime_uredjaja = delovi[1] if len(delovi) > 1 else "nepoznato"

                            alarm_poruka = json.dumps({"uredjaj": ime_uredjaja, "stanje": "safe_mode"})
                            if mqtt_client:
                                mqtt_client.publish("kupatilo/obavestenja", alarm_poruka)
                                

# Stampanje stanja liste
def stampaj_listu():
    """
    Pomocna funkcija — stampa trenutnu listu svih uredjaja
    Poziva se periodicno iz main-a
    """
    with lock:
        print("\n─── Trenutna lista uredjaja ────────────────────────────")
        if not uredjaji:
            print("  (prazno — cekam da se uredjaji oglase)")
        for uid, info in uredjaji.items():
            status_str = "AKTIVAN" if info["aktivan"] else "NEAKTIVAN"
            print(f"  [{status_str}] {uid}")
            print(f"    Tip:    {info['tip']}")
            print(f"    Grupa:  {info['grupa']}")
            print(f"    IP:     {info['ip']}:{info['port']}")
            print(f"    Stanje: {info['stanje']}")
        print("────────────────────────────────────────────────────────\n")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Kontroler povezan na MQTT broker")
        # Pretplata na teme svih senzora
        client.subscribe("kupatilo/+/stanje")
        # Pretplata na komande korisnicke aplikacije
        client.subscribe("kupatilo/komande")
    else:
        print(f"[MQTT] Neuspesna konekcija, kod: {rc}")


def da_li_je_aktivan(ime_uredjaja):
    with lock:
        for uid, info in uredjaji.items():
            if f"-{ime_uredjaja}-aktuator" in uid and info["aktivan"]:
                return True
        
        return False


def on_message(client, userdata, msg):
    global pocetak_bez_pokreta, svetlo_trenutno

    tema = msg.topic
    poruka = msg.payload.decode("utf-8")
    print(f"[MQTT] Tema: {tema} | Poruka: {poruka}")

    if tema.endswith("/stanje"):
        ime_uredjaja = tema.split("/")[1]

        try:
            stanje = json.loads(poruka)

            # Logika kontrolera
            if ime_uredjaja == "ventilator" and "vlaznost_vazduha" in stanje:
                vlaga = stanje["vlaznost_vazduha"]

                if vlaga > 50:
                    print(f"[LOGIC] Vlaznost je {vlaga}% -> PALIM VENTILATOR")
                    if da_li_je_aktivan("ventilator"):
                        client.publish("kupatilo/ventilator/set", "ON")
                elif vlaga < 30:
                    print(f"[LOGIC] Vlaznost je {vlaga}% -> GASIM VENTILATOR")
                    if da_li_je_aktivan("ventilator"):
                        client.publish("kupatilo/ventilator/set", "OFF")
            elif ime_uredjaja == "svetlo" and "pokret_detektovan" in stanje:
                pokret = stanje["pokret_detektovan"]
                sada = time.time()

                if pokret == True:
                    pocetak_bez_pokreta = None

                    if svetlo_trenutno != "on":
                        print("[LOGIC] Pokret detektovan -> PALIM SVETLO")
                        if da_li_je_aktivan("svetlo"):
                            client.publish("kupatilo/svetlo/set", "ON", qos=1)
                            svetlo_trenutno = "on"
                    else:
                        print("[LOGIC] Pokret detektovan -> svetlo je VEC UPALJENO")
                else:
                    if pocetak_bez_pokreta is None:
                        pocetak_bez_pokreta = sada

                    proslo = sada - pocetak_bez_pokreta

                    if svetlo_trenutno == "on" and proslo >= VREME_GASENJA_SVETLA_SEC:
                        print("[LOGIC] Pokret nije detektovan -> GASIM SVETLO")
                        if da_li_je_aktivan("svetlo"):
                            client.publish("kupatilo/svetlo/set", "OFF", qos=1)
                            svetlo_trenutno = "off"
            elif ime_uredjaja == "bojler" and "temperatura_vode" in stanje:
                temperatura = stanje["temperatura_vode"]

                if temperatura >= 60:
                    print(f"[LOGIC] Temperatura vode je {temperatura} -> GASIM BOJLER")
                    if da_li_je_aktivan("bojler"):
                        client.publish("kupatilo/bojler/set", "OFF")
                elif temperatura < 10:
                    print(f"[LOGIC] Temperatura vode je {temperatura} -> PALIM BOJLER")
                    if da_li_je_aktivan("bojler"):
                        client.publish("kupatilo/bojler/set", "ON")

        except json.JSONDecodeError:
            print("[FAILED] Neispravan JSON format")
    elif tema == "kupatilo/komande":
        try:
            komanda = json.loads(poruka)
            uredjaj = komanda.get("uredjaj")
            akcija = komanda.get("akcija")

            print(f"[APLIKACIJA] Korisnik rucno postavlja {uredjaj} na {akcija}")

            client.publish(f"kupatilo/{uredjaj}/set", akcija)

        except json.JSONDecodeError:
            print("[FAILED] Neispravan JSON format")

# Main
def main():
    global mqtt_client

    print("*** SSDP Kontroler pokrenut ***\n")
    print(f"  Slusam SSDP na portu  : {MULTICAST_PORT}")
    print(f"  Saljem STATUS sa porta: {KONTROLER_STATUS_PORT}")
    print(f"  Interval provere      : {INTERVAL_STATUS_SEC}s")
    print(f"  Dozvoljeni tipovi     : {DOZVOLJENI_TIPOVI}\n")

    # Pokrecemo dve niti koje rade paralelno:

    # Nit 1: Slusa SSDP multicast (ALIVE / BYEBYE od uredjaja)
    # Ova nit samo PRIMA i registruje/deregistruje uredjaje
    t1 = threading.Thread(target=slusaj_multicast, daemon=True)
    t1.start()

    # Nit 2: Periodicno salje STATUS svim aktivnim uredjajima I prima odgovore
    # Ova nit obavlja i slanje i primanje — isti socket, isti port (1902)
    t2 = threading.Thread(target=periodicni_status, daemon=True)
    t2.start()

    # Inicijalizacija MQTT modula na strani kontrolera 
    mqtt_client = mqtt.Client(client_id="id:kontrolna-jedinica")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Glavna nit samo ceka i svakih 10 sekundi stampa listu uredjaja
    try:
        while True:
            time.sleep(10)
            stampaj_listu()
    except KeyboardInterrupt:
        print("\n[EXIT] Kontroler se gasi.")


if __name__ == "__main__":
    main()
