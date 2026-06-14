import tkinter as tk
from tkinter import font
import paho.mqtt.client as mqtt
import json

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883

ui_elementi = {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Aplikacija povezana na broker")

        client.subscribe("kupatilo/+/stanje")

        client.subscribe("kupatilo/obavestenja")
    else:
        print(f"[MQTT] Neuspesna konekcija, kod: {rc}")


def azuriraj_boje(uredjaj, stanje):
    if uredjaj in ui_elementi:
        if stanje == "offline":
            boja_pozadine = "#c0392b"
            tekst_alarma = "⚠️ UREĐAJ NE ODGOVARA (SAFE MODE) ⚠️"
            obavestenje.set(f"🚨 ALARM: Sistem detektovao kvar na uređaju: {uredjaj.upper()} 🚨")
        else:
            boja_pozadine = "#34495e"
            tekst_alarma = ""
            obavestenje.set("✅ Sistem radi u normalnom režimu ✅")

        element = ui_elementi[uredjaj]
        element["frame"].configure(bg=boja_pozadine)
        element["naslov"].configure(bg=boja_pozadine)
        element["ocitavanje"].configure(bg=boja_pozadine)
        element["alarm_var"].set(tekst_alarma)
        element["btn_frame"].configure(bg=boja_pozadine)


def on_message(client, userdata, msg):
    tema = msg.topic
    poruka = msg.payload.decode("utf-8")

    if tema == "kupatilo/obavestenja":
        try:
            podaci = json.loads(poruka)
            uredjaj = podaci.get("uredjaj")
            stanje = podaci.get("stanje")

            if uredjaj:
                root.after(0, azuriraj_boje, uredjaj, stanje)
        
        except json.JSONDecodeError:
            print("[FAILED] Neispravan JSON format")
    
    elif tema.endswith("/stanje"):
        delovi = tema.split("/")

        if len(delovi) >= 2:
            uredjaj = delovi[1]
            root.after(0, azuriraj_boje, uredjaj, "online")
        else:
            return

        try:
            stanje = json.loads(poruka)

            if "ventilator" in tema and "vlaznost_vazduha" in stanje:
                vlaga.set(f"Trenutna vlažnost: {stanje["vlaznost_vazduha"]}%")
            elif "svetlo" in tema and "pokret_detektovan" in stanje:
                if stanje["pokret_detektovan"]:
                    pokret.set("Pokret detektovan: DA")
                else:
                    pokret.set("Pokret detektovan: NE")
            elif "bojler" in tema and "temperatura_vode" in stanje:
                temperatura.set(f"Temperatura vode: {stanje["temperatura_vode"]}°C")

        except json.JSONDecodeError:
            print("[FAILED] Neispravan JSON format")


def posalji_komandu(uredjaj, akcija):
    komanda = {"uredjaj": uredjaj, "akcija": akcija}

    mqtt_client.publish("kupatilo/komande", json.dumps(komanda))
    print(f"[KOMANDA] Poslato manuelno: {uredjaj} -> {akcija}")


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="korisnicka-aplikacija")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

root = tk.Tk()
root.title("PAMETNO KUPATILO")
root.geometry("450x550")
root.configure(bg="#2c3e50")

naslov_font = font.Font(family="Helvetica", size=14, weight="bold")
tekst_font = font.Font(family="Helvetica", size=12)

vlaga = tk.StringVar(value="Trenutna vlažnost: --%")
temperatura = tk.StringVar(value="Temperatura vode: --°C")
pokret = tk.StringVar(value="Pokret detektovan: --")
obavestenje = tk.StringVar(value="✅ Sistem radi u normalnom rezimu ✅")


def napravi_karticu(root, naslov_tekst, ocitavanje_vrednost, ime_uredjaja):
    frame = tk.Frame(root, bg="#34495e", bd=2, relief=tk.GROOVE)
    frame.pack(fill=tk.X, padx=15, pady=10)

    naslov = tk.Label(frame, text=naslov_tekst, font=naslov_font, bg="#34495e", fg="#ecf0f1")
    naslov.pack(pady=5)

    ocitavanje = tk.Label(frame, textvariable=ocitavanje_vrednost, font=tekst_font, bg="#34495e", fg="#f1c40f")
    ocitavanje.pack(pady=5)

    alarm_var = tk.StringVar(value="")
    alarm = tk.Label(frame, textvariable=alarm_var, font=("Helvetica", 10, "bold"), bg="#34495e", fg="white")
    alarm.pack()

    btn_frame = tk.Frame(frame, bg="#34495e")
    btn_frame.pack(pady=10)

    tk.Button(btn_frame, text="🟢 UKLJUČI", bg="#27ae60", fg="white", font=tekst_font, width=10,
              command=lambda: posalji_komandu(ime_uredjaja, "ON")).pack(side=tk.LEFT, padx=10)
    
    tk.Button(btn_frame, text="🔴 ISKLJUČI", bg="#c0392b", fg="white", font=tekst_font, width=10,
              command=lambda: posalji_komandu(ime_uredjaja, "OFF")).pack(side=tk.RIGHT, padx=10)
    

    ui_elementi[ime_uredjaja] = {
        "frame": frame, "naslov": naslov, "ocitavanje": ocitavanje,
        "alarm_var":alarm_var, "btn_frame": btn_frame
    }

tk.Label(root, text="KONTROLNA TABLA", font=("Helvetica", 18, "bold"), bg="#2c3e50", fg="white").pack(pady=15)

napravi_karticu(root, "VENTILACIJA", vlaga, "ventilator")
napravi_karticu(root, "BOJLER", temperatura, "bojler")
napravi_karticu(root, "OSVETLJENJE", pokret, "svetlo")

status = tk.Frame(root, bg="#f39c12")
status.pack(side=tk.BOTTOM, fill=tk.X)
tk.Label(status, textvariable=obavestenje, font=("Helvetica", 11, "bold"), bg="#f39c12", fg="black").pack(pady=10)

root.mainloop()