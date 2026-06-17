import tkinter as tk
from tkinter import font
import paho.mqtt.client as mqtt
import json

MQTT_BROKER = "10.1.148.26"
MQTT_PORT = 1883

ui_elementi = {}


# --- MQTT LOGIKA ---
def on_connect(client, userdata, flags, rc):
    print("[MQTT] Aplikacija povezana na broker!")
    client.subscribe("kupatilo/+/stanje")    # Ocitavanja senzora
    client.subscribe("kupatilo/+/set")       # Komande ka aktuatoru
    client.subscribe("kupatilo/obavestenja") # Alarmi za kvarove


def azuriraj_boje(meta_id, stanje):
    if meta_id in ui_elementi:
        kljucevi = [meta_id]
    else:
        kljucevi = [f"{meta_id}-senzor", f"{meta_id}-aktuator"]

    for kljuc in kljucevi:
        if kljuc in ui_elementi:
            elementi = ui_elementi[kljuc]
            
            # 1. KVAR (Safe Mode) -> Postavljamo CRVENU boju, ispisujemo gresku i gasimo dugmice
            if stanje == "safe_mode":
                boja_pozadine = "#c0392b"
                
                elementi["frame"].configure(bg=boja_pozadine)
                for widget in elementi["labele"]:
                    widget.configure(bg=boja_pozadine)
                    
                elementi["var"].set("NEDOSTUPAN\n(SAFE MODE)")
                
                # Onemogucavamo dugmice ako je u pitanju aktuator
                if not elementi["is_senzor"]:
                    for btn in elementi.get("dugmici", []):
                        btn.configure(state=tk.DISABLED)
                
            # 2. NORMALNO GAŠENJE (Offline / byebye) -> Vracamo pozadinu, crtice i gasimo dugmice
            elif stanje == "offline":
                boja_pozadine = "#34495e"
                
                elementi["frame"].configure(bg=boja_pozadine)
                for widget in elementi["labele"]:
                    widget.configure(bg=boja_pozadine)
                    
                if elementi["is_senzor"]:
                    if "ventilator" in kljuc:
                        elementi["var"].set("-- %")
                    elif "bojler" in kljuc:
                        elementi["var"].set("-- °C")
                    elif "svetlo" in kljuc or "pokret" in kljuc:
                        elementi["var"].set("--")
                else:
                    elementi["var"].set("Stanje: Nepoznato")
                    # Onemogucavamo dugmice jer uredjaj nije na mrezi
                    for btn in elementi.get("dugmici", []):
                        btn.configure(state=tk.DISABLED)
                    
            # 3. UREĐAJ RADI (Online) -> Vracamo PLAVU boju i palimo dugmice
            elif stanje == "online":
                boja_pozadine = "#34495e"
                
                elementi["frame"].configure(bg=boja_pozadine)
                for widget in elementi["labele"]:
                    widget.configure(bg=boja_pozadine)
                
                # Omogucavamo dugmice jer je uredjaj online
                if not elementi["is_senzor"]:
                    for btn in elementi.get("dugmici", []):
                        btn.configure(state=tk.NORMAL)


def on_message(client, userdata, msg):
    tema = msg.topic
    poruka = msg.payload.decode("utf-8")
    
    if tema == "kupatilo/obavestenja":
        try:
            podaci = json.loads(poruka)
            precizan_uredjaj = podaci.get("uredjaj")
            stanje = podaci.get("stanje")
            if precizan_uredjaj:
                root.after(0, azuriraj_boje, precizan_uredjaj, stanje)
        except json.JSONDecodeError:
            pass

    elif tema.endswith("/stanje"):
        delovi = tema.split("/")
        if len(delovi) >= 2:
            ime_senzora = delovi[1]
            root.after(0, azuriraj_boje, f"{ime_senzora}-senzor", "online")
            
        try:
            stanje = json.loads(poruka)
            if "ventilator" in tema and "vlaznost_vazduha" in stanje:
                vlaga_var.set(f"{stanje['vlaznost_vazduha']} %")
            elif "bojler" in tema and "temperatura_vode" in stanje:
                temp_var.set(f"{stanje['temperatura_vode']} °C")
            elif "svetlo" in tema and "pokret_detektovan" in stanje:
                pokret_var.set("DA" if stanje["pokret_detektovan"] else "NE")
        except json.JSONDecodeError:
            pass

    elif tema.endswith("/set"):
        delovi = tema.split("/")
        if len(delovi) >= 2:
            ime_aktuatora = delovi[1]
            root.after(0, azuriraj_boje, f"{ime_aktuatora}-aktuator", "online")
            
            novo_stanje = poruka.upper()
            if ime_aktuatora == "ventilator":
                vent_akt_var.set(f"Stanje: {novo_stanje}")
            elif ime_aktuatora == "bojler":
                bojler_akt_var.set(f"Stanje: {novo_stanje}")
            elif ime_aktuatora == "svetlo":
                svetlo_akt_var.set(f"Stanje: {novo_stanje}")


def posalji_komandu(uredjaj, akcija):
    komanda = {"uredjaj": uredjaj, "akcija": akcija}
    mqtt_client.publish("kupatilo/komande", json.dumps(komanda))

# Inicijalizacija MQTT klijenta
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="korisnicka-aplikacija")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

#-----GUI-----
root = tk.Tk()
root.title("Pametno Kupatilo")
root.geometry("600x650")
root.configure(bg="#1a252f") 

# Fontovi
naslov_font = font.Font(family="Helvetica", size=15, weight="bold")
mali_naslov = font.Font(family="Helvetica", size=15, weight="bold")
tekst_font = font.Font(family="Helvetica", size=16, weight="bold")
akt_font = font.Font(family="Helvetica", size=13)

# Varijable (Senzori)
vlaga_var = tk.StringVar(value="-- %")
temp_var = tk.StringVar(value="-- °C")
pokret_var = tk.StringVar(value="--")

# Varijable (Aktuatori)
vent_akt_var = tk.StringVar(value="Stanje: Nepoznato")
bojler_akt_var = tk.StringVar(value="Stanje: Nepoznato")
svetlo_akt_var = tk.StringVar(value="Stanje: Nepoznato")


def napravi_karticu(roditelj, naslov_tekst, ime_senzora, ime_aktuatora, senzor_var, aktuator_var, merna_jedinica_tekst):
    card_frame = tk.Frame(roditelj, bg="#2c3e50", bd=0)
    card_frame.pack(expand=True, fill=tk.BOTH, padx=15, pady=10)
    
    tk.Label(card_frame, text=naslov_tekst, font=naslov_font, bg="#2c3e50", fg="#ecf0f1").pack(pady=5)
    
    content_frame = tk.Frame(card_frame, bg="#2c3e50")
    content_frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=(0, 10))
    content_frame.columnconfigure(0, weight=1, uniform="polovina")
    content_frame.columnconfigure(1, weight=1, uniform="polovina")
    content_frame.rowconfigure(0, weight=1)

    # --- LEVA STRANA: SENZOR ---
    s_frame = tk.Frame(content_frame, bg="#34495e", bd=2, relief=tk.FLAT)
    s_frame.grid(row=0, column=0, sticky="nsew", padx=5)
    
    sl1 = tk.Label(s_frame, text="SENZOR", font=mali_naslov, bg="#34495e", fg="#95a5a6")
    sl1.pack(pady=10)
    sl2 = tk.Label(s_frame, text=merna_jedinica_tekst, font=("Helvetica", 13), bg="#34495e", fg="#ecf0f1")
    sl2.pack()
    sl3 = tk.Label(s_frame, textvariable=senzor_var, font=tekst_font, bg="#34495e", fg="#f1c40f")
    sl3.pack(expand=True, pady=(0, 15))
    
    ui_elementi[f"{ime_senzora}-senzor"] = {
        "frame": s_frame, 
        "labele": [sl1, sl2, sl3], 
        "var": senzor_var,
        "is_senzor": True
    }

    # --- DESNA STRANA: AKTUATOR ---
    a_frame = tk.Frame(content_frame, bg="#34495e", bd=2, relief=tk.FLAT)
    a_frame.grid(row=0, column=1, sticky="nsew", padx=5)
    
    al1 = tk.Label(a_frame, text="AKTUATOR", font=mali_naslov, bg="#34495e", fg="#95a5a6")
    al1.pack(pady=10)
    al2 = tk.Label(a_frame, textvariable=aktuator_var, font=akt_font, bg="#34495e", fg="#3498db")
    al2.pack(pady=5)
    
    btn_frame = tk.Frame(a_frame, bg="#34495e")
    btn_frame.pack(expand=True)
    
    btn_on = tk.Button(btn_frame, text="ON", bg="#27ae60", fg="white", font=("Helvetica", 10, "bold"), width=5,
              state=tk.DISABLED, command=lambda: posalji_komandu(ime_aktuatora, "ON"))
    btn_on.pack(side=tk.LEFT, padx=5)
    
    btn_off = tk.Button(btn_frame, text="OFF", bg="#c0392b", fg="white", font=("Helvetica", 10, "bold"), width=5,
              state=tk.DISABLED, command=lambda: posalji_komandu(ime_aktuatora, "OFF"))
    btn_off.pack(side=tk.RIGHT, padx=5)

    ui_elementi[f"{ime_aktuatora}-aktuator"] = {
        "frame": a_frame, 
        "labele": [al1, al2, btn_frame], 
        "var": aktuator_var,
        "is_senzor": False,
        "dugmici": [btn_on, btn_off] 
    }


# Generisanje Interfejsa (Prosleđujemo imena senzora i aktuatora)
tk.Label(root, text="KONTROLNA TABLA", font=("Helvetica", 18, "bold"), bg="#1a252f", fg="white").pack(pady=15)

napravi_karticu(root, "VENTILACIJA", "ventilator", "ventilator", vlaga_var, vent_akt_var, "Vlažnost vazduha:")
napravi_karticu(root, "BOJLER", "bojler", "bojler", temp_var, bojler_akt_var, "Temperatura vode:")
napravi_karticu(root, "OSVETLJENJE", "svetlo", "svetlo", pokret_var, svetlo_akt_var, "Prisustvo:")

root.mainloop()