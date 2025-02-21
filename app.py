import streamlit as st
import psycopg2
import os
from urllib.parse import urlparse
import bcrypt
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from dotenv import load_dotenv

# Verbindung zur PostgreSQL-Datenbank unter Verwendung von Umgebungsvariablen
def get_db_connection():
    # Lade Umgebungsvariablen aus der .env Datei
    load_dotenv()

    # Hole die Umgebungsvariablen
    pg_user = os.getenv('PGUSER')
    pg_password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('RAILWAY_TCP_PROXY_DOMAIN')
    port = os.getenv('RAILWAY_TCP_PROXY_PORT')
    database = os.getenv('PGDATABASE')

    # Prüfen, ob alle Variablen gesetzt sind
    if not all([pg_user, pg_password, host, port, database]):
        raise ValueError("Fehlende Umgebungsvariablen: PGUSER, POSTGRES_PASSWORD, RAILWAY_TCP_PROXY_DOMAIN, RAILWAY_TCP_PROXY_PORT, PGDATABASE")

    # Setze die URL zusammen
    database_url = f"postgresql://{pg_user}:{pg_password}@{host}:{port}/{database}"

    # Parse die URL
    result = urlparse(database_url)

    # Extrahiere die Verbindungsdetails
    host = result.hostname
    port = result.port if result.port else 5432  # Falls kein Port angegeben ist, verwende den Standardport 5432
    user = result.username
    password = result.password
    database = result.path[1:]  # Entferne das führende '/' von der Datenbank

    # Stelle die Verbindung her
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        return conn
    except Exception as e:
        st.error(f"Fehler bei der Verbindung zur Datenbank: {e}")
        st.text(host)
        raise

# Tabelle erstellen (PostgreSQL)
def create_db():
    conn = get_db_connection()
    c = conn.cursor()

    # Tabelle für Produkte erstellen
    c.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        weingut TEXT,
        rebsorte TEXT,
        lage TEXT,
        land TEXT,
        jahrgang TEXT,
        lagerort TEXT,
        bestandsmenge INTEGER DEFAULT 0,
        preis_pro_einheit REAL,
        gesamtpreis REAL,
        alko TEXT,      
        zucker TEXT,
        saure TEXT,
        info TEXT,      
        kauf_link TEXT,                        
        comments TEXT              
    )''')

    # Tabelle für Buchungen erstellen
    c.execute('''
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id SERIAL PRIMARY KEY,
        booking_art TEXT,
        product_id INTEGER,
        buchungsdatum DATE,
        menge INTEGER,
        buchungstyp TEXT,
        comments TEXT,      
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
    ''')

    # Tabelle für Benutzer erstellen
    c.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT
    )
    ''')

    conn.commit()
    conn.close()

 # Session-Variable initialisieren
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if "username" not in st.session_state:
        st.session_state["username"] = ""

# Funktion um Benutzer zu validieren (Login-Funktion)
def login(username, password):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM users WHERE username = %s', (username,))
    user = c.fetchone()
    conn.close()
    
    if user and bcrypt.checkpw(password.encode('utf-8'), user[1].encode('utf-8')):
        st.session_state["authenticated"] = True
        st.session_state["username"] = username
        st.sidebar.success(f"Willkommen {username}!")
    else:
        st.sidebar.error("Benutzername oder Passwort ist falsch!")

# Logout-Funktion
def logout():
    st.session_state["authenticated"] = False
    st.session_state["username"] = ""

# Funktion Produkt anlegen
def register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments):
    conn = get_db_connection()
    c = conn.cursor()

    # Überprüfen, ob das Produkt bereits existiert und die Produkt-ID abfragen
    c.execute('''
        SELECT product_id FROM products 
        WHERE weingut = %s AND rebsorte = %s AND lage = %s AND land = %s AND jahrgang = %s AND lagerort = %s
    ''', (weingut, rebsorte, lage, land, jahrgang, lagerort))

    existing_product = c.fetchone()

    if existing_product:
        # Produkt existiert bereits, die ID aus der Antwort extrahieren
        product_id = existing_product[0]
        st.error(f"Dieses Produkt ist bereits unter der Nummer {product_id} angelegt!")
    else:
        # Produkt einfügen, wenn es nicht existiert
        gesamtpreis = 0
        c.execute('''
            INSERT INTO products (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments, gesamtpreis)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id
        ''', (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments, gesamtpreis))
        
        # Die zurückgegebene product_id abrufen
        new_product_id = c.fetchone()[0]

        conn.commit()
        st.success(f"Die Produknummer {new_product_id} wurde erfolgreich angelegt!")

    conn.close()

# Funktion Produkt änderen
def adjust_product(product_id, new_weingut, new_rebsorte, new_lage, new_land, new_jahrgang,
                   new_lagerort, new_preis_pro_einheit, new_alko, new_zucker, new_saure,
                   new_info, new_kauf_link, new_comments):
     conn = get_db_connection()
     c = conn.cursor()

     # Produktdetails abrufen
     c.execute('''
             SELECT weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko,
             zucker, saure, info, kauf_link, comments
             FROM products
             WHERE product_id = %s
             ''', (product_id,))
     product = c.fetchone()

     if not product:
         st.error(f"Die Produktnummer {product_id} existiert nicht!")
         conn.close()
         return

     # Tupel-Indizierung
     old_weingut, old_rebsorte, old_lage, old_land, old_jahrgang, old_lagerort, old_preis_pro_einheit, \
     old_alko, old_zucker, old_saure, old_info, old_kauf_link, old_comments = product

     # Nur die geänderten Felder aktualisieren
     update_data = {}

     if new_weingut != old_weingut:
         update_data["weingut"] = new_weingut
     if new_rebsorte != old_rebsorte:
         update_data["rebsorte"] = new_rebsorte
     if new_lage != old_lage:
         update_data["lage"] = new_lage
     if new_land != old_land:
         update_data["land"] = new_land
     if new_jahrgang != old_jahrgang:
         update_data["jahrgang"] = new_jahrgang
     if new_lagerort != old_lagerort:
         update_data["lagerort"] = new_lagerort
     if new_preis_pro_einheit != old_preis_pro_einheit:
         update_data["preis_pro_einheit"] = new_preis_pro_einheit
     if new_alko != old_alko:
         update_data["alko"] = new_alko
     if new_zucker != old_zucker:
         update_data["zucker"] = new_zucker
     if new_saure != old_saure:
         update_data["saure"] = new_saure
     if new_info != old_info:
         update_data["info"] = new_info
     if new_kauf_link != old_kauf_link:
         update_data["kauf_link"] = new_kauf_link
     if new_comments != old_comments:
         update_data["comments"] = new_comments

     # Wenn Änderungen vorhanden sind, führe das Update durch
     if update_data:
         update_fields = ", ".join([f"{key} = %s" for key in update_data.keys()])
         values = list(update_data.values()) + [product_id] # Füge die Produkt-ID am Ende hinzu

     # SQL-Abfrage zur Aktualisierung des Produkts
         query = f"UPDATE products SET {update_fields} WHERE product_id = %s"
         c.execute(query, values)
         conn.commit()
         st.success(f"Die Produktnummer {product_id} wurde erfolgreich geändert!")
    
     else:
         st.warning("Keine Änderungen vorgenommen.")

     conn.close()

# Funktion Wareneingang buchen
def record_incoming_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = get_db_connection()
    c = conn.cursor()

    # Prüfen, ob die Produkt-ID existiert
    c.execute('SELECT * FROM products WHERE product_id = %s', (product_id,))
    product = c.fetchone()

    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return

    # Buchung in der Tabelle 'bookings' einfügen
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))

    # Die gerade eingefügte Buchungs-ID direkt abfragen
    c.execute('SELECT booking_id FROM bookings WHERE product_id = %s ORDER BY booking_id DESC LIMIT 1', (product_id,))
    booking_id = c.fetchone()[0]  # Holt die letzte Buchungs-ID für das Produkt
    
    # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute(''' 
        UPDATE products
        SET bestandsmenge = bestandsmenge + %s   
        WHERE product_id = %s 
    ''', (menge, product_id))
    
    c.execute(''' 
        UPDATE products
        SET gesamtpreis = bestandsmenge * preis_pro_einheit  
    ''')

    conn.commit()
    conn.close()
    st.success(f"Die Wareneingangsnummer {booking_id} wurde erfolgreich gebucht!")

# Funktion Warenausgang buchen
def record_outgoing_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = get_db_connection()
    c = conn.cursor()

    # Überprüfen, ob das Produkt existiert
    c.execute("SELECT bestandsmenge FROM products WHERE product_id = %s", (product_id,))
    product = c.fetchone()
    
    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return
    
    # Überprüfen, ob genügend Bestand vorhanden ist
    if product[0] < menge:
        conn.close()
        st.error(f"Nicht genügend Bestand für die Produktnummer {product_id} (verfügbar: {product[0]}, gewünscht: {menge})!")
        return

    # Buchung in der Tabelle 'bookings' einfügen
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))

    # Die gerade eingefügte Buchungs-ID direkt abfragen
    c.execute('SELECT booking_id FROM bookings WHERE product_id = %s ORDER BY booking_id DESC LIMIT 1', (product_id,))
    booking_id = c.fetchone()[0]  # Holt die letzte Buchungs-ID für das Produkt

   # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute(''' 
        UPDATE products
        SET bestandsmenge = bestandsmenge - %s 
        WHERE product_id = %s
    ''', (menge, product_id))

    c.execute(''' 
        UPDATE products
        SET gesamtpreis = bestandsmenge * preis_pro_einheit
    ''')

    conn.commit()
    conn.close()
    st.success(f"Die Warenausgangsnummer {booking_id} wurde erfolgreich gebucht!")

# Funktion Buchung ändern
def adjust_booking(booking_id, new_menge, new_buchungstyp, new_booking_art, new_buchungsdatum, new_comments):
     conn = get_db_connection()
     c = conn.cursor()

     try:
         # Buchungsdetails abrufen
         c.execute('SELECT product_id, menge, buchungstyp, booking_art, comments, buchungsdatum FROM bookings WHERE booking_id = %s', (booking_id,))
         booking = c.fetchone()

         if not booking:
             st.error(f"Die Buchungsnummer {booking_id} existiert nicht!")
             conn.rollback()  # Änderung rückgängig machen
             conn.close()
             return

         # Tupel-Indizierung
         product_id = booking[0]
         old_menge = booking[1]
         buchungstyp = booking[2]
         booking_art = booking[3]
         comments = booking[4]
         buchungsdatum = booking[5]

         # Buchung in der bookings-Tabelle aktualisieren, wenn sich etwas geändert hat
         if new_menge != old_menge or new_buchungstyp != buchungstyp or new_booking_art != booking_art or new_comments != comments or new_buchungsdatum != buchungsdatum:
             c.execute(''' 
                 UPDATE bookings
                 SET menge = %s, buchungstyp = %s, booking_art = %s, comments = %s, buchungsdatum = %s
                 WHERE booking_id = %s
             ''', (new_menge, new_buchungstyp, new_booking_art, new_comments, new_buchungsdatum, booking_id))

         # Berechnung der Menge nur durchführen, wenn sich die Menge geändert hat
         if new_menge != old_menge or new_buchungstyp != buchungstyp:
             c.execute(''' 
                     SELECT SUM(menge)
                     FROM bookings
                     WHERE product_id = %s and booking_art = 'Wareneingang'
                 ''', (product_id,))

             sum_we = c.fetchone()[0] 
             sum_we = sum_we if sum_we is not None else 0

             c.execute(''' 
                     SELECT SUM(menge)
                     FROM bookings
                     WHERE product_id = %s and booking_art = 'Warenausgang'
                 ''', (product_id,))

             sum_wa = c.fetchone()[0] 
             sum_wa = sum_wa if sum_wa is not None else 0

             new_bestand = sum_we - sum_wa

             if new_bestand >= 0:
                 # Bestandsmenge in products Tabelle anpassen
                 c.execute(''' 
                     UPDATE products
                     SET bestandsmenge = %s
                     WHERE product_id = %s
                 ''', (new_bestand, product_id))

                 # Gesamtpreis in products Tabelle anpassen
                 c.execute(''' 
                     UPDATE products
                     SET gesamtpreis = bestandsmenge * preis_pro_einheit
                     WHERE product_id = %s
                 ''', (product_id,))

                 # Änderung in der Datenbank speichern
                 conn.commit()
                 conn.close()
                 st.success(f"Die Buchungsnummer {booking_id} wurde erfolgreich geändert!")
             else:
                 st.error(f"Die Buchungsnummer {booking_id} wurde nicht geändert! Der Bestand der Produktnummer {product_id} würde durch die Änderung negativ werden: {new_bestand}. Bitte prüfen!")
                 conn.rollback() # Änderung rückgängig machen
         else:
             # Falls keine Änderung der Menge vorgenommen wurde, wird die Buchung ohne Bestandsprüfung gespeichert
             conn.commit()
             conn.close()
             st.success(f"Die Buchungsnummer {booking_id} wurde erfolgreich geändert! Der Bestand blieb unverändert.")
             
     except Exception as e:
         # Fehlerbehandlung und Rollback bei Problemen
         st.error(f"Leider ist ein Fehler aufgetreten: {e}")
         conn.rollback()

     finally:
         # Verbindung schließen
         conn.close()
        
# Funktion Produkt löschen
def delete_product(product_id):
    conn = get_db_connection()
    c = conn.cursor()

    # Überprüfen, ob das Produkt existiert
    c.execute("SELECT product_id FROM products WHERE product_id = %s", (product_id,))
    product = c.fetchone()
    
    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return
    
    # Lösche das Produkt aus der Tabelle "products"
    c.execute('''
        DELETE FROM products WHERE product_id = %s
    ''', (product_id,))
    
    # Lösche alle zugehörigen Buchungen aus der Tabelle "bookings"
    c.execute('''
        DELETE FROM bookings WHERE product_id = %s
    ''', (product_id,))

    conn.commit()
    conn.close()
    st.success(f"Die Produktnummer {product_id} wurde erfolgreich gelöscht!")

# Funktion Buchung löschen
def delete_booking(booking_id):
    conn = get_db_connection()
    c = conn.cursor()

    # Prüfen, ob die Buchung existiert und relevante Daten abrufen
    c.execute('''
        SELECT product_id, menge, booking_art FROM bookings WHERE booking_id = %s
    ''', (booking_id,))
    booking = c.fetchone()

    if not booking:
        conn.close()
        st.error(f"Die Buchungsnummer {booking_id} existiert nicht!")
        return
    
    if booking:
        product_id, menge, booking_art = booking
    
        # Bestand anpassen: Wenn es sich um einen Wareneingang handelt, verringern, sonst erhöhen
        if booking_art == 'Wareneingang':
            c.execute('''
                UPDATE products
                SET bestandsmenge = bestandsmenge - %s
                WHERE product_id = %s
            ''', (menge, product_id))
        else:  # Warenausgang rückgängig machen
            c.execute('''
                UPDATE products
                SET bestandsmenge = bestandsmenge + %s
                WHERE product_id = %s
            ''', (menge, product_id))

    # Gesamtpreis aktualisieren
        c.execute('''
            UPDATE products
            SET gesamtpreis = bestandsmenge * preis_pro_einheit
        ''')

    # Buchung löschen
    c.execute('''
        DELETE FROM bookings WHERE booking_id = %s
    ''', (booking_id,))

    conn.commit()
    conn.close()
    st.success(f"Die Buchungsnummer {booking_id} wurde erfolgreich gelöscht!")

# Funktion Grafik mit monatlichen Konsum und Käufen erstellen
def plot_bar_chart():
    conn = get_db_connection()
    c = conn.cursor()

    query = '''
    SELECT TO_CHAR(buchungsdatum, 'YYYY-MM') AS Monat_Jahr, 
           SUM(CASE WHEN buchungstyp = 'Konsum' THEN menge ELSE 0 END) AS Konsum, 
           SUM(CASE WHEN buchungstyp = 'Kauf' THEN menge ELSE 0 END) AS Kauf
    FROM bookings
    GROUP BY Monat_Jahr 
    ORDER BY Monat_Jahr DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    df.columns = ["Monat_Jahr", "Konsum", "Kauf"]
    
    # Umwandlung von 'Monat_Jahr' in ein datetime Format
    df['Monat_Jahr'] = pd.to_datetime(df['Monat_Jahr'], format='%Y-%m')

    conn.close()
   
    # Create figure and axes for plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Slight offset for the second set of bars
    position_a = list(range(len(df['Monat_Jahr'])))
    position_b = [pos + 0.2 for pos in position_a]  
    
    # Plotting the bars with slight offsets to avoid overlap
    bars_konsum = ax.bar(position_a, df['Konsum'], width=0.2, color='darkorange', label='Konsum')
    bars_kauf = ax.bar(position_b, df['Kauf'], width=0.2, color='darkgreen', label='Kauf')

    # Werte über den Balken anzeigen
    for bar in bars_konsum:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{bar.get_height():.0f}', 
                ha='center', va='bottom', fontsize=10, color='black')

    for bar in bars_kauf:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{bar.get_height():.0f}', 
                ha='center', va='bottom', fontsize=10, color='black')
    
    # Formatting x-axis and adding labels
    ax.set_xlabel('Monat & Jahr')
    ax.set_ylabel('Menge')
    ax.legend()
    
    # Set the x-axis major formatter to show month and year (e.g., Jan 2025, Feb 2025)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    
    # Define the tick positions (use the positions based on the data)
    ax.set_xticks(position_a)  # Set the ticks to the positions corresponding to the months
    ax.set_xticklabels(df['Monat_Jahr'].dt.strftime('%b %Y'))  # Format x-tick labels as 'Jan 2025', 'Feb 2025', etc.
    
    # Rotate and format x-ticks to avoid overlap
    plt.xticks(rotation=45)
    plt.tight_layout()  # Ensures there's no clipping of labels
    
    # Display the plot in Streamlit
    st.pyplot(fig)

# Funktion Bestand & Gesamtpreis pro Lagerort
def show_inventory_per_location():
    conn = get_db_connection()
    c = conn.cursor()

    query = """
    SELECT lagerort AS LAGERORT, SUM(bestandsmenge) AS BESTANDSMENGE, SUM(gesamtpreis) AS GESAMTWERT, 'EUR' AS WÄHRUNG
    FROM products
    GROUP BY LAGERORT, WÄHRUNG
    """

    df = pd.read_sql(query, conn)
    df.columns = ["LAGERORT", "BESTANDSMENGE", "GESAMTWERT", "WÄHRUNG"]

    # Bestandsmenge auf Ganzzahlen (keine Dezimalstellen)
    df['BESTANDSMENGE'] = df['BESTANDSMENGE'].astype(int)

    # Gesamtwert sicher auf 2 Dezimalstellen runden und als float behandeln
    df['GESAMTWERT'] = df['GESAMTWERT'].apply(lambda x: round(float(x), 2))

    # Gesamtsumme berechnen und auch auf 2 Dezimalstellen runden
    total_quantity = df['BESTANDSMENGE'].sum()
    total_price = round(df['GESAMTWERT'].sum(), 2)
    total_währung = 'EUR'
 
    # Gesamtsumme als neue Zeile hinzufügen
    df_total = pd.DataFrame({'BESTANDSMENGE': [total_quantity], 'GESAMTWERT': [total_price], 'WÄHRUNG': [total_währung]})

    conn.close()

    if df.empty:
        st.write("Es sind keine Produkte vorhanden.")
    else:
        # Manuelle Formatierung in HTML für die Anzeige von 2 Dezimalstellen
        df['GESAMTWERT'] = df['GESAMTWERT'].apply(lambda x: f"{x:.2f}")
        df_total['GESAMTWERT'] = df_total['GESAMTWERT'].apply(lambda x: f"{x:.2f}")

        st.header("Bestand pro Lagerort")
        st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)  # index=False entfernt den Index
        st.header("Gesamtübersicht")
        st.markdown(df_total.to_html(escape=False, index=False), unsafe_allow_html=True)  # index=False entfernt den Index

############# Frontend Streamlit
def main():
    # Get the current timestamp
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    # Display the current timestamp in Streamlit
    st.title("Weinlager Carla & Steffen")

    # Create Databank
    create_db()

    # Sidebar Login
    st.sidebar.header("Login 🔑")
    if not st.session_state["authenticated"]:
        username = st.sidebar.text_input("Benutzername")
        password = st.sidebar.text_input("Passwort", type="password")
        if st.sidebar.button("Login"):
            login(username, password)
    else:
        st.sidebar.write(f"Angemeldet als **{st.session_state['username']}**")
        if st.sidebar.button("Logout"):
            logout()
            st.rerun()

    if st.session_state["authenticated"]:
         st.sidebar.markdown("<h3>Was möchtest du tun? 🪄</h3>", unsafe_allow_html=True)
         action = st.sidebar.selectbox("", [
             'Gesamtübersicht anzeigen', 'Bestand anzeigen', 'Buchung erfassen', 'Buchung ändern', 'Buchung anzeigen',
             'Buchung löschen', 'Produkt anlegen', 'Produkt ändern', 'Produkt anzeigen', 'Produkt löschen', 'Inventur anzeigen'
         ], index=None)

        # Das Bild nur anzeigen, wenn keine Aktion gewählt wurde
         if action is None:
             st.image("weinbild.jpg", caption="Willkommen im Weinlager 🍷", use_container_width=False)
             st.write(f"{formatted_timestamp}")
         else:
             st.write(f"{formatted_timestamp}")  # Zeige nur den Timestamp, falls eine Aktion gewählt wurde

         if action == 'Produkt anlegen':               
             st.header("Produkt anlegen")
             weingut = st.text_input("Weingut")
             rebsorte = st.text_input("Rebsorte")
             lage = st.text_input("Lage")
             land = st.text_input("Land")
             jahrgang = st.text_input("Jahrgang")
             lagerort = st.text_input("Lagerort")
             preis_pro_einheit = st.number_input("Preis pro Einheit")
             alko = st.text_input("Alkohol")
             zucker = st.text_input("Restzucker")
             saure = st.text_input("Säure")
             info = st.text_input("Weitere Infos")
             kauf_link = st.text_input("Link zur Bestellung")
             comments = st.text_input("Bemerkungen")
    
             if st.button("Produkt anlegen"):
                 register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments)
      
         elif action == 'Produkt ändern':
             st.header("Produkt ändern")

             # Initialisieren von `selected_product_id` als None
             selected_product_id = None

             product_id = st.number_input("Produktnummer", min_value=0, step=1)

             # Eingabe zur Produktsuche
             search_term = st.text_input("Suchbegriff (z.B. Weingut, Rebsorte, Lage)", "")

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             product_details = None
             
             # Überprüfung, ob eine Suche oder Produktnummer eingegeben wurde
             if search_term:  # Wenn ein Suchbegriff eingegeben wurde
                 query = '''
                     SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE weingut ILIKE %s OR rebsorte ILIKE %s OR lage ILIKE %s
                     ORDER BY 1,3,4,7
                 '''
                 search_results = pd.read_sql(query, conn, params=(f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

                 if not search_results.empty:
                    # Kombinierte Anzeige der Produktinformationen in der selectbox
                    product_display = search_results.apply(
                         lambda row: f"ID: {row['product_id']} | {row['weingut']} | {row['rebsorte']} | {row['lage']} | {row['land']} | {row['jahrgang']} | {row['lagerort']}", 
                         axis=1
                    )

                    # Benutzer kann ein Produkt anhand der kombinierten Anzeige auswählen
                    selected_product_info = st.selectbox(
                         "Suchergebnis", 
                         ["Produkt auswählen"] + product_display.tolist(),
                         index=0
                    )

                    if selected_product_info != "Produkt auswählen":
                         # Produkt-ID extrahieren
                         selected_product_id = int(selected_product_info.split(" | ")[0].replace("ID: ", "").strip())
                    else:
                         selected_product_id = None
                 else:
                    st.warning("Keine Produkte gefunden, die dem Suchbegriff entsprechen.")

             # Wenn eine Produktnummer direkt eingegeben wird, dann setzen wir `selected_product_id`    
             if product_id > 0 and not selected_product_id:
                 selected_product_id = product_id

             # Wenn eine Produkt-ID direkt eingegeben wurde, Produktdetails anzeigen
             if selected_product_id is not None and selected_product_id > 0:
                 query = '''
                    SELECT weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments
                    FROM products
                    WHERE product_id = %s
                 '''
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(selected_product_id,))

                 if not product_details.empty:
                     # Wenn Produktdetails gefunden wurden, diese anzeigen
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "EINZELPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
                     
                     new_weingut = st.text_input("Weingut", value=product_details["WEINGUT"].iloc[0] if product_details is not None else "")
                     new_rebsorte = st.text_input("Rebsorte", value=product_details["REBSORTE"].iloc[0] if product_details is not None else "")
                     new_lage = st.text_input("Lage", value=product_details["LAGE"].iloc[0] if product_details is not None else "")
                     new_land = st.text_input("Land", value=product_details["LAND"].iloc[0] if product_details is not None else "")
                     new_jahrgang = st.text_input("Jahrgang", value=product_details["JAHRGANG"].iloc[0] if product_details is not None else "")
                     new_lagerort = st.text_input("Lagerort", value=product_details["LAGERORT"].iloc[0] if product_details is not None else "")
                     new_preis_pro_einheit = st.number_input("Preis pro Einheit", value=product_details["EINZELPREIS"].iloc[0] if product_details is not None else 0.0)
                     new_alko = st.text_input("Alkohol", value=product_details["ALKOHOL"].iloc[0] if product_details is not None else "")
                     new_zucker = st.text_input("Restzucker", value=product_details["RESTZUCKER"].iloc[0] if product_details is not None else "")
                     new_saure = st.text_input("Säure", value=product_details["SÄURE"].iloc[0] if product_details is not None else "")
                     new_info = st.text_input("Weitere Infos", value=product_details["WEITERE_INFOS"].iloc[0] if product_details is not None else "")
                     new_kauf_link = st.text_input("Link zur Bestellung", value=product_details["LINK_ZUR_BESTELLUNG"].iloc[0] if product_details is not None else "")
                     new_comments = st.text_input("Bemerkungen", value=product_details["BEMERKUNGEN"].iloc[0] if product_details is not None else "")
         
                 else:
                     st.warning("Bitte die Produktnummer prüfen!")

             if st.button("Produkt ändern"):
                 if not product_details.empty:
                     adjust_product(selected_product_id, new_weingut, new_rebsorte, new_lage, new_land, new_jahrgang, 
                                    new_lagerort, new_preis_pro_einheit, new_alko, new_zucker, new_saure, 
                                    new_info, new_kauf_link, new_comments)
                 else:
                     st.error (f"Die Produktnummer {selected_product_id} existiert nicht!")

             conn.close()


         elif action == 'Buchung erfassen':
             st.header("Buchung erfassen")

             # Initialisieren von `selected_product_id` als None
             selected_product_id = None

             product_id = st.number_input("Produktnummer",min_value=0)

             # Eingabe zur Produktsuche (Optional: auch nach anderen Kriterien wie Weingut, Rebsorte, etc.)
             search_term = st.text_input("Suchbegriff (z.B. Weingut, Rebsorte, Lage)", "")

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()

             selected_product_id = product_id
             
             if search_term:  # Wenn ein Suchbegriff eingegeben wurde
                 query = '''
                     SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE weingut ILIKE %s OR rebsorte ILIKE %s OR lage ILIKE %s
                     ORDER BY 3,4,7
                 '''
                 # SQL-Abfrage ausführen
                 search_results = pd.read_sql(query, conn, params=(f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

                 if not search_results.empty:
                     #Kombinierte Anzeige der Produktinformationen in der selectbox
                     product_display = search_results.apply(
                         lambda row: f"ID: {row['product_id']} | {row['weingut']} | {row['rebsorte']} | {row['lage']} | {row['land']} | {row['jahrgang']} | {row['lagerort']}", 
                     axis=1
                     )
            
                     # Füge eine Option für "Bitte auswählen" hinzu
                     product_display = ["Produkt auswählen"] + product_display.tolist()
            
                     # Benutzer kann nun ein Produkt anhand der kombinierten Anzeige auswählen
                     selected_product_info = st.selectbox(
                         "Suchergebnis", 
                         product_display, 
                         index=0 
                     )

                     # Die Produkt-ID aus der ausgewählten Anzeige extrahieren
                     if selected_product_info != "Produkt auswählen":
                         selected_product_id = int(selected_product_info.split(" | ")[0].replace("ID: ", "").strip())
                     else:
                         selected_product_id = None
                 else:
                     st.warning("Keine Produkte gefunden, die dem Suchbegriff entsprechen.")
             
             
             # Wenn eine Produkt-ID ausgewählt wurde, Produktdetails anzeigen
             if selected_product_id is not None and selected_product_id > 0 and not search_term:
                 query = '''
                     SELECT weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE product_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(selected_product_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not product_details.empty:
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 elif product_details.empty: 
                     st.warning("Bitte die Produktnummer prüfen!")
                 
             buchungsdatum = st.date_input("Buchungsdatum")
             menge = st.number_input("Menge", min_value=1)
             buchungstyp = st.selectbox("Buchungsart", ["Kauf", "Konsum", "Geschenk", "Entsorgung", "Umlagerung", "Inventur", "Andere"], index=None)
             comments = st.text_input("Bemerkungen")
             booking_art = st.radio("Buchungstyp",('Wareneingang', 'Warenausgang'), index=None)
    
             if st.button("Buchung erfassen"):
                 if selected_product_id is not None and selected_product_id > 0:
                     if booking_art == 'Wareneingang':
                         record_incoming_booking(selected_product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
                     if booking_art == 'Warenausgang':
                         record_outgoing_booking(selected_product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
                 else:
                     st.error(f"Die Produktnummer {selected_product_id} existiert nicht!")
             conn.close()

         elif action == 'Produkt anzeigen':
             st.header("Produkte")
             conn = get_db_connection()
             query = '''
                SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments
                FROM products
                ORDER BY 2,3,4,5,6
                '''
             df = pd.read_sql(query, conn)
             df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "EINZELPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
             conn.close()

             # Ersetzen von None durch leere Strings
             df = df.fillna('')

             # Preise auf 2 Dezimalstellen runden
             df['EINZELPREIS'] = df['EINZELPREIS'].round(2)

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             styled_df = df.style.applymap(highlight, subset=["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"])

             # Formatierung der Preise auf 2 Dezimalstellen für die Anzeige
             styled_df = styled_df.format({"EINZELPREIS": "{:.2f}"})

             st.dataframe(styled_df)

         elif action == 'Bestand anzeigen':
             st.header("Bestand")
             conn = get_db_connection()
             query = '''
                SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandsmenge, preis_pro_einheit, gesamtpreis, alko, zucker, saure, info, kauf_link, comments
                FROM products
                WHERE bestandsmenge <> '0'
                ORDER BY 2,3,4,5,6
                '''
             df = pd.read_sql(query, conn)
             df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
             conn.close()

             # Ersetzen von None durch leere Strings
             df = df.fillna('')

             # Preise auf 2 Dezimalstellen runden
             df['EINZELPREIS'] = df['EINZELPREIS'].round(2)
             df['GESAMTPREIS'] = df['GESAMTPREIS'].round(2)

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             styled_df = df.style.applymap(highlight, subset=["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"])

             # Formatierung der Preise auf 2 Dezimalstellen für die Anzeige
             styled_df = styled_df.format({"EINZELPREIS": "{:.2f}", "GESAMTPREIS": "{:.2f}"})

             st.dataframe(styled_df)
 
         elif action == 'Inventur anzeigen':
             st.header("Inventur")
             conn = get_db_connection()
             query = '''
                    SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandsmenge, preis_pro_einheit, gesamtpreis, alko, zucker, saure, info, kauf_link, comments
                    FROM products
                    ORDER BY 2,3,4,5,6
                    '''
             df = pd.read_sql(query, conn)
             df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
             conn.close()

             # Ersetzen von None durch leere Strings
             df = df.fillna('')
             
             # Preise auf 2 Dezimalstellen runden
             df['EINZELPREIS'] = df['EINZELPREIS'].round(2)
             df['GESAMTPREIS'] = df['GESAMTPREIS'].round(2)

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             styled_df = df.style.applymap(highlight, subset=["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"])

             # Formatierung der Preise auf 2 Dezimalstellen für die Anzeige
             styled_df = styled_df.format({"EINZELPREIS": "{:.2f}", "GESAMTPREIS": "{:.2f}"})

             st.dataframe(styled_df)
                 
             # Konvertiere kauf_link zu einem anklickbaren HTML-Link
             #df['LINK_ZUR_BESTELLUNG'] = df['LINK_ZUR_BESTELLUNG'].apply(lambda x: f'<a href="{x}" target="_blank">{x}</a>')
             #conn.close()
             # Ausgabe der Tabelle mit HTML-Links
             #st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
            
         elif action == 'Buchung anzeigen':
             st.header("Buchungen")
             conn = get_db_connection()
             query = '''
                   SELECT a.booking_id, a.booking_art, a.buchungstyp, a.buchungsdatum, a.menge, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.comments 
                   FROM bookings a 
                   LEFT OUTER JOIN products b 
                   ON a.product_id = b.product_id
                   ORDER BY 4,1
                   '''
             df = pd.read_sql(query, conn)
             df.columns = ["BUCHUNGSNR", "BUCHUNGSTYP", "BUCHUNGSART", "BUCHUNGSDATUM", "MENGE", "PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BEMERKUNGEN"]
             conn.close()
             
             # Ersetzen von None durch leere Strings
             df = df.fillna('')

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             styled_df = df.style.applymap(highlight, subset=["BUCHUNGSNR", "BUCHUNGSTYP", "BUCHUNGSART", "BUCHUNGSDATUM", "MENGE", "PRODUKTNR"])
             st.dataframe(styled_df)

         elif action == 'Produkt löschen':
             st.header("Produkt löschen")

             # Initialisieren von `selected_product_id` als None
             selected_product_id = None

             product_id = st.number_input("Produktnummer", min_value=0)

             # Eingabe zur Produktsuche
             search_term = st.text_input("Suchbegriff (z.B. Weingut, Rebsorte, Lage)", "")

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()

             # Überprüfung, ob eine Suche oder Produktnummer eingegeben wurde
             if search_term:  # Wenn ein Suchbegriff eingegeben wurde
                 query = '''
                     SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE weingut ILIKE %s OR rebsorte ILIKE %s OR lage ILIKE %s
                     ORDER BY 1,3,4,7
                 '''
                 search_results = pd.read_sql(query, conn, params=(f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

                 if not search_results.empty:
                     # Kombinierte Anzeige der Produktinformationen in der selectbox
                     product_display = search_results.apply(
                         lambda row: f"ID: {row['product_id']} | {row['weingut']} | {row['rebsorte']} | {row['lage']} | {row['land']} | {row['jahrgang']} | {row['lagerort']}", 
                         axis=1
                     )

                     # Benutzer kann ein Produkt anhand der kombinierten Anzeige auswählen
                     selected_product_info = st.selectbox(
                         "Suchergebnis", 
                         ["Produkt auswählen"] + product_display.tolist(),
                         index=0
                     )

                     if selected_product_info != "Produkt auswählen":
                         # Produkt-ID extrahieren
                         selected_product_id = int(selected_product_info.split(" | ")[0].replace("ID: ", "").strip())
                     else:
                         selected_product_id = None
                 else:
                    st.warning("Keine Produkte gefunden, die dem Suchbegriff entsprechen.")

             # Wenn eine Produktnummer direkt eingegeben wird, dann setzen wir `selected_product_id`    
             if product_id > 0 and not selected_product_id:
                     selected_product_id = product_id

             # Wenn eine Produkt-ID direkt eingegeben wurde, Produktdetails anzeigen
             if selected_product_id is not None and selected_product_id > 0 and not search_term:
                 query = '''
                     SELECT weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE product_id = %s
                     '''
                 
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(selected_product_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not product_details.empty:
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Produktnummer prüfen!")
    
             if st.button("Produkt löschen"):
                     delete_product(selected_product_id)

             conn.close()
            
         elif action == 'Buchung löschen':
             st.header("Buchung löschen")

             # Initialisieren von `selected_booking_id` als None
             selected_booking_id = None

             # Buchungsnummer Eingabe
             booking_id = st.number_input("Buchungsnummer", min_value=0)

             # Eingabe zur Buchungssuche (optional, z.B. nach Produkt oder Buchungsart)
             search_term = st.text_input("Suchbegriff (z.B. Weingut, Lage, Buchungstyp)", "")

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()

             # Überprüfung, ob eine Suche oder Buchungsnummer eingegeben wurde
             if search_term:  # Wenn ein Suchbegriff eingegeben wurde
                 query = '''
                     SELECT a.booking_id, a.booking_art, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.menge, a.buchungstyp, a.buchungsdatum
                     FROM bookings a
                     LEFT OUTER JOIN products b ON a.product_id = b.product_id
                     WHERE b.weingut ILIKE %s OR b.lage ILIKE %s OR a.buchungstyp ILIKE %s
                     ORDER BY a.booking_id
                    '''
                 search_results = pd.read_sql(query, conn, params=(f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

                 if not search_results.empty:
                     # Kombinierte Anzeige der Buchungsinformationen in der selectbox
                     booking_display = search_results.apply(
                         lambda row: f"ID: {row['booking_id']} | {row['booking_art']} | {row['buchungsdatum']} | {row['menge']} | {row['buchungstyp']} | {row['weingut']} | {row['lage']}",
                         axis=1
                     )

                     # Füge eine Option für "Bitte auswählen" hinzu
                     booking_display = ["Buchung auswählen"] + booking_display.tolist()

                     # Benutzer kann nun eine Buchung anhand der kombinierten Anzeige auswählen
                     selected_booking_info = st.selectbox(
                         "Suchergebnis", 
                         booking_display,
                         index=0
                     )
                     
                     # Buchungs-ID extrahieren
                     if selected_booking_info != "Buchung auswählen":
                         selected_booking_id = int(selected_booking_info.split(" | ")[0].replace("ID: ", "").strip())
                     else:
                         selected_booking_id = None
                 else:
                     st.warning("Keine Buchungen gefunden, die dem Suchbegriff entsprechen.")

             # Wenn eine Buchungsnummer direkt eingegeben wird, dann setzen wir `selected_booking_id`
             if booking_id > 0 and selected_booking_id is None:
                 selected_booking_id = booking_id
            
             # Wenn eine Buchungs-ID direkt eingegeben wurde, Buchungsdetails anzeigen
             if selected_booking_id is not None and selected_booking_id > 0 and not search_term:
                 query = '''
                     SELECT a.booking_art, a.buchungstyp, a.buchungsdatum, a.menge, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort 
                     FROM bookings a 
                     LEFT OUTER JOIN products b 
                     ON a.product_id = b.product_id
                     WHERE a.booking_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 booking_details = pd.read_sql(query, conn, params=(selected_booking_id,))
        
                 # Wenn Buchungsdetails gefunden wurden, diese anzeigen
                 if not booking_details.empty:
                     booking_details.columns = ["BUCHUNGSART", "BUCHUNGSTYP", "BUCHUNGSDATUM", "MENGE", "PRODUKTNUMMER", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Buchungsdetails')
                     st.dataframe(booking_details)
                 else:
                     st.warning("Bitte die Buchungsnummer prüfen!")

             if st.button("Buchung löschen"):
                 if selected_booking_id is not None and selected_booking_id > 0:
                     delete_booking(selected_booking_id)
             
             conn.close()
         
         elif action == 'Buchung ändern':
             st.header("Buchung ändern")

             # Initialisieren von `selected_booking_id` als None
             selected_booking_id = None

             # Auswahl der zu bearbeitenden Buchung
             booking_id = st.number_input("Buchungsnummer", min_value=0)

             # Eingabe zur Buchungssuche (optional, z.B. nach Produkt oder Buchungsart)
             search_term = st.text_input("Suchbegriff (z.B. Weingut, Lage, Buchungstyp)", "")

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             buchungsdatum = None
             menge = None
             buchungstyp = None
             comments = None
             booking_art = None

             selected_booking_id = booking_id

             # Überprüfung, ob eine Suche oder Buchungsnummer eingegeben wurde
             if search_term:  # Wenn ein Suchbegriff eingegeben wurde
                 query = '''
                     SELECT a.booking_id, a.booking_art, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.menge, a.buchungstyp, a.buchungsdatum
                     FROM bookings a
                     LEFT OUTER JOIN products b ON a.product_id = b.product_id
                     WHERE b.weingut ILIKE %s OR b.lage ILIKE %s OR a.buchungstyp ILIKE %s
                     ORDER BY a.booking_id
                    '''
                 search_results = pd.read_sql(query, conn, params=(f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))

                 if not search_results.empty:
                     # Kombinierte Anzeige der Buchungsinformationen in der selectbox
                     booking_display = search_results.apply(
                         lambda row: f"ID: {row['booking_id']} | {row['booking_art']} | {row['buchungsdatum']} | {row['menge']} |{row['buchungstyp']} | {row['weingut']} | {row['lage']}",
                         axis=1
                     )

                     # Füge eine Option für "Bitte auswählen" hinzu
                     booking_display = ["Buchung auswählen"] + booking_display.tolist()

                     # Benutzer kann nun eine Buchung anhand der kombinierten Anzeige auswählen
                     selected_booking_info = st.selectbox(
                         "Suchergebnis", 
                         booking_display,
                         index=0
                     )
                     
                     # Buchungs-ID extrahieren
                     if selected_booking_info != "Buchung auswählen":
                         selected_booking_id = int(selected_booking_info.split(" | ")[0].replace("ID: ", "").strip())
                     else:
                         selected_booking_id = None
                 else:
                     st.warning("Keine Buchungen gefunden, die dem Suchbegriff entsprechen.")

             # Wenn eine Buchungsnummer direkt eingegeben wird, dann setzen wir `selected_booking_id`
             if booking_id > 0 and selected_booking_id is None:
                 selected_booking_id 
             
             # Wenn eine Buchungs-ID direkt eingegeben wurde, Buchungsdetails anzeigen
             if selected_booking_id is not None and selected_booking_id > 0:
                 query = '''
                     SELECT b.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort
                     FROM bookings a 
                     LEFT OUTER JOIN products b 
                     ON a.product_id = b.product_id
                     WHERE a.booking_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(selected_booking_id,))

                 # Abfrage für die Buchungsdetails basierend auf der Buchung-ID
                 query = '''
                     SELECT booking_art, menge, buchungstyp, buchungsdatum, comments
                     FROM bookings 
                     WHERE booking_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 booking_details = pd.read_sql(query, conn, params=(selected_booking_id,))
        
                 # Wenn Produkdetails & Buchungsdetails gefunden wurden
                 if not product_details.empty:
                     product_details.columns = ["PRODUKTNUMMER", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     product_details = product_details.fillna('')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Buchungsnummer prüfen!")

                 if not booking_details.empty:
                     booking_details.columns = ["BUCHUNGSTYP", "MENGE", "BUCHUNGSART", "BUCHUNGSDATUM", "BEMERKUNGEN"]
                     # Buchungsdaten aus der Tabelle extrahieren
                     buchungsdatum = booking_details['BUCHUNGSDATUM'].iloc[0]
                     menge = booking_details['MENGE'].iloc[0]
                     buchungstyp = booking_details['BUCHUNGSART'].iloc[0]
                     comments = booking_details['BEMERKUNGEN'].iloc[0]
                     booking_art = booking_details['BUCHUNGSTYP'].iloc[0]
         
                     new_buchungsdatum = st.date_input("Buchungsdatum", value=buchungsdatum if buchungsdatum is not None else None, key="buchungsdatum_input")
                     new_menge = st.number_input("Menge", min_value=0, value=menge if menge is not None else 0, key="menge_input")
                     new_buchungstyp = st.selectbox("Buchungsart", ["Kauf", "Konsum", "Geschenk", "Entsorgung", "Umlagerung", "Inventur", "Andere"],
                                                    index=["Kauf", "Konsum", "Geschenk", "Entsorgung", "Umlagerung", "Inventur", "Andere"].index(buchungstyp) if buchungstyp is not None else 0,key="buchungstyp_input")
                     new_comments = st.text_input("Bemerkungen", value=comments if comments is not None else "", key="comments_input")
                     new_booking_art = st.radio("Buchungstyp", ('Wareneingang', 'Warenausgang'), index=0 if booking_art == "Wareneingang" else 1 if booking_art == "Warenausgang" else 0, key="booking_art_input")

             if st.button("Buchung ändern"):
                if not booking_details.empty:
                     if new_booking_art != booking_art or new_buchungstyp != buchungstyp or new_menge != menge or new_comments != comments or new_buchungsdatum != buchungsdatum:
                        adjust_booking(selected_booking_id, new_menge, new_buchungstyp, new_booking_art, new_buchungsdatum, new_comments)
                     else:
                       st.warning("Keine Änderungen vorgenommen.")
                else:
                     st.error(f"Die Buchungsnummer {selected_booking_id} existiert nicht!")

             conn.close()

         elif action == 'Gesamtübersicht anzeigen':
             show_inventory_per_location()
             st.text ("")
             plot_bar_chart()
             
         else:
             st.text("") 

# Main-Funktion aufrufen
if __name__ == "__main__":
    main()