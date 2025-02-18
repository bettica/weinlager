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

# Funktion Produkt registrieren
def register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments):
    gesamtpreis = 0
    conn = get_db_connection()
    c = conn.cursor()

    c.execute('''
        INSERT INTO products (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments, gesamtpreis)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, alko, zucker, saure, info, kauf_link, comments, gesamtpreis))
        
    conn.commit()
    conn.close()

# Funktion Produkt anpassen
def update_product(product_id, **kwargs):
    if not kwargs:
        return  # Falls keine Änderungen angegeben sind, wird nichts aktualisiert

    conn = get_db_connection()
    c = conn.cursor()

    # Aktuelle Daten abrufen
    c.execute("SELECT * FROM products WHERE product_id = %s", (product_id,))
    product = c.fetchone()
    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return 

    # Spaltennamen abrufen
    c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'products'")
    columns = [col[0] for col in c.fetchall()]

    # Nur die Felder aktualisieren, die angegeben wurden
    update_fields = [f"{key} = %s" for key in kwargs.keys() if key in columns]
    values = list(kwargs.values()) + [product_id]

    query = f"UPDATE products SET {', '.join(update_fields)} WHERE product_id = %s"
    c.execute(query, values)

    conn.commit()
    conn.close()
    st.success(f"Die Produktnummer {product_id} wurde erfolgreich geändert!")

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
    st.success("Die Wareneingang wurde erfolgreich gebucht!")

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
    st.success("Die Warenausgang wurde erfolgreich gebucht!")

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
             'Gesamtübersicht anzeigen', 'Bestand anzeigen', 'Buchung erfassen', 'Buchung anzeigen',
             'Buchung löschen', 'Produkt anlegen', 'Produkt ändern', 'Produkt löschen', 'Inventur anzeigen'
         ], index=None)

        # Das Bild nur anzeigen, wenn keine Aktion gewählt wurde
         if action is None:
             st.image("weinbild.jpg", caption="Willkommen im Weinlager 🍷", use_container_width=False)
             st.write(f"{formatted_timestamp}")
         else:
             st.write(f"{formatted_timestamp}")  # Zeige nur den Timestamp, falls eine Aktion gewählt wurde

         if action == 'Produkt anlegen':               
             #Produktregistrierung
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
                 st.success("Das Produkt wurde erfolgreich angelegt!")

         elif action == 'Produkt ändern':
             st.header("Produkt ändern")
             product_id = st.number_input("Produktnummer", min_value=0, step=1)

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             if product_id > 0:
                 # Abfrage für die Produktdetails basierend auf der Produkt-ID
                 query = '''
                     SELECT weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE product_id = %s
                     '''
                 
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(product_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not product_details.empty:
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Produktnummer prüfen!")

             # Eingabefelder für mögliche Änderungen
             weingut = st.text_input("Weingut", value="")
             rebsorte = st.text_input("Rebsorte", value="")
             lage = st.text_input("Lage", value="")
             land = st.text_input("Land", value="")
             jahrgang = st.text_input("Jahrgang", value="")
             lagerort = st.text_input("Lagerort", value="")
             preis_pro_einheit = st.number_input("Preis pro Einheit", value=0.0)
             alko = st.text_input("Alkohol", value="")
             zucker = st.text_input("Restzucker", value="")
             saure = st.text_input("Säure", value="")
             info = st.text_input("Weitere Infos", value="")
             kauf_link = st.text_input("Link zur Bestellung", value="")
             comments = st.text_input("Bemerkungen", value="")

             if st.button("Produkt ändern"):
                 update_data = {key: value for key, value in {
                     "weingut": weingut,
                     "rebsorte": rebsorte,
                     "lage": lage,
                     "land": land,
                     "jahrgang": jahrgang,
                     "lagerort": lagerort,
                     "preis_pro_einheit": preis_pro_einheit,
                     "alko": alko,
                     "zucker": zucker,
                     "saure": saure,
                     "info": info,
                     "kauf_link": kauf_link,
                     "comments": comments
                 }.items() if value}  # Nur nicht-leere Werte speichern

                 if update_data:
                     result = update_product(product_id, **update_data)
                 else:
                     st.warning("Keine Änderungen vorgenommen.")  
             conn.close()    
        
         elif action == 'Buchung erfassen':
             st.header("Buchung erfassen")
             product_id = st.number_input("Produktnummer",min_value=0)

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             if product_id > 0:
                 # Abfrage für die Produktdetails basierend auf der Produkt-ID
                 query = '''
                     SELECT weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE product_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(product_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not product_details.empty:
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Produktnummer prüfen!")

             buchungsdatum = st.date_input("Buchungsdatum")
             menge = st.number_input("Menge", min_value=1)
             buchungstyp = st.selectbox("Buchungsart", ["Kauf", "Konsum", "Geschenk", "Entsorgung", "Umlagerung", "Inventur", "Andere"], index=None)
             comments = st.text_input("Bemerkungen")
             booking_art = st.radio("Buchungstyp",('Wareneingang', 'Warenausgang'), index=None)
    
             if st.button("Buchung erfassen"):
                 if booking_art == 'Wareneingang':
                     record_incoming_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
                 if booking_art == 'Warenausgang':
                     record_outgoing_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
             conn.close()

         elif action == 'Bestand anzeigen':
             st.header("Bestand")
             conn = get_db_connection()
             query = '''
                SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandsmenge, preis_pro_einheit, gesamtpreis, alko, zucker, saure, info, kauf_link, comments
                FROM products
                WHERE bestandsmenge <> '0'
                ORDER BY 2
                '''
             df = pd.read_sql(query, conn)
             df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
             conn.close()

             # Preise auf 2 Dezimalstellen runden
             df['EINZELPREIS'] = df['EINZELPREIS'].round(2)
             df['GESAMTPREIS'] = df['GESAMTPREIS'].round(2)

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             # Stil anwenden und Dataframe anzeigen
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
                    ORDER BY 2
                    '''
             df = pd.read_sql(query, conn)
             df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "ALKOHOL", "RESTZUCKER", "SÄURE", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
             conn.close()
             
             # Preise auf 2 Dezimalstellen runden
             df['EINZELPREIS'] = df['EINZELPREIS'].round(2)
             df['GESAMTPREIS'] = df['GESAMTPREIS'].round(2)

             # Styling anwenden
             def highlight(val):
                 color = 'background-color: #f0f2f6'
                 return color

             # Stil anwenden und Dataframe anzeigen
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
                   SELECT a.booking_id, a.booking_art, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.menge, a.buchungstyp, a.buchungsdatum, a.comments 
                   FROM bookings a 
                   LEFT OUTER JOIN products b 
                   ON a.product_id = b.product_id
                   ORDER BY a.buchungsdatum
                   '''
             df = pd.read_sql(query, conn)
             df.columns = ["BUCHUNGSNR", "BUCHUNGSTYP", "PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "MENGE", "BUCHUNGSART", "BUCHUNGSDATUM", "BEMERKUNGEN"]
             conn.close()
             st.dataframe(df)

         elif action == 'Produkt löschen':
             st.header("Produkt löschen")
             product_id = st.number_input("Produktnummer", min_value=0)

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             if product_id > 0:
                 # Abfrage für die Produktdetails basierend auf der Produkt-ID
                 query = '''
                     SELECT weingut, rebsorte, lage, land, jahrgang, lagerort
                     FROM products
                     WHERE product_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 product_details = pd.read_sql(query, conn, params=(product_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not product_details.empty:
                     product_details.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Produktnummer prüfen!")
    
             if st.button("Produkt löschen"):
                 delete_product(product_id)

             conn.close()
            
         elif action == 'Buchung löschen':
             st.header("Buchung löschen")
             booking_id = st.number_input("Buchungsnummer", min_value=0)

             # Verbindung zur Datenbank herstellen
             conn = get_db_connection()
             if booking_id > 0:
                 # Abfrage für die Buchungsdetails basierend auf der Buchung-ID
                 query = '''
                     SELECT a.booking_id, a.booking_art, a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.menge, a.buchungstyp, a.buchungsdatum
                     FROM bookings a 
                     LEFT OUTER JOIN products b 
                     ON a.product_id = b.product_id
                     WHERE booking_id = %s
                     '''
                 # SQL-Abfrage ausführen
                 booking_details = pd.read_sql(query, conn, params=(booking_id,))
        
                 # Wenn Produktdetails gefunden wurden, diese anzeigen
                 if not booking_details.empty:
                     booking_details.columns = ["BUCHUNGSTYP", "PRODUKTNUMMER", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "MENGE", "BUCHUNGSART", "BUCHUNGSDATUM"]
                     st.caption('Produktdetails')
                     st.dataframe(product_details)
                 else:
                     st.warning("Bitte die Buchungsnummer prüfen!")

             if st.button("Buchung löschen"):
                 delete_booking(booking_id)
             
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