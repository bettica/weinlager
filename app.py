import streamlit as st
import sqlite3
import os
import toml
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Tabelle einrichten
def create_db():
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Tabelle für Produkte erstellen
    c.execute('''CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        weingut TEXT,
        rebsorte TEXT,
        lage TEXT,
        land TEXT,
        jahrgang TEXT,
        lagerort TEXT,
        bestandsmenge INTEGER DEFAULT 0,
        preis_pro_einheit REAL,
        gesamtpreis REAL,      
        zucker TEXT,
        saure TEXT,
        alko TEXT,
        info TEXT,      
        kauf_link TEXT,                        
        comments TEXT              
    )''')

    # Tabelle für Buchungen erstellen
    c.execute('''CREATE TABLE IF NOT EXISTS bookings (
        booking_id INTEGER PRIMARY KEY,
        booking_art TEXT,
        product_id INTEGER,
        buchungsdatum DATE,
        menge INTEGER,
        buchungstyp TEXT,
        comments TEXT,      
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )''')
   
    conn.commit()
    conn.close()

# Zugangsdaten aus GitHub Secrets holen
credentials_toml = os.getenv("USER_CREDENTIALS")

# Fehlerbehandlung für fehlende oder ungültige Zugangsdaten
if credentials_toml is None:
    st.sidebar.error("Fehler: Die Zugangsdaten wurden nicht geladen. Bitte prüfen, ob die Umgebungsvariable 'USER_CREDENTIALS' gesetzt ist.")
    users = {}
else:
    try:
        # TOML-String in ein Dictionary umwandeln
        credentials = toml.loads(credentials_toml)
        users = credentials.get("users", {})
    except toml.TomlDecodeError:
        st.sidebar.error("Fehler: Die Zugangsdaten konnten nicht gelesen werden. Bitte prüfen, ob das TOML-Format korrekt ist.")
        users = {}

# Funktion Produkt registrieren
def register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, zucker, saure, alko, info, kauf_link, comments):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
   
    c.execute('''
        INSERT INTO products (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, zucker, saure, alko, info, kauf_link, comments)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, zucker, saure, alko, info, kauf_link, comments))
    
    conn.commit()
    conn.close()

# Funktion Produkt anpassen
def update_product(product_id, **kwargs):
    if not kwargs:
        return  # Falls keine Änderungen angegeben sind, wird nichts aktualisiert

    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()

    # Aktuelle Daten abrufen
    c.execute("SELECT * FROM products WHERE product_id = ?", (product_id,))
    product = c.fetchone()
    if not product:
        conn.close()
        st.error ("Produkt nicht gefunden!")
        return 

    # Spaltennamen abrufen
    c.execute("PRAGMA table_info(products)")
    columns = [col[1] for col in c.fetchall()]

    # Nur die Felder aktualisieren, die angegeben wurden
    update_fields = [f"{key} = ?" for key in kwargs.keys() if key in columns]
    values = list(kwargs.values()) + [product_id]

    query = f"UPDATE products SET {', '.join(update_fields)} WHERE product_id = ?"
    c.execute(query, values)

    conn.commit()
    conn.close()
    st.success("Produkt erfolgreich geändert!")

# Funktion Wareneingang buchen
def record_incoming_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()

    # Prüfen, ob die Produkt-ID existiert
    c.execute('''SELECT * FROM products WHERE product_id = ?''', (product_id,))
    product = c.fetchone()

    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return

    # Buchung in der Tabelle 'bookings' einfügen
    c.execute(''' 
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))
    
    # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute(''' 
        UPDATE products
        SET bestandsmenge = bestandsmenge + ?   
        WHERE product_id = ? 
    ''', (menge, product_id))
    
    c.execute(''' 
        UPDATE products
        SET gesamtpreis = bestandsmenge * preis_pro_einheit  
    ''')

    conn.commit()
    conn.close()
    st.success("Wareneingang erfolgreich gebucht!")

# Funktion Warenausgang buchen
def record_outgoing_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()

    # Überprüfen, ob das Produkt existiert
    c.execute("SELECT bestandsmenge FROM products WHERE product_id = ?", (product_id,))
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
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))

    # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute('''
        UPDATE products
        SET bestandsmenge = bestandsmenge - ? 
        WHERE product_id = ?
    ''', (menge, product_id))

    c.execute(''' 
        UPDATE products
        SET gesamtpreis = bestandsmenge * preis_pro_einheit
    ''')

    conn.commit()
    conn.close()
    st.success("Warenausgang erfolgreich gebucht!")

# Funktion Produkt löschen
def delete_product(product_id):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()

    # Überprüfen, ob das Produkt existiert
    c.execute("SELECT product_id FROM products WHERE product_id = ?", (product_id,))
    product = c.fetchone()
    
    if not product:
        conn.close()
        st.error(f"Die Produktnummer {product_id} existiert nicht!")
        return
    
    # Lösche das Produkt aus der Tabelle "products"
    c.execute('''
        DELETE FROM products WHERE product_id = ?
    ''', (product_id,))
    
    # Lösche alle zugehörigen Buchungen aus der Tabelle "bookings"
    c.execute('''
        DELETE FROM bookings WHERE product_id = ?
    ''', (product_id,))

    conn.commit()
    conn.close()
    st.success(f"Produktnummer {product_id} wurde erfolgreich gelöscht!")

# Funktion Buchung löschen
def delete_booking(booking_id):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Prüfen, ob die Buchung existiert und relevante Daten abrufen
    c.execute('''
        SELECT product_id, menge, booking_art FROM bookings WHERE booking_id = ?
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
                SET bestandsmenge = bestandsmenge - ?
                WHERE product_id = ?
            ''', (menge, product_id))
        else:  # Warenausgang rückgängig machen
            c.execute('''
                UPDATE products
                SET bestandsmenge = bestandsmenge + ?
                WHERE product_id = ?
            ''', (menge, product_id))
        
        # Gesamtpreis aktualisieren
        c.execute('''
            UPDATE products
            SET gesamtpreis = bestandsmenge * preis_pro_einheit
        ''')
        
        # Buchung löschen
        c.execute('''
            DELETE FROM bookings WHERE booking_id = ?
        ''', (booking_id,))
        
        conn.commit()
    conn.close()
    st.success(f"Buchungnummer {booking_id} wurde erfolgreich gelöscht!")

# Grafik mit den monatlichen Konsum und Käufe erstellen
def plot_bar_chart():
    conn = sqlite3.connect('inventur.db')
    query = '''
    SELECT strftime('%Y-%m', buchungsdatum) AS Monat_Jahr, 
           SUM(CASE WHEN buchungstyp = 'Konsum' THEN menge ELSE 0 END) AS Konsum, 
           SUM(CASE WHEN buchungstyp = 'Kauf' THEN menge ELSE 0 END) AS Kauf 
    FROM bookings
    GROUP BY Monat_Jahr 
    ORDER BY Monat_Jahr DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    df.columns = ["Monat_Jahr", "Konsum", "Kauf"]
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
    ax.set_xlabel('Jahr & Monat')
    ax.set_ylabel('Menge')
    ax.legend()
    
    # Set the x-axis major formatter to show month and year (e.g., Jan 2025, Feb 2025)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    
    # Define the tick positions (use the positions based on the data)
    ax.set_xticks(ticks=position_a, labels=df['Monat_Jahr'])  # Set the ticks to the positions corresponding to the months
    
    # Rotate and format x-ticks to avoid overlap
    plt.xticks(rotation=45)
    plt.tight_layout()  # Ensures there's no clipping of labels
    
    # Display the plot in Streamlit
    st.pyplot(fig)

# Bestand & Gesamtpreis pro Lagerort
def show_inventory_per_location():
    conn = sqlite3.connect('inventur.db')
    
    query = """
    SELECT lagerort AS LAGERORT, SUM(bestandsmenge) AS BESTANDSMENGE, SUM(gesamtpreis) AS GESAMTWERT, 'EUR' AS WÄHRUNG
    FROM products
    GROUP BY LAGERORT, WÄHRUNG
    """

    df = pd.read_sql(query, conn)
    
    # Gesamtsumme berechnen
    total_quantity = df['BESTANDSMENGE'].sum()
    total_price = df['GESAMTWERT'].sum()
    total_währung = 'EUR'
 
    # Gesamtsumme als neue Zeile hinzufügen
    df_total = pd.DataFrame({'BESTANDSMENGE': [total_quantity], 'GESAMTWERT': [total_price], 'WÄHRUNG': [total_währung]})

    conn.close()
    
    if df.empty:
        st.write("Es sind keine Produkte vorhanden.")
    else:
        st.header("Bestand pro Lagerort")
        st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
        st.header("Gesamtübersicht")
        st.markdown(df_total.to_html(escape=False), unsafe_allow_html=True)

############# Frontend Streamlit
def main():
    # Get the current timestamp
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    # Display the current timestamp in Streamlit
    st.title("Weinlager Carla & Steffen")
    
    # Create Databank
    create_db()
    
    # Login
    st.sidebar.header("Login 🔑")
    username = st.sidebar.text_input("Benutzername")
    password = st.sidebar.text_input("Passwort", type="password")

    if st.sidebar.button("Login"):
         if not users:
             st.sidebar.error("Fehler: Keine Benutzerinformationen verfügbar.")
         elif username in users and users[username] == password:
             st.sidebar.success(f"Willkommen {username}!")

             st.sidebar.markdown("<h3>Was möchtest du tun? 🪄</h3>", unsafe_allow_html=True)
             action = st.sidebar.selectbox("", ['Gesamtübersicht anzeigen', 'Bestand anzeigen', 'Buchung erfassen', 'Buchung anzeigen', 'Buchung löschen', 'Produkt anlegen', 'Produkt ändern', 'Produkt löschen', 'Inventur anzeigen'], index=None)

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
                 zucker = st.text_input("Restzucker")
                 saure = st.text_input("Säure")
                 alko = st.text_input("Alkohol")
                 info = st.text_input("Weitere Infos")
                 kauf_link = st.text_input("Link zur Bestellung")
                 comments = st.text_input("Bemerkungen")
    
                 if st.button("Produkt anlegen"):
                     register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, zucker, saure, alko, info, kauf_link, comments)
                     st.success("Produkt erfolgreich registriert!")

             elif action == 'Produkt ändern':
                 st.header("Produkt ändern")
                 product_id = st.number_input("Produkt-ID", min_value=1, step=1)

                 # Eingabefelder für mögliche Änderungen
                 weingut = st.text_input("Weingut", value="")
                 rebsorte = st.text_input("Rebsorte", value="")
                 lage = st.text_input("Lage", value="")
                 land = st.text_input("Land", value="")
                 jahrgang = st.text_input("Jahrgang", value="")
                 lagerort = st.text_input("Lagerort", value="")
                 preis_pro_einheit = st.number_input("Preis pro Einheit", value=0.0)
                 zucker = st.text_input("Restzucker", value="")
                 saure = st.text_input("Säure", value="")
                 alko = st.text_input("Alkohol", value="")
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
                         "zucker": zucker,
                         "saure": saure,
                         "alko": alko,
                         "info": info,
                         "kauf_link": kauf_link,
                         "comments": comments
                     }.items() if value}  # Nur nicht-leere Werte speichern

                     if update_data:
                         result = update_product(product_id, **update_data)
                     else:
                         st.warning("Keine Änderungen vorgenommen.")      
        
             elif action == 'Buchung erfassen':
                 st.header("Buchung erfassen")
                 product_id = st.number_input("Produktnummer", min_value=1)
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

             elif action == 'Bestand anzeigen':
                 st.header("Bestand")
                 conn = sqlite3.connect('inventur.db')
                 query = '''
                    SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandsmenge, preis_pro_einheit, gesamtpreis, zucker, saure, alko, info, kauf_link, comments
                    FROM products
                    WHERE bestandsmenge <> '0'
                    ORDER BY 2
                    '''
                 df = pd.read_sql(query, conn)
                 df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "RESTZUCKER", "SÄURE", "ALKOHOL", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
                 conn.close()
                 st.dataframe(df)
             
             elif action == 'Inventur anzeigen':
                 st.header("Inventur")
                 conn = sqlite3.connect('inventur.db')
                 query = '''
                    SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandsmenge, preis_pro_einheit, gesamtpreis, zucker, saure, alko, info, kauf_link, comments
                    FROM products
                    ORDER BY 2
                    '''
                 df = pd.read_sql(query, conn)
                 df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDSMENGE", "EINZELPREIS", "GESAMTPREIS", "RESTZUCKER", "SÄURE", "ALKOHOL", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
                 conn.close()
                 st.dataframe(df)
                 
                 # Konvertiere kauf_link zu einem anklickbaren HTML-Link
                 #df['LINK_ZUR_BESTELLUNG'] = df['LINK_ZUR_BESTELLUNG'].apply(lambda x: f'<a href="{x}" target="_blank">{x}</a>')
                 #conn.close()
                 # Ausgabe der Tabelle mit HTML-Links
                 #st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
            
             elif action == 'Buchung anzeigen':
                 st.header("Buchungen")
                 conn = sqlite3.connect('inventur.db')
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
                 product_id_delete = st.number_input("Produktnummer zum Löschen", min_value=1)
    
                 if st.button("Produkt löschen"):
                     delete_product(product_id_delete)
            
             elif action == 'Buchung löschen':
                 st.header("Buchung löschen")
                 booking_id_delete = st.number_input("Buchungsnummer zum Löschen", min_value=1)
    
                 if st.button("Buchung löschen"):
                     delete_booking(booking_id_delete)
    
             elif action == 'Gesamtübersicht anzeigen':
                 show_inventory_per_location()
                 st.text ("")
                 plot_bar_chart()
             
             else:
                 st.text("") 

         else:
             st.sidebar.error("Benutzername oder Passwort ist falsch!")
    else:
        st.text ("")
         
if __name__ == "__main__":
    main()
