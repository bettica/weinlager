import streamlit as st
import sqlite3
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
        bestandmenge INTEGER DEFAULT 0,
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

# Benutzerauthentifizierung
def authenticate(username, password):
    valid_users = {
        "Steffen": "10021983SF-",
        "Carla": "08121985CB-"
    }
    return valid_users.get(username) == password

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
        return "Produkt nicht gefunden!"

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

    return "Produkt erfolgreich aktualisiert!"

# Funktion Wareneingang buchen
def record_incoming_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Buchung in der Tabelle 'bookings' einfügen
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))
    
    # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute('''
        UPDATE products
        SET bestandmenge = bestandmenge + ?   
        WHERE product_id = ?
        ''', (menge, product_id))
    
    c.execute('''
        UPDATE products
        SET gesamtpreis = bestandmenge * preis_pro_einheit  
        ''')
    
    conn.commit()
    conn.close()

# Funktion Warenausgang buchen
def record_outgoing_booking(product_id, menge, buchungstyp, buchungsdatum, booking_art, comments):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Buchung in der Tabelle 'bookings' einfügen
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum, booking_art, comments))
    
    # Bestand und Gesamtpreis in der Tabelle 'products' aktualisieren
    c.execute('''
        UPDATE products
        SET bestandmenge = bestandmenge - ?
        WHERE product_id = ?
        ''', (menge, product_id))
    
    c.execute('''
        UPDATE products
        SET gesamtpreis = bestandmenge * preis_pro_einheit  
        ''')
    
    conn.commit()
    conn.close()

# Funktion Produkt löschen
def delete_product(product_id):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
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

# Funktion Buchung löschen
def delete_booking(booking_id):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Prüfen, ob die Buchung existiert und relevante Daten abrufen
    c.execute('''
        SELECT product_id, menge, booking_art FROM bookings WHERE booking_id = ?
    ''', (booking_id,))
    booking = c.fetchone()
    
    if booking:
        product_id, menge, booking_art = booking
        
        # Bestand anpassen: Wenn es sich um einen Wareneingang handelt, verringern, sonst erhöhen
        if booking_art == 'Wareneingang':
            c.execute('''
                UPDATE products
                SET bestandmenge = bestandmenge - ?
                WHERE product_id = ?
            ''', (menge, product_id))
        else:  # Warenausgang rückgängig machen
            c.execute('''
                UPDATE products
                SET bestandmenge = bestandmenge + ?
                WHERE product_id = ?
            ''', (menge, product_id))
        
        # Gesamtpreis aktualisieren
        c.execute('''
            UPDATE products
            SET gesamtpreis = bestandmenge * preis_pro_einheit
        ''')
        
        # Buchung löschen
        c.execute('''
            DELETE FROM bookings WHERE booking_id = ?
        ''', (booking_id,))
        
        conn.commit()
    
    conn.close()

# Grafik mit den monatlichen Konsum und Käufe erstellen
def plot_bar_chart():
    conn = sqlite3.connect('inventur.db')
    query = '''
    SELECT strftime('%Y-%m', buchungsdatum) AS Monat_Jahr, 
           SUM(CASE WHEN buchungstyp = 'Getrunken' THEN menge ELSE 0 END) AS Konsum, 
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
    SELECT lagerort AS LAGERORT, SUM(bestandmenge) AS BESTANDMENGE, SUM(gesamtpreis) AS GESAMTPREIS, 'EUR' AS WÄHRUNG
    FROM products
    GROUP BY LAGERORT, WÄHRUNG
    """

    df = pd.read_sql(query, conn)
    
    # Gesamtsumme berechnen
    total_quantity = df['BESTANDMENGE'].sum()
    total_price = df['GESAMTPREIS'].sum()
    total_währung = 'EUR'
 
    # Gesamtsumme als neue Zeile hinzufügen
    df_total = pd.DataFrame({'BESTANDMENGE': [total_quantity], 'GESAMTPREIS': [total_price], 'WÄHRUNG': [total_währung]})

    conn.close()
    
    if df.empty:
        st.write("Es sind keine Produkte vorhanden.")
    else:
        st.header("Inventur pro Lagerort")
        st.dataframe(df)
        st.header("Gesamte Inventur")
        st.dataframe(df_total)

# Frontend Streamlit
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
    
    if username != "" and password != "":
         if authenticate(username, password):
             st.sidebar.success(f"Willkommen {username}!")

             st.sidebar.markdown("<h3>Was möchtest du tun? 🪄</h3>", unsafe_allow_html=True)
             action = st.sidebar.selectbox("", ['Produktregistrierung', 'Wareneingang buchen', 'Warenausgang buchen', 'Buchungen anzeigen', 'Inventur anzeigen', 'Inventur pro Lagerort anzeigen', 'Grafik anzeigen', 'Produkt anpassen', 'Produkt löschen', 'Buchung löschen'], index=None)

             # Das Bild nur anzeigen, wenn keine Aktion gewählt wurde
             if action is None:
                 st.image("weinbild.jpg", caption="Willkommen im Weinlager 🍷", use_container_width=False)
                 st.write(f"{formatted_timestamp}")
             else:
                 st.write(f"{formatted_timestamp}")  # Zeige nur den Timestamp, falls eine Aktion gewählt wurde

             if action == 'Produktregistrierung':               
               #Produktregistrierung
                 st.header("Produktregistrierung")
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
    
                 if st.button("Produkt registrieren"):
                     register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, zucker, saure, alko, info, kauf_link, comments)
                     st.success("Produkt erfolgreich registriert!")

             elif action == 'Produkt anpassen':
                 st.header("Produkt anpassen")
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

                 if st.button("Produkt aktualisieren"):
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
                         st.success(result)
                     else:
                         st.warning("Keine Änderungen vorgenommen.")      
        
             elif action == 'Wareneingang buchen':
                 st.header("Wareneingang buchen")
                 product_id_in = st.number_input("Produktnummer", min_value=1)
                 buchungsdatum_in = st.date_input("Buchungsdatum")
                 menge_in = st.number_input("Menge", min_value=1)
                 buchungstyp_in = st.selectbox("Buchungsart", ["Kauf", "Geschenk", "Umlagerung", "Inventur"])
                 comments_in = st.text_input("Bemerkungen")
                 booking_art_in = st.radio("Buchungstyp",('Wareneingang'))
    
                 if st.button("Wareneingang buchen"):
                     record_incoming_booking(product_id_in, menge_in, buchungstyp_in, buchungsdatum_in, booking_art_in, comments_in)
                     st.success("Wareneingang erfolgreich gebucht!")
            
             elif action == 'Warenausgang buchen':
                 st.header("Warenausgang buchen")
                 product_id_out = st.number_input("Produktnummer", min_value=1)
                 buchungsdatum_out = st.date_input("Buchungsdatum")
                 menge_out = st.number_input("Menge", min_value=1)
                 buchungstyp_out = st.selectbox("Buchungsart", ["Getrunken", "Geschenk", "Entsorgt", "Umlagerung", "Inventur"])
                 comments_out = st.text_input("Bemerkungen")
                 booking_art_out = st.radio("Buchungstyp",('Warenausgang'))

                 if st.button("Warenausgang buchen"):
                     record_outgoing_booking(product_id_out, menge_out, buchungstyp_out, buchungsdatum_out, booking_art_out, comments_out)
                     st.success("Warenausgang erfolgreich gebucht!")
            
             elif action == 'Inventur anzeigen':
                 conn = sqlite3.connect('inventur.db')
                 query = '''
                    SELECT product_id, weingut, rebsorte, lage, land, jahrgang, lagerort, bestandmenge, preis_pro_einheit, gesamtpreis, zucker, saure, alko, info, kauf_link, comments
                    FROM products
                    ORDER BY 2
                    '''
                 df = pd.read_sql(query, conn)
                 df.columns = ["PRODUKTNR", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDMENGE", "EINZELPREIS", "GESAMTPREIS", "RESTZUCKER", "SÄURE", "ALKOHOL", "WEITERE_INFOS", "LINK_ZUR_BESTELLUNG", "BEMERKUNGEN"]
                 conn.close()
                 st.dataframe(df)
                 
                 # Konvertiere kauf_link zu einem anklickbaren HTML-Link
                 #df['LINK_ZUR_BESTELLUNG'] = df['LINK_ZUR_BESTELLUNG'].apply(lambda x: f'<a href="{x}" target="_blank">{x}</a>')
                 #conn.close()
                 # Ausgabe der Tabelle mit HTML-Links
                 #st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
            
             elif action == 'Buchungen anzeigen':
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
                     st.success(f"Produktnummer {product_id_delete} wurde erfolgreich gelöscht!")
            
             elif action == 'Buchung löschen':
                 st.header("Buchung löschen")
                 booking_id_delete = st.number_input("Buchungsnummer zum Löschen", min_value=1)
    
                 if st.button("Buchung löschen"):
                     delete_booking(booking_id_delete)
                     st.success(f"Buchungnummer {booking_id_delete} wurde erfolgreich gelöscht!")
         
             elif action == 'Grafik anzeigen':
                 plot_bar_chart()
    
             elif action == 'Inventur pro Lagerort anzeigen':
                 show_inventory_per_location()
                     
             else:
                 st.text("") 

         elif not authenticate(username, password):
             st.sidebar.error("Benutzername oder Passwort ist falsch!")
    else:
        st.text ("")
         
if __name__ == "__main__":
    main()
