import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# SQLite-Verbindung einrichten
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
        jahrgang INTEGER,
        lagerort TEXT,
        bestandmenge INTEGER DEFAULT 0,
        preis_pro_einheit FLOAT,
        gesamtpreis FLOAT,
        kauf_link TEXT
    )''')

    # Tabelle für Buchungen erstellen
    c.execute('''CREATE TABLE IF NOT EXISTS bookings (
        booking_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        buchungsdatum TEXT,
        menge INTEGER,
        buchungstyp TEXT,
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

# Funktion ein Produkt zu erstellen
def register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, kauf_link):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
   
    c.execute('''
        INSERT INTO products (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, kauf_link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, kauf_link))
    
    conn.commit()
    conn.close()

# Funktion WE zu buchen
def record_incoming_booking(product_id, menge, buchungstyp, buchungsdatum):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum)
        VALUES (?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum))
    
    if buchungstyp in ["Kauf", "Geschenk", "Umlagerung", "Inventur"]:
        c.execute('''
            UPDATE products
            SET bestandmenge = bestandmenge + ?, gesamtpreis = gesamtpreis + (? * preis_pro_einheit)
            WHERE product_id = ?
        ''', (menge, menge, product_id))
    
    conn.commit()
    conn.close()

# Funktion WA zu buchen
def record_outgoing_booking(product_id, menge, buchungstyp, buchungsdatum):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    c.execute('''
        INSERT INTO bookings (product_id, menge, buchungstyp, buchungsdatum)
        VALUES (?, ?, ?, ?)
    ''', (product_id, menge, buchungstyp, buchungsdatum))
    
    if buchungstyp in ["Getrunken", "Geschenkt", "Entsorgt", "Umlagerung", "Inventur"]:
        c.execute('''
            UPDATE products
            SET bestandmenge = bestandmenge - ?, gesamtpreis = gesamtpreis - (? * preis_pro_einheit)
            WHERE product_id = ?
        ''', (menge, menge, product_id))
    
    conn.commit()
    conn.close()

# Funktion zum Löschen eines Produkts
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

# Funktion zum Löschen einer Buchung
def delete_booking(booking_id):
    conn = sqlite3.connect('inventur.db')
    c = conn.cursor()
    
    # Lösche die Buchung aus der Tabelle "bookings"
    c.execute('''
        DELETE FROM bookings WHERE booking_id = ?
    ''', (booking_id,))
    
    conn.commit()
    conn.close()

# Grafiken erstellen
def plot_consumption():
    conn = sqlite3.connect('inventur.db')
    query = """
    SELECT strftime('%Y-%m', buchungsdatum) AS month, SUM(menge) AS consumption
    FROM bookings
    WHERE buchungstyp = 'Getrunken'
    GROUP BY month
    """
    df = pd.read_sql(query, conn)
    conn.close()
        
    plt.figure(figsize=(10, 6))
    plt.plot(df['month'], df['consumption'], marker='o')
    plt.title('Monatliche Konsummenge')
    plt.xlabel('Monat')
    plt.ylabel('Menge')
    plt.xticks(rotation=45)
    st.pyplot()

def plot_purchase():
    conn = sqlite3.connect('inventur.db')
    query = """
    SELECT strftime('%Y-%m', buchungsdatum) AS month, SUM(menge) AS purchase
    FROM bookings
    WHERE buchungstyp = 'Kauf'
    GROUP BY month
    """
    df = pd.read_sql(query, conn)
    conn.close()
        
    plt.figure(figsize=(10, 6))
    plt.plot(df['month'], df['purchase'], marker='o')
    plt.title('Monatliche Kaufmenge')
    plt.xlabel('Monat')
    plt.ylabel('Menge')
    plt.xticks(rotation=45)
    st.pyplot()

# Monatliche Konsum- und Kauf-Grafik erstellen
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
    
    # Convert the column Monat_Jahr to datetime
    df['Monat_Jahr'] = pd.to_datetime(df['Monat_Jahr'], format='%Y-%m')
    
    # Create figure and axes for plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    
    position_a = list(range(len(df['Monat_Jahr'])))
    position_b = [pos + 0.25 for pos in position_a]  # Slight offset for the second set of bars
    
    # Plotting the bars with slight offsets to avoid overlap
    ax.bar(position_a, df['Konsum'], width=0.25, label='Konsum', color='red')
    ax.bar(position_b, df['Kauf'], width=0.25, label='Kauf', color='blue', alpha=0.5)
    
    # Formatting x-axis and adding labels
    ax.set_xlabel('Monat & Jahr')
    ax.set_ylabel('Menge')
    ax.legend()
    
    # Set the x-axis major formatter to show month and year (e.g., Jan 2025, Feb 2025)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    
    # Define the tick positions (use the positions based on the data)
    ax.set_xticks(position_a)  # Set the ticks to the positions corresponding to the months
    
    # Rotate and format x-ticks to avoid overlap
    plt.xticks(rotation=45)
    plt.tight_layout()  # Ensures there's no clipping of labels
    
    # Display the plot in Streamlit
    st.pyplot(fig)

# Anzeige der Inventur pro Lagerort
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

# Streamlit Settings
def main():
    # Get the current timestamp
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    # Display the current timestamp in Streamlit
    st.title("Weinlager Carla & Steffen")
    st.write(f"{formatted_timestamp}")
    
    create_db()
    
    # Login
    st.sidebar.header("Login")
    username = st.sidebar.text_input("Benutzername")
    password = st.sidebar.text_input("Passwort", type="password")
    
    if username != "" and password != "":
         if authenticate(username, password):
             st.sidebar.success(f"Willkommen {username}!")
        
             action = st.sidebar.selectbox("Was möchtest du tun?", ['Produkt erfassen', 'Wareneingangbuchen', 'Warenausgangbuchen', 'Inventur anzeigen', 'Buchungen anzeigen', 'Produkt löschen', 'Buchungen löschen', 'Grafik anzeigen', 'Inventur pro Lagerort anzeigen'], index=None)

             if action == 'Produkt erfassen':               
               #Produktregistrierung
                 st.header("Produktregistrierung")
                 weingut = st.text_input("Weingut")
                 rebsorte = st.text_input("Rebsorte")
                 lage = st.text_input("Lage")
                 land = st.text_input("Land")
                 jahrgang = st.number_input("Jahrgang", step=1)
                 lagerort = st.text_input("Lagerort")
                 preis_pro_einheit = st.number_input("Preis pro Einheit", min_value=0.0, step=0.01, format="%.2f")
                 kauf_link = st.text_input("Kauf-Link")
    
                 if st.button("Produkt registrieren"):
                     register_product(weingut, rebsorte, lage, land, jahrgang, lagerort, preis_pro_einheit, kauf_link)
                     st.success("Produkt erfolgreich registriert!")
        
             elif action == 'Wareneingangbuchen':
                 st.header("Wareneingang buchen")
                 product_id_in = st.number_input("Produkt ID (für Wareneingang)", min_value=1)
                 buchungsdatum_in = st.date_input("Buchungsdatum für Wareneingang")
                 menge_in = st.number_input("Menge", min_value=1)
                 buchungstyp_in = st.selectbox("Buchungstyp", ["Kauf", "Geschenk", "Umlagerung", "Inventur"])
    
                 if st.button("Wareneingang buchen"):
                     record_incoming_booking(product_id_in, menge_in, buchungstyp_in, buchungsdatum_in)
                     st.success("Wareneingang erfolgreich gebucht!")
            
             elif action == 'Warenausgangbuchen':
                 st.header("Warenausgang buchen")
                 product_id_out = st.number_input("Produkt ID (für Warenausgang)", min_value=1)
                 buchungsdatum_out = st.date_input("Buchungsdatum für Warenausgang")
                 menge_out = st.number_input("Menge", min_value=1)
                 buchungstyp_out = st.selectbox("Buchungstyp", ["Getrunken", "Geschenkt", "Entsorgt", "Umlagerung", "Inventur"])

                 if st.button("Warenausgang buchen"):
                     record_outgoing_booking(product_id_out, menge_out, buchungstyp_out, buchungsdatum_out)
                     st.success("Warenausgang erfolgreich gebucht!")
            
             elif action == 'Inventur anzeigen':
                 conn = sqlite3.connect('inventur.db')
                 query = '''
                    SELECT weingut, rebsorte, lage, land, jahrgang, lagerort, bestandmenge, preis_pro_einheit, gesamtpreis, kauf_link
                    FROM products
                    ORDER BY 1
                    '''
                 df = pd.read_sql(query, conn)
                 df.columns = ["WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "BESTANDMENGE", "EINZELPREIS", "GESAMTPREIS", "LINK ZUR KAUF"]
                 conn.close()
                 st.dataframe(df)
            
             elif action == 'Buchungen anzeigen':
                 conn = sqlite3.connect('inventur.db')
                 query = '''
                   SELECT a.product_id, b.weingut, b.rebsorte, b.lage, b.land, b.jahrgang, b.lagerort, a.menge, a.buchungstyp, a.buchungsdatum 
                   FROM bookings a 
                   LEFT OUTER JOIN products b 
                   ON a.product_id = b.product_id
                   ORDER BY a.buchungsdatum
                   '''
                 df = pd.read_sql(query, conn)
                 df.columns = ["PRODUCT_ID", "WEINGUT", "REBSORTE", "LAGE", "LAND", "JAHRGANG", "LAGERORT", "MENGE", "BUCHUNGSTYP", "BUCHUNGSDATUM"]
                 conn.close()
                 st.dataframe(df)

             elif action == 'Produkt löschen':
                 st.header("Produkt löschen")
                 product_id_delete = st.number_input("Produkt ID zum Löschen", min_value=1)
    
                 if st.button("Produkt löschen"):
                     delete_product(product_id_delete)
                     st.success(f"Produkt mit ID {product_id_delete} wurde erfolgreich gelöscht!")
            
             elif action == 'Buchungen löschen':
                 st.header("Buchung löschen")
                 booking_id_delete = st.number_input("Buchung ID zum Löschen", min_value=1)
    
                 if st.button("Buchung löschen"):
                     delete_booking(booking_id_delete)
                     st.success(f"Buchung mit ID {booking_id_delete} wurde erfolgreich gelöscht!")
         
             elif action == 'Grafik anzeigen':
                 plot_bar_chart()
                 #  plot_consumption()
                 #  plot_purchase()
    
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