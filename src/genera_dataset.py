import csv, random
import pandas as pd
from faker import Faker

faker = Faker()

numero_transazioni = 20000
numero_clienti = 10000
numero_negozi = 5000

print()
# Creazione dei dati dei Clienti
dati_clienti = [["ID_Cliente", "Nome Cliente", "Cognome Cliente", "Carta di Credito", "Paese"]]

for i in range(numero_clienti):
    id_cliente = faker.uuid4()
    nome_cliente = faker.first_name()
    cognome_cliente = faker.last_name()
    carta_di_credito = faker.credit_card_number(card_type="mastercard")
    paese = random.choice(["USA", "Canada", "UK", "Germany", "France", "Italy", "Spain"])
    
    dati_clienti.append([id_cliente, nome_cliente, cognome_cliente, carta_di_credito, paese])

# Nome del file CSV dei Clienti
nome_file_clienti = "clienti_100%.csv"

# Creazione del file CSV dei Clienti
with open(nome_file_clienti, mode="w", newline="") as file_csv:
    writer = csv.writer(file_csv)
    writer.writerows(dati_clienti)

print(f"I Clienti sono stati scritti nel file '{nome_file_clienti}'.")

# Creazione dei dati dei Negozi
dati_negozi = [["ID_Negozio", "Sito Corrispondente"]]

for i in range(numero_negozi):
    id_negozio = faker.uuid4()
    sito_corrispondente = faker.url()
    
    dati_negozi.append([id_negozio, sito_corrispondente])

# Nome del file CSV dei Negozi
nome_file_negozi = "negozi_100%.csv"

# Creazione del file CSV dei Negozi
with open(nome_file_negozi, mode="w", newline="") as file_csv:
    writer = csv.writer(file_csv)
    writer.writerows(dati_negozi)

print(f"I Negozi sono stati scritti nel file '{nome_file_negozi}'.")

# Creazione dei dati delle Transazioni
dati_transazioni = [["ID_Transazione", "Importo", "Data", "Ora", "ID_Cliente", "ID_Negozio", "Fraud"]]

for i in range(numero_transazioni):
    id_transazione = faker.uuid4()
    importo = round(random.uniform(1, 1000), 2)
    data = faker.date_between(start_date="-3y", end_date="today").strftime("%Y-%m-%d")
    ora = faker.time(pattern="%H:%M:%S")
    id_cliente = random.choice([cliente[0] for cliente in dati_clienti[1:]])
    id_negozio = random.choice([negozio[0] for negozio in dati_negozi[1:]])
    fraud = random.choice(["YES", "NO"])

    dati_transazioni.append([id_transazione, importo, data, ora, id_cliente, id_negozio, fraud])

# Nome del file CSV delle Transazioni
nome_file_transazioni = "transazioni_100%.csv"

# Creazione del file CSV delle Transazioni
with open(nome_file_transazioni, mode="w", newline="") as file_csv:
    writer = csv.writer(file_csv)
    writer.writerows(dati_transazioni)

print(f"Le Transazioni sono state scritte nel file '{nome_file_transazioni}'.")

# Creazione dei dataset più piccoli a partire da quelli 100%
clienti_100 = pd.read_csv('clienti_100%.csv')
negozi_100 = pd.read_csv('negozi_100%.csv')
transazioni_100 = pd.read_csv('transazioni_100%.csv')

total_rows_clienti = len(clienti_100)
total_rows_negozi = len(negozi_100)
total_rows_transazioni = len(transazioni_100)

rows_25_percent_clienti = int(total_rows_clienti * 0.25)
rows_50_percent_clienti = int(total_rows_clienti * 0.50)
rows_75_percent_clienti = int(total_rows_clienti * 0.75)

rows_25_percent_negozi = int(total_rows_negozi * 0.25)
rows_50_percent_negozi = int(total_rows_negozi * 0.50)
rows_75_percent_negozi = int(total_rows_negozi * 0.75)

rows_25_percent_transazioni = int(total_rows_transazioni * 0.25)
rows_50_percent_transazioni = int(total_rows_transazioni * 0.50)
rows_75_percent_transazioni = int(total_rows_transazioni * 0.75)

df_25_percent_clienti = clienti_100.sample(n=rows_25_percent_clienti)
df_50_percent_clienti = clienti_100.sample(n=rows_50_percent_clienti)
df_75_percent_clienti = clienti_100.sample(n=rows_75_percent_clienti)

df_25_percent_negozi = negozi_100.sample(n=rows_25_percent_negozi)
df_50_percent_negozi = negozi_100.sample(n=rows_50_percent_negozi)
df_75_percent_negozi = negozi_100.sample(n=rows_75_percent_negozi)

df_25_percent_transazioni = transazioni_100.sample(n=rows_25_percent_transazioni)
df_50_percent_transazioni = transazioni_100.sample(n=rows_50_percent_transazioni)
df_75_percent_transazioni = transazioni_100.sample(n=rows_75_percent_transazioni)

df_25_percent_clienti.to_csv('clienti_25%.csv', index=False)
df_50_percent_clienti.to_csv('clienti_50%.csv', index=False)
df_75_percent_clienti.to_csv('clienti_75%.csv', index=False)

df_25_percent_negozi.to_csv('negozi_25%.csv', index=False)
df_50_percent_negozi.to_csv('negozi_50%.csv', index=False)
df_75_percent_negozi.to_csv('negozi_75%.csv', index=False)

df_25_percent_transazioni.to_csv('transazioni_25%.csv', index=False)
df_50_percent_transazioni.to_csv('transazioni_50%.csv', index=False)
df_75_percent_transazioni.to_csv('transazioni_75%.csv', index=False)

print("\nFile CSV derivati creati con successo.\n")