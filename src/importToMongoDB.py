from pymongo import MongoClient
import pandas as pd

# Connessione al database MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['rilevamento_frodi']

# Legge i dati dai file CSV utilizzando pandas
clienti_25 = pd.read_csv('clienti_25%.csv')
clienti_50 = pd.read_csv('clienti_50%.csv')
clienti_75 = pd.read_csv('clienti_75%.csv')
clienti_100 = pd.read_csv('clienti_100%.csv')

negozi_25 = pd.read_csv('negozi_25%.csv')
negozi_50 = pd.read_csv('negozi_50%.csv')
negozi_75 = pd.read_csv('negozi_75%.csv')
negozi_100 = pd.read_csv('negozi_100%.csv')

transazioni_25 = pd.read_csv('transazioni_25%.csv')
transazioni_50 = pd.read_csv('transazioni_50%.csv')
transazioni_75 = pd.read_csv('transazioni_75%.csv')
transazioni_100 = pd.read_csv('transazioni_100%.csv')

# Converte i dati in formato JSON
clienti_25_json = clienti_25.to_dict(orient='records')
clienti_50_json = clienti_50.to_dict(orient='records')
clienti_75_json = clienti_75.to_dict(orient='records')
clienti_100_json = clienti_100.to_dict(orient='records')

negozi_25_json = negozi_25.to_dict(orient='records')
negozi_50_json = negozi_50.to_dict(orient='records')
negozi_75_json = negozi_75.to_dict(orient='records')
negozi_100_json = negozi_100.to_dict(orient='records')

transazioni_25_json = transazioni_25.to_dict(orient='records')
transazioni_50_json = transazioni_50.to_dict(orient='records')
transazioni_75_json = transazioni_75.to_dict(orient='records')
transazioni_100_json = transazioni_100.to_dict(orient='records')

# Inserimento dei dati nelle collezioni del database
collection_clienti_25 = db['clienti_25%']
collection_clienti_50 = db['clienti_50%']
collection_clienti_75 = db['clienti_75%']
collection_clienti_100 = db['clienti_100%']

collection_negozi_25 = db['negozi_25%']
collection_negozi_50 = db['negozi_50%']
collection_negozi_75 = db['negozi_75%']
collection_negozi_100 = db['negozi_100%']

collection_transazioni_25 = db['transazioni_25%']
collection_transazioni_50 = db['transazioni_50%']
collection_transazioni_75 = db['transazioni_75%']
collection_transazioni_100 = db['transazioni_100%']

collection_clienti_25.insert_many(clienti_25_json)
collection_clienti_50.insert_many(clienti_50_json)
collection_clienti_75.insert_many(clienti_75_json)
collection_clienti_100.insert_many(clienti_100_json)

collection_negozi_25.insert_many(negozi_25_json)
collection_negozi_50.insert_many(negozi_50_json)
collection_negozi_75.insert_many(negozi_75_json)
collection_negozi_100.insert_many(negozi_100_json)

collection_transazioni_25.insert_many(transazioni_25_json)
collection_transazioni_50.insert_many(transazioni_50_json)
collection_transazioni_75.insert_many(transazioni_75_json)
collection_transazioni_100.insert_many(transazioni_100_json)

print("Dati inseriti nelle collezioni MongoDB corrispondenti con successo.")