from pymongo import MongoClient
from scipy import stats
import time, csv, math
import numpy as np

# Connessione al database MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['rilevamento_frodi']
collections = ["25%", "50%", "75%", "100%"]

num_iterations = 31
confidence_level = 0.95

query_times = {
    "query1": {},
    "query2": {},
    "query3": {},
    "query4": {}
}
for collection in collections:
    transazioni = db[f'transazioni_{collection}']
    clienti = db[f'clienti_{collection}']
    negozi = db[f'negozi_{collection}']
    print_query1 = True
    print_query2 = True
    print_query3 = True
    print_query4 = True
    print()
    for iteration in range(num_iterations):

# QUERY 1 - Verifica quanti clienti Italiani sono presenti
        query_1 = {
            "Paese": "Italy"
        }
        start_time = time.time()
        clienti_italiani = clienti.count_documents(query_1)
        result_1 = clienti.find(query_1)
        found_results = False
        for _ in result_1:
            found_results = True
            break
        query_times["query1"].setdefault(collection, []).append((time.time() - start_time) * 1000)
        if print_query1:
            if found_results:
                print(f"QUERY 1 - Nella collezione {collection} sono presenti {clienti_italiani} clienti Italiani")
            else:
                print(f"QUERY 1 - Nella collezione {collection} NON sono presenti clienti Italiani")
            print_query1 = False

# QUERY 2 - Calcola quante transazioni fraudolente ci sono, e la loro somma 
        query_2 = [
                    {
                "$match": {
                    "Fraud": "YES"
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_count": { "$sum": 1 },
                    "total_amount": {"$sum": "$Importo"}
                }
            }
        ]
        start_time = time.time()  
        result_2 = transazioni.aggregate(query_2)
        fraud_transactions_summary = next(result_2, None)
        if print_query2:
            if fraud_transactions_summary:
                total_count = fraud_transactions_summary["total_count"]
                total_amount = fraud_transactions_summary["total_amount"]
                print(f"QUERY 2 - Nella collezione {collection} sono presenti {total_count} transazioni fraudolente, per un totale di €{total_amount:.2f}")
            else:
                print("QUERY 2 - Nessuna transazione fraudolenta trovata.")
        for doc in result_2:
            pass
        query_times["query2"].setdefault(collection, []).append((time.time() - start_time) * 1000)
        print_query2 = False

# QUERY 3 - Calcola quante transazioni fraudolente ci sono, fatte da italiani , e la somma degli importi
        query_3 = [
    {
        "$match": {
            "Fraud": "YES"
        }
    },
    {
        "$lookup": {
            "from": f"clienti_{collection}",
            "localField": "ID_Cliente",
            "foreignField": "ID_Cliente",
            "as": "cliente"
        }
    },
    {
        "$match": {
            "cliente.Paese": "Italy"
        }
    },
    {
        "$group": {
            "_id": None,
            "total_transactions": { "$sum": 1 },
            "total_amount": { "$sum": "$Importo" }
        }
    }
]
        start_time = time.time()
        result_3 = transazioni.aggregate(query_3)
        tx_fraudolente_italiani = next(result_3, None)
        if print_query3:
            if tx_fraudolente_italiani and "total_transactions" in tx_fraudolente_italiani and "total_amount" in tx_fraudolente_italiani:
                total_count = tx_fraudolente_italiani["total_transactions"]
                total_amount = round(tx_fraudolente_italiani["total_amount"], 2)
                print(f"QUERY 3 - Nella collezione {collection} sono presenti {total_count} transazioni fraudolente fatte da italiani, per un totale di €{total_amount:.2f}")
            else:
                print(f"QUERY 3 - Nessuna transazione fraudolenta da clienti italiani trovata nel dataset {collection}")
        print_query3 = False
        for doc in result_3:
                pass
        query_times["query3"].setdefault(collection, []).append((time.time() - start_time) * 1000)

# QUERY 4 - Quale è il negozio con il maggior numero di transazioni fraudolente, fatte da italiani
        query_4 = [
    {
        "$match": {
            "Fraud": "YES"
        }
    },
    {
        "$lookup": {
            "from": f"clienti_{collection}",
            "localField": "ID_Cliente",
            "foreignField": "ID_Cliente",
            "as": "cliente"
        }
    },
    {
        "$match": {
            "cliente.Paese": "Italy"
        }
    },
    {
        "$lookup": {
            "from": f"negozi_{collection}",
            "localField": "ID_Negozio",
            "foreignField": "ID_Negozio",
            "as": "negozio"
        }
    },
    {
        "$group": {
            "_id": "$ID_Negozio",
            "negozio_name": { "$first": "$negozio.Sito Corrispondente" },
            "numero_transazioni_fraudolente": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "numero_transazioni_fraudolente": -1
        }
    },
    {
        "$limit": 1
    }
]
        start_time = time.time()
        result_4 = transazioni.aggregate(query_4)
        if print_query4:
            for doc in result_4:
                negozio_id = doc["_id"]
                negozio_name = doc["negozio_name"]
                numero_transazioni_fraudolente = doc["numero_transazioni_fraudolente"]
                print(f"QUERY 4 - Nella collezione {collection}, il negozio con il maggior numero di transazioni fraudolente fatte in Italia è '{negozio_name}' con {numero_transazioni_fraudolente} transazioni fraudolente.")
        print_query4 = False
        for doc in result_4:
                pass
        query_times["query4"].setdefault(collection, []).append((time.time() - start_time) * 1000)

print()


with open('execution_times_MongoDB.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Colonne del CSV
    writer.writerow(["Collection", "Query", "First Execution Time (ms)", "Average Execution Time (ms)", "Confidence Interval Min (ms)", "Confidence Interval Max (ms)"])
    
    for query_name, collection_times in query_times.items():
        for collection_name, times in collection_times.items():
            # Calcolo il tempo medio (in ms) utilizzando NumPy
            mean_time = np.mean(times)
            
            # Calcolo la deviazione standard (in millisecondi) utilizzando NumPy
            std_dev = np.std(times, ddof=1)
            
            # Calcolo l'intervallo di confidenza (in ms) utilizzando NumPy
            times_for_confidence = times[1:]  # Escludo la prima esecuzione
            num_samples = len(times_for_confidence)
            confidence_interval = stats.t.ppf((1 + confidence_level) / 2, num_samples - 1) * std_dev / math.sqrt(num_samples)
            
            # Calcolo il limite inferiore e superiore dell'intervallo di confidenza
            lower_limit = mean_time - confidence_interval
            upper_limit = mean_time + confidence_interval
            
            # Scrivo il tempo medio e l'intervallo di confidenza nel CSV (ms)
            writer.writerow([collection_name, query_name, times[0], mean_time, lower_limit, upper_limit])