from neo4j import GraphDatabase
from scipy import stats
import time, csv, math
import numpy as np

# Connessione a Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "12345678"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Lista delle collezioni e nomi dei database Neo4j
percentages = ['25%', '50%', '75%', '100%']
db_names = ['frodi25', 'frodi50', 'frodi75', 'frodi100']
confidence_level = 0.95
num_iterations = 31

query_times = {
    "query1": {},
    "query2": {},
    "query3": {},
    "query4": {}
}

def run_query(session, query):
    return session.run(query)

for percentage, db_name in zip(percentages, db_names):
    print_query1 = True
    print_query2 = True
    print_query3 = True
    print_query4 = True
    print()
    for iteration in range(num_iterations):
        with driver.session(database=db_name) as session:

# QUERY 1 - Verifica quanti clienti Italiani sono presenti
            start_time = time.time()
            query_1 = (
                f"MATCH (t:clienti) "
                f"WHERE t.Paese = 'Italy' "
                f"RETURN COUNT(t) AS total_italian_customers"
            )
            result_1 = run_query(session, query_1)
            total_italian_customers = result_1.single()["total_italian_customers"]
            if print_query1:
                if total_italian_customers>0:
                    print (f"QUERY 1 - Nella collezione {percentage} sono presenti {total_italian_customers} clienti Italiani")
                else:
                    print(f"QUERY 1 - Nella collezione {percentage} NON sono presenti clienti Italiani")
            print_query1 = False
            if result_1:
                pass
            query_times["query1"].setdefault(percentage, []).append((time.time() - start_time) * 1000)

# QUERY 2 - Calcola quante transazioni fraudolente ci sono, e la loro somma
            start_time = time.time()
            query_2 = (
                f"MATCH (t:transazioni) "
                f"WHERE t.Fraud = 'YES' "
                f"RETURN COUNT(t) AS total_fraud, SUM(t.Importo) AS total_import"
            )
            result_2 = run_query(session, query_2)
            record = result_2.single()
            total_fraud = record["total_fraud"]
            total_import = record["total_import"]
            if print_query2:
                print(f"QUERY 2 - Nella collezione {percentage} sono presenti {total_fraud} transazioni fraudolente, per un totale di €{total_import:.2f}")
            print_query2 = False
            if result_2:
                pass
            query_times["query2"].setdefault(percentage, []).append((time.time() - start_time) * 1000)

# QUERY 3 - Calcola quante transazioni fraudolente ci sono, fatte da italiani , e la somma degli importi
# SOMMA SBAGLIATA
            start_time = time.time()
            query_3 = (
                f"MATCH (c:clienti)-[:HA_EFFETTUATO]->(t:transazioni) "
                f"WHERE t.Fraud = 'YES' AND c.Paese = 'Italy' "
                f"RETURN COUNT(*) AS total_fraud_italian, SUM(t.Importo) AS total_importo_fraud_italian"
            )
            result_3 = run_query(session, query_3)
            results=list(result_3)
            total_fraud_italian = results[0]["total_fraud_italian"]
            total_importo_fraud_italian = results[0]["total_importo_fraud_italian"]
            if print_query3:
                if total_fraud_italian>0:
                    print(f"QUERY 3 - Nella collezione {percentage} sono presenti {total_fraud_italian} transazioni fraudolente fatte da italiani, per un totale di €{total_importo_fraud_italian:.2f}")
                else:
                    print(f"QUERY 3 - Nessuna transazione fraudolenta da clienti italiani trovata nel dataset {percentage}")
            print_query3 = False
            if result_3:
                pass
            query_times["query3"].setdefault(percentage, []).append((time.time() - start_time) * 1000)

# QUERY 4 - Quale è il negozio con il maggior numero di transazioni fraudolente, fatte da italiani
            start_time = time.time()
            query_4 = (
                f"MATCH (t:transazioni)<-[:HA_EFFETTUATO]-(c:clienti) "
                f"WHERE t.Fraud = 'YES' AND c.Paese = 'Italy' "
                f"WITH t "
                f"MATCH (t)-[:AVVENUTA_IN]->(n:negozi) "
                f"WITH n, COUNT(t) AS numero_transazioni_fraudolente "
                f"RETURN n.`Sito Corrispondente` AS sito, numero_transazioni_fraudolente "
                f"ORDER BY numero_transazioni_fraudolente DESC "
                f"LIMIT 1"
            )
            result_4 = run_query(session, query_4)
            if print_query4:
                if result_4.peek():
                    for record in result_4:
                        sito_corrispondente = record["sito"]
                        numero_transazioni_fraudolente = record["numero_transazioni_fraudolente"]
                        print(f"QUERY 4 - Nella collezione {percentage}, il negozio con il maggior numero di transazioni fraudolente fatte in Italia è '{sito_corrispondente}' con {numero_transazioni_fraudolente} transazioni fraudolente.")
                else:
                    print("QUERY 4 - Nessun risultato trovato per la QUERY 4")
            print_query4 = False
            if result_4:
                pass
            query_times["query4"].setdefault(percentage, []).append((time.time() - start_time) * 1000)

print()

with open('execution_times_Neo4j.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Colonne del CSV
    writer.writerow(["Collection", "Query", "First Execution Time (ms)", "Average Execution Time (ms)", "Confidence Interval Min (ms)", "Confidence Interval Max (ms)"])
    
    for query_name, collection_times in query_times.items():
        for collection_name, times in collection_times.items():
            # Calcola il tempo medio (ms) utilizzando NumPy
            mean_time = np.mean(times)
            
            # Calcolo la deviazione standard (ms) utilizzando NumPy
            std_dev = np.std(times, ddof=1)
            
            # Calcolo l'intervallo di confidenza (ms) utilizzando NumPy
            times_for_confidence = times[1:]  # Escludi la prima esecuzione
            num_samples = len(times_for_confidence)
            confidence_interval = stats.t.ppf((1 + confidence_level) / 2, num_samples - 1) * std_dev / math.sqrt(num_samples)
            
            # Calcolo il limite inferiore e superiore dell'intervallo di confidenza
            lower_limit = mean_time - confidence_interval
            upper_limit = mean_time + confidence_interval
            
            # Scrivo il tempo medio e l'intervallo di confidenza nel file CSV (in ms)
            writer.writerow([collection_name, query_name, times[0], mean_time, lower_limit, upper_limit])

# Chiude la connessione al database Neo4j
driver.close()