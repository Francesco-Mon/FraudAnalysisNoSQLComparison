import pandas as pd
from py2neo import Graph, Node, Relationship

# Connessione ai grafi Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="frodi100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="frodi75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="frodi50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="frodi25")

# Dizionario per mappare le percentuali ai grafi
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# Tipi di dati
data_types = ['clienti', 'negozi', 'transazioni']

for data_type in data_types:
    for percentage in graphs_by_percentage:
        csv_filename = f'{data_type}_{percentage}%.csv'
        
        data = pd.read_csv(csv_filename)
    
        data_dict_list = data.to_dict(orient='records')
         
        # Ottiene il grafo corrispondente alla percentuale
        graph = graphs_by_percentage[percentage]
        
        for index, row in data.iterrows():
            node = Node(data_type, **row.to_dict())
            graph.create(node)

            if data_type == 'transazioni':
                # Crea relazione con cliente
                ID_Cliente = row['ID_Cliente']
                nodo_cliente = graph.nodes.match('clienti', ID_Cliente=ID_Cliente).first()
                if nodo_cliente:
                    transazione_del_cliente = Relationship(nodo_cliente, 'HA_EFFETTUATO', node)
                    graph.create(transazione_del_cliente)

                # Crea relazione con negozio
                ID_Negozio = row['ID_Negozio']
                nodo_negozio = graph.nodes.match('negozi', ID_Negozio=ID_Negozio).first()
                if nodo_negozio:
                    transazione_al_negozio = Relationship(node, 'AVVENUTA_IN', nodo_negozio)
                    graph.create(transazione_al_negozio)

        print(f"Dati del dataset {data_type}_{percentage}% inseriti in Neo4j con successo.")

print("\nInserimento completato per tutti i dataset.\n")