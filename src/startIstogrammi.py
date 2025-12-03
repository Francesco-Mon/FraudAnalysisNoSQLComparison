import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.patches as mpatches

tempi_mongodb = pd.read_csv('execution_times_MongoDB.csv')
tempi_neo4j = pd.read_csv('execution_times_Neo4j.csv')
dataset = ['25%', '50%', '75%', '100%']
queries = ['query1', 'query2', 'query3', 'query4']

colors = ['#00FF00', '#007FFF']

for query in queries:
    data_mongo_query = tempi_mongodb[tempi_mongodb['Query'] == query]
    data_neo4j_query = tempi_neo4j[tempi_neo4j['Query'] == query]

    # Primo istogramma - Tempi delle prime esecuzioni
    plt.figure(figsize=(10, 6))
    for size in dataset:
        values_mongo_first = data_mongo_query[data_mongo_query['Collection'] == size]['First Execution Time (ms)']
        values_neo4j_first = data_neo4j_query[data_neo4j_query['Collection'] == size]['First Execution Time (ms)']
        plt.bar([f"{size} (MongoDB)", f"{size} (Neo4j)"], [values_mongo_first.values[0], values_neo4j_first.values[0]], color=colors)
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione (ms)')
    plt.title(f'Tempo della Prima Esecuzione per {query}')
    mongo_patch = mpatches.Patch(color=colors[0], label='MongoDB')
    neo4j_patch = mpatches.Patch(color=colors[1], label='Neo4j')
    plt.legend(handles=[mongo_patch, neo4j_patch], loc='upper left')
    plt.tight_layout()
    plt.show()

    # Secondo istogramma - Tempi medi delle successive 30 esecuzion
    plt.figure(figsize=(10, 6))
    for size in dataset:
        values_mongo_avg = data_mongo_query[data_mongo_query['Collection'] == size]['Average Execution Time (ms)']
        values_neo4j_avg = data_neo4j_query[data_neo4j_query['Collection'] == size]['Average Execution Time (ms)']
        mongo_ci_min = data_mongo_query[data_mongo_query['Collection'] == size]['Confidence Interval Min (ms)'].values[0]
        mongo_ci_max = data_mongo_query[data_mongo_query['Collection'] == size]['Confidence Interval Max (ms)'].values[0]
        neo4j_ci_min = data_neo4j_query[data_neo4j_query['Collection'] == size]['Confidence Interval Min (ms)'].values[0]
        neo4j_ci_max = data_neo4j_query[data_neo4j_query['Collection'] == size]['Confidence Interval Max (ms)'].values[0]
        mongo_ci = (mongo_ci_max - mongo_ci_min) / 2
        neo4j_ci = (neo4j_ci_max - neo4j_ci_min) / 2
        plt.bar([f"{size} (MongoDB)", f"{size} (Neo4j)"],
                [values_mongo_avg.values[0], values_neo4j_avg.values[0]],
                yerr=[[mongo_ci, neo4j_ci]],
                color=colors, capsize=10)
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione medio (ms)')
    plt.title(f'Tempo di Esecuzione Medio per {query}')
    plt.legend(handles=[mongo_patch, neo4j_patch], loc='upper left')
    plt.tight_layout()
    plt.show()