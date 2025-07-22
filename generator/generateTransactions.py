import csv
import threading
import os
from models.Transaction import Transaction
from .utils import writeBatch, log

transactionHeaders = ['id', 'source', 'target', 'date', 'time', 'amount', 'currency']

def __generateTransactions(edges, transactionsFile, batchSize, label):
	try:
		os.remove(transactionsFile)
	except OSError:
		pass

	totalNumberOfTransactions = 0

	with open(transactionsFile, 'a') as transactions:
		batch = []
		sourceNodesCount = 0

		for sourceNode, targets in edges.items():
			sourceNodesCount += 1
			if sourceNodesCount % batchSize == 1: log(label + ": generating transactions for source node " + str(sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))
			
			targetNodesCount = 0
			for targetNode, transactionsCount in targets.items():
				targetNodesCount += 1

				for i in range(0, transactionsCount):
					t = Transaction(sourceNode, targetNode)

					batch.append(t.toRow(transactionHeaders))
					totalNumberOfTransactions += 1

				if len(batch) > batchSize:
					writeBatch(transactions, batch)
					batch = []

		if len(batch) != 0:
			writeBatch(transactions, batch)

		log(label + ": TOTAL: generating transactions for source node " + str(sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))

def generateTransactions(files, batchSize):
	print("Reading nodes in memory")
	clientEdges = {}
	companyEdges = {}

	try:
		with open(files['clients-clients-edges'], 'r') as file:
			reader = csv.reader(file, delimiter="|")
			for row in reader:
				if row and len(row) > 1:  # Check if row has enough elements
					if not row[0] in clientEdges:
						clientEdges[row[0]] = {}
					clientEdges[row[0]].update(eval(row[1]))
	except Exception as e:
		print(f"Error reading clients-clients-edges: {e}")

	try:
		with open(files['clients-companies-edges'], 'r') as file:
			reader = csv.reader(file, delimiter="|")
			for row in reader:
				if row and len(row) > 1:  # Check if row has enough elements
					if not row[0] in clientEdges:
						clientEdges[row[0]] = {}
					clientEdges[row[0]].update(eval(row[1]))
	except Exception as e:
		print(f"Error reading clients-companies-edges: {e}")

	try:
		with open(files['clients-atms-edges'], 'r') as file:
			reader = csv.reader(file, delimiter="|")
			for row in reader:
				if row and len(row) > 1:  # Check if row has enough elements
					if not row[0] in clientEdges:
						clientEdges[row[0]] = {}
					clientEdges[row[0]].update(eval(row[1]))
	except Exception as e:
		print(f"Error reading clients-atms-edges: {e}")

	try:
		with open(files['companies-clients-edges'], 'r') as file:
			reader = csv.reader(file, delimiter="|")
			for row in reader:
				if row and len(row) > 1:  # Check if row has enough elements
					if not row[0] in companyEdges:
						companyEdges[row[0]] = {}
					companyEdges[row[0]].update(eval(row[1]))
	except Exception as e:
		print(f"Error reading companies-clients-edges: {e}")

	if clientEdges:
		clientSourcingTransactions = threading.Thread(target = lambda: __generateTransactions(
			clientEdges,
			files['clients-sourcing-transactions'],
			batchSize,
			label='transaction(client->*)'
		))
		clientSourcingTransactions.start()
	else:
		print("No client edges found, skipping client transactions generation")
		clientSourcingTransactions = None

	if companyEdges:
		companyClientTransactions = threading.Thread(target = lambda: __generateTransactions(
			companyEdges,
			files['companies-sourcing-transactions'],
			batchSize,
			label='transaction(company->client)'
		))
		companyClientTransactions.start()
	else:
		print("No company edges found, skipping company transactions generation")
		companyClientTransactions = None

	if clientSourcingTransactions:
		clientSourcingTransactions.join()
	if companyClientTransactions:
		companyClientTransactions.join()

