import networkx as nx


if __name__ == '__main__':
    G = nx.DiGraph([(1, 2), (1, 3)])
    nx.draw(G)
    print(G)