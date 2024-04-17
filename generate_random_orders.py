import random

items = ["F", "I", "G", "A", "U"]
nb = 50

with open("data/orders.txt", mode="w") as f:
    for i in range(nb):
        random.shuffle(items)
        print(str(items)[1:-1].replace(", ", "<").replace("'",""), file=f)
