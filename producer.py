from kafka import KafkaProducer
import json
import time
import re

producer_obj = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer=lambda data: json.dumps(data).encode('utf-8'))

def registered_item(exp):
    ip = exp[0:exp.find('-')]
    tempDataRex = re.findall(r'"([^"]*)"', exp)
    page = tempDataRex[0].split(' ')[1]
    category = tempDataRex[0].split('/')[1].strip()
    subCategory = tempDataRex[0].split('/')[2].strip()
    item_obj = {
        "IP": ip,
        "category1": category,
        "category2": subCategory,
        "page": page
    }
    return item_obj

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    with open('Weblogs.csv') as f:
        while True:
            line = f.readline()
            if not line:
                break
            item1 = line.strip()
            item = registered_item(item1)
            print(item)
            producer_obj.send("registered_user", item)
            time.sleep(0.8)

    print("Kafka Producer Application Completed ... ")
