import json
import sys
import ast
import matplotlib.pyplot as plt


filename = sys.argv[1]
x = []

with open(filename) as logs :
    for line in logs :
        current_point = ast.literal_eval(line)
	#current_point = json.loads(line);
        print(current_point["topologyStats"][3]["acked"])
	x.append(current_point["topologyStats"][3]["acked"])

plt.plot(x)
plt.show()

