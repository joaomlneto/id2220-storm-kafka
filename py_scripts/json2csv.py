import time
import urllib2
import json
import sys


storm_ui_url = "http://"+sys.argv[1]
topology_string =  storm_ui_url+"/api/v1/topology/"+ sys.argv[2]
sleep_t = int(sys.argv[3])

print "emitted, transferred, completeLatency, acked, failed"

while True:
    response = urllib2.urlopen(topology_string)
    data = json.load(response)
#    result_string = ""
#    result_string = str(data["topologyStats"][3]["emitted"]) + ", "
#    result_string += str(data["topologyStats"][3]["transferred"]) + ", "
#    result_string += data["topologyStats"][3]["completeLatency"] +", "
#    result_string += str(data["topologyStats"][3]["acked"]) +", "
#    result_string += str(data["topologyStats"][3]["failed"])
    print data
    time.sleep(sleep_t)


