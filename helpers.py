import paho.mqtt.client as paho
import sys
import time
import requests

# helper method to connect to mqtt broker via paho client
def connectClient(BROKER_ADDRESS):
      client = paho.Client()

      if client.connect(BROKER_ADDRESS, 1883, 60) != 0:
            print("Could not connect to MQTT Broker.")
            sys.exit(-1)
      print("Connected to MQTT broker")

      return client

# helper method to disconnect paho client from mqtt broker 
def disconnectClient(client):
      print("Disconnecting from MQTT Broker.")
      client.disconnect()

# input: mqtt message
# output: targeted topic of message 
# output type: str
def getDeviceTopicFromMsg(msg):
      return msg.topic.split('/')[1]

# input: mqtt message
# output: classification of mqtt message type (last slice of the whole topic of the message)
# output type: str
def getTopicClassificationFromMsg(msg):
      return msg.topic.split('/')[-1]

# input: mqtt message
# output: full topic sent with message, split into individual strings in a list
# output type: str []
def getTopicArrayFromMsg(msg):
      return msg.topic.split('/')

# input: mqtt message
# output: decoded payload of the mqtt message
# output type: str
def getDecodedPayloadFromMsg(msg):
      return msg.payload.decode()

# input: topic/device extracted from mqtt message
# output: element added to global devicesList if not already present
# output type: -
def addToDevicesList(devicesList, topic):
      if topic not in devicesList:
        devicesList.append(topic)

# input: topic/device that sent an mqtt message; payload of that message (sensor data from device)
# output: element added to global currentDeviceSatistics: if topic is already present as a key, the current value is updated
#         with the most recent readings; if topic is not present as a key, a new entry is created in the dictionary
# output type: -
def addToCurrentDeviceStatistics(currentDeviceStatistics, topic, payload):
      currentDeviceStatistics[topic] = payload

# input: topic/device that sent mqtt message; payload of mqtt message (json)
# output: entry added to device entry in global energyConsumptionDict with current time as key and current electricity consumption as value
# output type: - 
def addToEnergyConsumptionDict(energyConsumptionDict, topic, payload):
      t = time.localtime()
      currentTime = time.strftime("%H:%M:%S", t)
      print("addToEnergyConsumption:")
      print(topic)
      print(payload)
      if topic not in energyConsumptionDict.keys():
        energyConsumptionDict[topic] = {}
      energyConsumptionDict[topic][currentTime] = payload['ENERGY']['Current']

# input: -
# output: global devicesList cleared of all entries
# output type: -
def clearDevicesList(devicesList):
      return devicesList.clear()

# input: client id of device to be removed
# output: entry of specified device removed from global devicesList
# output type: -
def removeFromDevicesList(devicesList, client):
      return devicesList.pop(client, None)

# input: -
# output: global currentDeviceSatistics dictionary cleared of all entries
# output type: -
def clearCurrentDeviceSatistics(currentDeviceStatistics):
      return currentDeviceStatistics.clear()

# input: client id of device to be removed
# output: entry of specified device removed from global currentDeviceSatistics
# output type: -
def removeFromcurrentDeviceSatistics(currentDeviceStatistics, client):
      return currentDeviceStatistics.pop(client, None)

# input: -
# output: global energyConsumptionDict cleared of all entries
# output type: -
def clearEnergyConsumptionDict(energyConsumptionDict):
      return energyConsumptionDict.clear()

# input: client id of device to be removed
# output: entry of specified device removed from global energyConsumptionDict
# output type: -
def removeFromEnergyConsumptionDict(energyConsumptionDict, client):
      return energyConsumptionDict.pop(client, None)

# input: topic and payload of mqtt message to be published
# output: specified message is published to mqtt broker
# output type: -
def publishMessage(client, topic, payload):
  info = client.publish(topic, payload, 0)
  info.wait_for_publish()
  print(info.is_published())
  print(f"Message: {payload} was published to topic: {topic}")

# input: mqtt paho client, deviceID to be targeted, duration in seconds for the plug to be toggled on 
# ouput: device with the specified ID is toggled for the specified duration before toggling back
# output type: -
def threadDuration(client, device_id, duration):
     TOGGLE_TOPIC = f"cmnd/{device_id}/POWER"
     publishMessage(client, TOGGLE_TOPIC, "TOGGLE")
     time.sleep(duration)
     publishMessage(client, TOGGLE_TOPIC, "TOGGLE")

# input: mqtt paho client, device_id to be targeted, duration in seconds for plug to be toggled, callback function
# output: 
# output type: -
def threadWait(client, device_id, duration, callback):
     TOGGLE_TOPIC = f"cmnd/{device_id}/POWER"
     publishMessage(client, TOGGLE_TOPIC, "TOGGLE")
     time.sleep(int(duration))
     publishMessage(client, TOGGLE_TOPIC, "TOGGLE")
     requests.put(callback)
