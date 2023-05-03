# Imports
from threading import Thread
from fastapi import FastAPI, Response, Request, Form
from pydantic import BaseModel
import paho.mqtt.client as paho
import json
import helpers




# Reuseable String definitions - Ip address of mqtt broker, various useful topics
DEVICE_TOPIC = "delock"
BROKER_ADDRESS = "131.159.6.111"
TOGGLE_TOPIC = f"cmnd/{DEVICE_TOPIC}/POWER"
STATE_TOPIC = f"tele/{DEVICE_TOPIC}/STATE"
STIRRING_STATE_TOPIC = f"tele/{DEVICE_TOPIC}/stiring/STATE"
SENSORS_TOPIC = "tasmota/discovery/C44F33D7B423/sensors"
SENSOR_TOPIC = f"tele/{DEVICE_TOPIC}/SENSOR"

# Request Body Pydantic Model declarations
class Duration(BaseModel):
      duration: int

class Topic(BaseModel):
     newTopic: str

# Gloabal variables
# energyConsumptionDict - key: timestamp of current time; value: measure of electricity consumption at the specified time
# devicesList - List of devices/topics connected to the mqtt broker that can be adressed
# currentDeviceSastistics - key: device/topic; value: most recent data from device sensor sent via mqtt
energyConsumptionDict = {}
devicesList = []
currentDeviceStatistics = {}

# initialize fastAPI
app = FastAPI()    

# The callback for when the client receives a CONNACK response from the server.
def onConnect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")

# MQTT callback function - gets called everytime a message is received to which the client is subscribed to
def onMessage(client, userdata, msg):
    topicArray = helpers.getTopicArrayFromMsg(msg)
    deviceTopic = helpers.getDeviceTopicFromMsg(msg)
    topicClassification = helpers.getTopicClassificationFromMsg(msg)
    payload = helpers.getDecodedPayloadFromMsg(msg) or ""
    
    helpers.addToDevicesList(devicesList, deviceTopic)
    
    if(topicClassification == "SENSOR"):
          helpers.addToEnergyConsumptionDict(energyConsumptionDict, deviceTopic, json.loads(payload))
          helpers.addToCurrentDeviceStatistics(currentDeviceStatistics, deviceTopic, json.loads(payload))

    print("CLIENT:")
    print(client)
    
    print("USERDATA:")
    print(userdata)
    
    print("MESSAGE:")
    print(topicArray)
    print(payload)

    print("DEVICE TOPIC:")
    print(deviceTopic)
    
    print("DEVICES LIST:")
    print(devicesList)
          
    print("CURRENT DEVICE STATISTICS:")
    print(currentDeviceStatistics)

    print("DEVICE ELECTRICITY CONSUMPTION OVER TIME:")
    print(energyConsumptionDict)

# Request path: /
# Request Type: GET
# Params: -
# Expected Behavior: Returns a list of all devices connected to mqtt broker
@app.get("/")
def getDevices():
    return devicesList

# Request path: /statistics/
# Request Type: GET
# Params: -
# Expected Behavior: Returns list of current sensor data of all devices connected to mqtt broker
@app.get("/statistics/")
def getStatistics():
      return currentDeviceStatistics

# Request path: /*deviceID*/
# Request Type: GET
# Params: device ID for which to retrieve most recent sensor data
# Expected Behavior: Returns some statistics on the specific device requested
@app.get("/{device_id}/statistics/")
def getDeviceByID (device_id: str):
  return currentDeviceStatistics[device_id]

# Request path: /energyConsumption
# Request Type: GET
# Params: -
# Expected Behavior: Returns energy consumption of all devices over time
@app.get("/energyConsumption/")
def getAllEnergyConsumption():
      return energyConsumptionDict

# Request path: /*deviceID*/energyConsumption/
# Request Type: GET
# Params: device ID for which to receive energy consumption data
# Expected Behavior: Returns energy consumption over time of specified device
@app.get("/{device_id}/energyConsumption/")
def getDeviceEnergyConsumption(device_id: str):
      return energyConsumptionDict[device_id]

# Request path: /
# Request Type: DELETE
# Params: -
# Expected Behavior: Clears the global list of all devices connected to mqtt broker
@app.delete("/")
def clearDevices():
      return devicesList.clear()

# Request path: /*deviceID*/
# Request Type: DELETE
# Params: device ID to be removed from global list
# Expected Behavior: removes specified device from global list of all devices connected to mqtt broker
@app.delete("/{device_id}/")
def removeDeviceById(device_id: str):
      return helpers.removeFromDevicesList(devicesList, device_id)

# Request path: /statistics
# Request Type: DELETE
# Params: -
# Expected Behavior: Clears the global list of current sensor data per device
@app.delete("/statistics/")
def clearDeviceStatistics():
      return helpers.clearCurrentDeviceSatistics(currentDeviceStatistics)

# Request path: /*deviceID*/statistics/
# Request Type: DELETE
# Params: device ID for which to remove satistics from global dictionary
# Expected Behavior: removes specified device from global dictionary of current sensor data per device
@app.delete("/{device_id}/statistics/")
def removeDeviceSatisticsById(device_id: str):
      return helpers.removeFromcurrentDeviceSatistics(currentDeviceStatistics, device_id)

# Request path: /energyConsumption/
# Request Type: DELETE
# Params: -
# Expected Behavior: Clears the global dictionary of energy consumption over time per device
@app.delete("/energyConsumption/")
def clearEnergyConsumption():
      return helpers.clearEnergyConsumptionDict(energyConsumptionDict)

# Request path: /*deviceID*/energyConsumption
# Request Type: DELETE
# Params: device ID for which to remove energy consumption data from global dictionary
# Expected Behavior: removes the energy consumption over time entry for the specified device from global dictionary
@app.delete("/{device_id}/energyConsumption/")
def removeEnergyConsumptionById(device_id: str):
      return helpers.removeFromEnergyConsumptionDict(energyConsumptionDict, device_id)

# Request path: /*deviceID*/
# Request Type: PUT
# Params: New Device ID
# Expected Behavior: Replace current Device ID with new one passed in Request
@app.put("/{device_id}/")
def nameDevice (device_id: str, newTopic: Topic):
    # cmnd/delock/Topic -m delock1
    topic = newTopic.newTopic
    RENAME_TOPIC_TOPIC = f"cmnd/{device_id}/Topic"
    helpers.publishMessage(client, RENAME_TOPIC_TOPIC, topic)

# Request path: /*deviceID*/state/
# Request Type: PUT
# Params: device ID for which to toggle the current POWER state
# Expected Behavior: Toggle Power state of Device with *deviceID*
@app.put("/{device_id}/state/")
def togglePower (device_id: str):
  TOGGLE_TOPIC = f"cmnd/{device_id}/POWER"
  helpers.publishMessage(client, TOGGLE_TOPIC, "TOGGLE")

# Request path: /*deviceID*/duration/
# Request Type: PUT (Synchronous)
# Params: Duration in Seconds
# Expected Behavior: Device should wait as long as specified with duration before toggling again
@app.put("/{device_id}/duration/")
def setDuration (device_id: str, duration: Duration):
      duration = duration.duration
      thread = Thread(target=helpers.threadDuration, args=[client, device_id, duration])
      thread.start()
      

# Request path: /*deviceID*/wait/
# Request Type: PUT (Asynchronous)
# Params: Duration in Seconds
# Expected Behavior: The running process instance should wait for the specified duration and receive data concerning electricity consumption
@app.put("/{device_id}/wait/")
async def setWait(device_id: str, response: Response, request: Request, duration: str = Form(...)):
       callback = request.headers.get('cpee-callback')
       thread = Thread(target=helpers.threadWait, args=[client, device_id, duration, callback])
       thread.start()
       response.headers['CPEE-CALLBACK'] = 'true'
       return

# connect paho client to mqtt broker
print("Connecting client..")
client = helpers.connectClient(BROKER_ADDRESS)

# set the mqtt callback function to call when messages are received on topics subscribed to
print("setting callback functions..")
client.on_message = onMessage
client.on_connect = onConnect

# initialize the client loop
print("starting client loop..")
client.loop_start()

#helpers.disconnectClient(client)
