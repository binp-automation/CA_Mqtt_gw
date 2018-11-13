import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

def testprint(client, userdata, msg):
    print(msg.topic)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("VEPP3/#")
    #client.message_callback_add("VEPP3/#",testprint)

def on_message(client, userdata, msg):
    print(msg.topic+" ")
    #getChannel(msg.topic).updatePv(msg.payload)



client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.176.128")

client.loop_forever()