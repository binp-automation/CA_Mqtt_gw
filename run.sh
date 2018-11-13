EPICS_CA_AUTO_ADDR_LIST=NO
EPICS_CA_ADDR_LIST="127.0.0.1 192.168.176.128"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/devel/epics/base-3.15.5/lib/linux-x86_64/
python ca_mqtt_gw.py

