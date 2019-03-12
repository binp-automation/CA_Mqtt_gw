# CA-MQTT Gateway

[![Travis CI][travis_badge]][travis]
[![License][license_badge]][license]

[travis_badge]: https://api.travis-ci.org/binp-automation/ca_mqtt_gw.svg
[license_badge]: https://img.shields.io/github/license/binp-automation/ca_mqtt_gw.svg

[travis]: https://travis-ci.org/binp-automation/ca_mqtt_gw
[license]: https://github.com/binp-automation/ca_mqtt_gw/blob/master/LICENSE

## Requirements

### EPICS Base

Version `>= 3.15`

### Packages (Debian / Ubuntu)

+ `python2`
+ `libpython-dev`
+ `python-qt4`

### Python2 libs

+ `numpy`
+ `paho-mqtt`
+ `cothread`


## Usage

### Prepare

```bash
apt-get install python2 libpython-dev python-qt4
pip2 install -r requirements.txt
```

### Run

```bash
export EPICS_BASE=/path/to/epics-base
export EPICS_HOST_ARCH=$($EPICS_BASE/startup/EpicsHostArch)
export EPICS_CA_MAX_ARRAY_BYTES=65536
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$EPICS_BASE/lib/$EPICS_HOST_ARCH/

python2 ca_mqtt_gw.py gateway_config.json
```

### Tests

```bash
python2 test.py
```
