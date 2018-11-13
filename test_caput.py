import cothread
import numpy as np
from cothread.catools import *


randnums = np.random.randint(1,90,4000)

sinarr = []
for i in range(0,1000):
    sinarr.append((np.sin(i*np.pi/200))*15000000)

app = cothread.iqt()
caput("VEPP3_H_Iwf-SP",sinarr)
#caput("VEPP3_H_DAC-I",200.0)
