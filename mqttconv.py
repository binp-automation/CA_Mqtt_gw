import numpy as np
import struct

from wfaccum import WfAccum

import unittest

# MQTT data converter base class
class MqttConv:
    def __init__(self, convcfg):
        pass

    # converts value to list of mqtt messages
    def encode(self, topic, value): # -> [(topic, payload), ...]
        raise NotImplementedError

    # converts mqtt message to value
    def decode(self, topic, payload): # -> value or None
        raise NotImplementedError


class MqttConvInt(MqttConv):
    def __init__(self, convcfg):
        MqttConv.__init__(self, convcfg)

    def encode(self, topic, value):
        return [(topic, struct.pack(">i", value))]

    def decode(self, topic, payload):
        return struct.unpack(">i", payload)[0]


class MqttConvString(MqttConv):
    def __init__(self, convcfg):
        MqttConv.__init__(self, convcfg)

    def encode(self, topic, value):
        return [(topic, value)] # TODO: change when migrate to python3

    def decode(self, topic, payload):
        return payload # TODO: change when migrate to python3


class MqttConvWfInt1(MqttConv): # TODO: remove this type from proto
    def __init__(self, convcfg):
        MqttConv.__init__(self, convcfg)
        self.wfidcnt = 0

    def encode(self, topic, value):
        payload = struct.pack(">iii", (self.wfidcnt, 1, num))
        self.wfidcnt = (self.wfidcnt + 1) % self.si_mod
        return payload

    def decode(self, topic, payload):
        struct.unpack(">iii", payload)[2]


class MqttConvWfInt(MqttConv):
    def __init__(self, convcfg):
        MqttConv.__init__(self, convcfg)
        self.wfidcnt = 0

        self.segsize = convcfg["segment_size_max"]
        self.misize = 2*4 # metainfo size

        self.si_dig = convcfg["segment_index_digits"]
        self.si_mod = 10**self.si_dig

        self.wfaccum = self.WfAccum(
            idxmod=self.si_mod,
            wfdd=convcfg["waveform_queue_size"]
        )

    def wfid_next(self):
        wfid = self.wfidcnt;
        self.wfidcnt = (self.wfidcnt + 1) % self.si_mod
        return wfid

    def segidx(self, num):
        if num >= self.si_mod:
            raise ValueError("Segment index is greater than allowed max value (%d > %d)" % (num, self.si_mod - 1))
        yield str(num).zfill(self.si_dig)

    def encode(self, topic, value):
        wfid = self.wfid_next()
    
        if not topic.endswith("/"):
            topic += "/"

        array = value
        size = len(array)
        output = []
        sds = (self.segsize - self.misize)//4 # segment data size // sizeof(int)

        for i in range((size - 1)//sds + 1):
            meta = struct.pack(">ii", wfid, size)
            data = array[i*sds:(i+1)*sds].astype(">i4").tobytes()
            output.append((
                topic + self.segidx(i),
                meta + data,
            ))

        return output

    def decode(self, topic, payload):
        segidx = int(topic.split("/")[-1])
        meta, data = payload[:self.misize], payload[self.misize:]
        wfid, size = struct.unpack(">ii", meta)
        array = np.ndarray(shape=(-1,), dtype='>i4', buffer=data).astype(np.int32)

        wf = self.wfaccum.push(wfid, segidx, size, array)

        if wf is not None:
            return wf[1]
        else:
            return None

def getmqttconv(dtype, convcfg):
    if dtype == "int":
        return MqttConvInt(convcfg)
    elif dtype == "string":
        return MqttConvString(convcfg)
    elif dtype == "wfint1":
        return MqttConvWfInt1(convcfg)
    elif dtype == "wfint":
        return MqttConvWfInt(convcfg)
    else:
        raise TypeError("Unknown type '%s'" % dtype)


class Test(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

        self.string_pairs = [
            "",
            "abcABC",
            " ",
            "\n\t\r",
        ]

    def test_int(self):
        conv = getmqttconv("int", {})
        pairs = [
            (0, chr(0)*4),
            (1, chr(0)*3 + chr(1)),
            (-1, chr(0xFF)*4),
            (0x7FFFFFFF, chr(0x7F) + chr(0xFF)*3),
            (-0x80000000, chr(0x80) + chr(0x00)*3),
            (0x1234ABCD, "".join([chr(c) for c in [0x12, 0x34, 0xAB, 0xCD]])),
        ]
        for n, b in pairs:
            self.assertEqual(conv.encode("", n)[0][1], b)

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    unittest.TextTestRunner(verbosity=2).run(suite)
