import numpy as np

import unittest


# Waveform concatenator
#   joins segments of one waveform
class WfCat:
    def __init__(self, size):
        self.size = size
        self.segs = {}
        self.dc = 0 # data counter

    def add(self, idx, seg):
        if len(seg) <= 0:
            raise ValueError("Segment must have length > 0")

        if self.dc + len(seg) > self.size:
            raise ValueError("Total length of segments (%d) larger than waveform size (%d)" % (self.dc, self.size))

        self.segs[idx] = seg
        self.dc += len(seg)
        if self.dc == self.size:
            return True
        else:
            return False

    def join(self):
        if self.dc != self.size:
            raise ValueError("Total length of segments (%d) is not equal waveform size (%d)" % (self.dc, self.size))
        seqlen = max(self.segs.keys()) + 1
        seq = []
        for i in range(seqlen):
            if i not in self.segs:
                raise IndexError("Segments must have continuous indexing (missing index %s of %s)" % (i, seqlen))
            seq.append(self.segs[i])
        return np.concatenate(seq)


# Waveform accumulator
#   collects different wavefrom segments, drops outdated waveforms
#   returns waveform when it is complete
class WfAccum:
    def __init__(self, wfdd):
        self.wfdd = wfdd # waveform id drop distance
        self.wfs = {}

    def push(self, wfid, idx, size, array):
        # remove distant incomplete waveforms
        for key in list(self.wfs.keys()):
            d = key - wfid
            if d > 2*self.wfdd or d < -self.wfdd:
                del self.wfs[key]

        if wfid in self.wfs:
            cat = self.wfs[wfid]
            if cat.size != size:
                raise ValueError("Total size of segments of the same waveform mismatch (%d != %d)" % (cat.size, size))
        else:
            cat = WfCat(size)
            self.wfs[wfid] = cat
        
        if cat.add(idx, array):
            # waveform completed
            wf = cat.join()
            # remove previous incomplete waveforms
            for key in list(self.wfs.keys()):
                if key < wfid:
                    del self.wfs[key]
            return (wfid, wf)
        return None

# Waveform compare
#   compares waveforms returned by WfAccum
def wfcmp(wfa, wfb):
    return wfa[0] == wfb[0] and np.array_equal(wfa[1], wfb[1])

# Unittests
class Test(unittest.TestCase):
    def test_wfcat(self):
        cat = WfCat(10)
        self.assertFalse(cat.add(0, np.array([0, 1, 2])))
        self.assertFalse(cat.add(1, np.array([3, 4, 5, 6])))
        self.assertTrue(cat.add(2, np.array([7, 8, 9])))
        self.assertTrue(np.array_equal(
            cat.join(),
            np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        ))

    def test_wfcat_rev(self):
        cat = WfCat(10)
        self.assertFalse(cat.add(2, np.array([7, 8, 9])))
        self.assertFalse(cat.add(1, np.array([3, 4, 5, 6])))
        self.assertTrue(cat.add(0, np.array([0, 1, 2])))
        self.assertTrue(np.array_equal(
            cat.join(),
            np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        ))

    def test_wfcat_unord(self):
        cat = WfCat(10)
        self.assertFalse(cat.add(4, np.array([7, 8])))
        self.assertFalse(cat.add(2, np.array([4])))
        self.assertFalse(cat.add(0, np.array([0, 1])))
        self.assertFalse(cat.add(5, np.array([9])))
        self.assertFalse(cat.add(1, np.array([2, 3])))
        self.assertTrue(cat.add(3, np.array([5, 6])))
        self.assertTrue(np.array_equal(
            cat.join(),
            np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        ))

    def test_wfcat_len_err(self):
        cat = WfCat(6)
        self.assertFalse(cat.add(0, np.array([0, 1, 2])))
        with self.assertRaises(ValueError):
            cat.join()
        with self.assertRaises(ValueError):
            cat.add(1, np.array([]))
        self.assertTrue(cat.add(1, np.array([3, 4, 5])))
        self.assertTrue(np.array_equal(
            cat.join(),
            np.array([0, 1, 2, 3, 4, 5])
        ))
        with self.assertRaises(ValueError):
            cat.add(2, np.array([6]))

    def test_wfcat_idx_err(self):
        cat = WfCat(4)
        self.assertFalse(cat.add(0, np.array([0, 1])))
        self.assertTrue(cat.add(2, np.array([2, 3])))
        with self.assertRaises(IndexError):
            cat.join()

    def test_wfaccum(self):
        accum = WfAccum(2)
        self.assertIsNone(accum.push(1, 0, 6, np.array([0, 1])))
        self.assertIsNone(accum.push(1, 2, 6, np.array([4, 5])))
        self.assertTrue(wfcmp(
            accum.push(1, 1, 6, np.array([2, 3])),
            (1, np.array([0, 1, 2, 3, 4, 5]))
        ))

    def test_wfaccum_1212(self):
        accum = WfAccum(2)
        self.assertIsNone(accum.push(1, 0, 4, np.array([10, 11])))
        self.assertIsNone(accum.push(2, 0, 4, np.array([20, 21])))
        self.assertTrue(wfcmp(
            accum.push(1, 1, 4, np.array([12, 13])),
            (1, np.array([10, 11, 12, 13]))
        ))
        self.assertTrue(wfcmp(
            accum.push(2, 1, 4, np.array([22, 23])),
            (2, np.array([20, 21, 22, 23]))
        ))

    def test_wfaccum_1221(self):
        accum = WfAccum(2)
        self.assertIsNone(accum.push(1, 0, 4, np.array([10, 11])))
        self.assertIsNone(accum.push(2, 0, 4, np.array([20, 21])))
        self.assertTrue(wfcmp(
            accum.push(2, 1, 4, np.array([22, 23])),
            (2, np.array([20, 21, 22, 23]))
        ))
        self.assertIsNone(accum.push(1, 1, 4, np.array([12, 13])))

    def test_wfaccum_drop(self):
        accum = WfAccum(2)
        self.assertIsNone(accum.push(1, 0, 4, np.array([10, 11])))
        self.assertIsNone(accum.push(2, 0, 4, np.array([20, 21])))
        self.assertIsNone(accum.push(3, 0, 4, np.array([30, 31])))
        self.assertIsNone(accum.push(4, 0, 4, np.array([40, 41])))
        self.assertIsNone(accum.push(1, 1, 4, np.array([12, 13])))
        self.assertTrue(wfcmp(
            accum.push(2, 1, 4, np.array([22, 23])),
            (2, np.array([20, 21, 22, 23]))
        ))
        self.assertTrue(wfcmp(
            accum.push(4, 1, 4, np.array([42, 43])),
            (4, np.array([40, 41, 42, 43]))
        ))

    def test_wfaccum_size_err(self):
        accum = WfAccum(2)
        self.assertIsNone(accum.push(1, 0, 4, np.array([0, 1])))
        with self.assertRaises(ValueError):
            accum.push(1, 1, 6, np.array([2, 3, 4, 5]))

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    unittest.TextTestRunner(verbosity=2).run(suite)
