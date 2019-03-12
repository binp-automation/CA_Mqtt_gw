#!/usr/bin/python

import unittest

import wfaccum
import mqttconv


if __name__ == '__main__':
    suite = unittest.TestSuite()
    for mod in [wfaccum, mqttconv]:
        suite.addTest(unittest.TestLoader().loadTestsFromModule(mod))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    exit(not result.wasSuccessful())
