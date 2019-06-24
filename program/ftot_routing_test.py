# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 12:12:44 2017

@author: Matthew.Pearlson.CTR
"""

import unittest

class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertFalse('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

#def run_test(): 
#    
#    unittest.main()

#if __name__ == '__main__':
#    unittest.main()
#
#if __name__ == "__main__":
#     unittest.main()
#else:
#     unittest.main()

unittest.main()