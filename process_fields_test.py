import unittest
import process_fields
from datetime import datetime
import pandas as pd

class TestProcessFields(unittest.TestCase):
    def test_01_str_to_dt(self):
        str_1 = '2017-10-01 04:00:00'
        dt_1 = process_fields.str_to_dt(str_1)  
        str_2 = '01/10/2017 00:00:00'
        dt_2 = process_fields.str_to_dt(str_2)
        str_3 ='2009-07-02T09:00:00.000' 
        dt_3 = process_fields.str_to_dt(str_3)
        self.assertTrue(isinstance(dt_1,datetime))
        self.assertTrue(isinstance(dt_2,datetime))
        self.assertTrue(isinstance(dt_3,datetime))

    def test_02_str_to_dt(self):
        str_4 ='text' 
        with self.assertRaises(Exception): 
            dt_4 = process_fields.str_to_dt(str_4)

    def test_03_str_to_dt(self):        
        str_5 =100 
        with self.assertRaises(Exception):
            dt_5 = process_fields.str_to_dt(str_5)

    def test_04_process_fields(self):
        df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['A', 'A', 'B'],
        'col3': [2.2, 3.3, 4.4],
        'col4': ['c', 3.3, 4.4]
        })        
        result_1 = process_fields.process_column(df,'col1', 'int') # okay
        result_2 = process_fields.process_column(df,'col2', 'str') # okay
        result_3 = process_fields.process_column(df,'col3', 'float') # okay
        result_4 = process_fields.process_column(df,'col4', 'int') # invalid
        result_5 = process_fields.process_column(df,'col4', 'str') # okay

        self.assertEqual(result_1,(3,0))
        self.assertEqual(result_2,(3,0))
        self.assertEqual(result_3,(3,0))
        self.assertEqual(result_4,(2,1))
        self.assertEqual(result_5,(3,0))        

if __name__ == '__main__':
    unittest.main()