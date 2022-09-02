import unittest
import access_data

test_dataset_id_01 = "b2ak-trbp"
test_dataset_pk_01 = "id"
test_dataset_id_02 = "h57g-5234"
test_dataset_pk_02 = "sor_id"
test_dataset_id_03 = "h5-5234"
test_dataset_pk_03 = "sensor_id"

class TestAccessData(unittest.TestCase):
    def test_01_request_dataset(self):
        tc01_data = access_data.request_dataset(test_dataset_id_01, test_dataset_pk_01, test_mode=True)  # good                
        self.assertFalse(tc01_data[0].empty)
    def test_02_request_dataset(self):    
        with self.assertRaises(Exception):
            access_data.request_dataset(test_dataset_id_02, test_dataset_pk_02)  # wrong pk            
    def test_03_request_dataset(self):
        with self.assertRaises(Exception):
            access_data.request_dataset(test_dataset_id_03, test_dataset_pk_03)  # wrong dataset id
        

if __name__ == '__main__':
    unittest.main()