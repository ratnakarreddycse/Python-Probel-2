import unittest
from anonymize_data import anonymize_row

class TestAnonymization(unittest.TestCase):
    
    def test_anonymize_row(self):
        original_row = ['John', 'Doe', '123 Elm St', '1990-01-01']
        anonymized_row = anonymize_row(original_row.copy())
        
        self.assertNotEqual(original_row[0], anonymized_row[0])  # First name
        self.assertNotEqual(original_row[1], anonymized_row[1])  # Last name
        self.assertNotEqual(original_row[2], anonymized_row[2])  # Address

if __name__ == '__main__':
    unittest.main()