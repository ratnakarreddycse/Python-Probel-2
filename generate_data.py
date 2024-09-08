import csv
from faker import Faker

fake = Faker()

def generate_large_csv(output_file, num_rows):
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'last_name', 'address', 'date_of_birth'])
        
        for _ in range(num_rows):
            writer.writerow([fake.first_name(), fake.last_name(), fake.address(), fake.date_of_birth()])

if __name__ == "__main__":
    # Adjust num_rows to make it large, e.g., 10 million for a 2GB file
    generate_large_csv('large_dataset.csv', num_rows=10000000)
    print("CSV file generated.")