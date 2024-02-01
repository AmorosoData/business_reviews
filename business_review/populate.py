
import os
import json

# Path to the corrected customers.txt file
customers_file = '/Users/namoroso/github_repos/misc_nick_python/business_reviews/business_review/customers.txt'  # Update this to the actual path
# Base directory where reports are stored
reports_dir = '/Users/namoroso/github_repos/misc_nick_python/business_reviews/business_review/reports'

# Function to read and parse the customers file
def read_customers(file_path):
    customers = []
    with open(file_path, 'r') as file:
        content = file.read()
        # Splitting the content by ']\n[' to handle separate JSON arrays
        customer_blocks = content.strip().split(']\n[')
        for block in customer_blocks:
            # Ensuring proper JSON format for each block
            if not block.startswith('['):
                block = '[' + block
            if not block.endswith(']'):
                block += ']'
            # Parsing each corrected block and adding to the customers list
            customers.extend(json.loads(block))
    return customers

# Function to update the config file for a matching directory
def update_config(customer_data, base_dir):
    customer_name = customer_data['name']
    target_dir = os.path.join(base_dir, customer_name)
    if os.path.exists(target_dir):
        config_path = os.path.join(target_dir, 'config')
        with open(config_path, 'w') as config_file:
            # Wrap the customer_data in a list before dumping to ensure it's enclosed in brackets
            json.dump([customer_data], config_file, indent=4)



# Main process
def main():
    customers = read_customers(customers_file)
    for customer in customers:
        update_config(customer, reports_dir)

# Execute the main process
if __name__ == '__main__':
    main()
