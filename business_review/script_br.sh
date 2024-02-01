# Check if the number of arguments is correct
if [ $# -ne 1 ]; then
  echo "Usage: $0 \"customer1:date1,customer2:date2,...\""
  exit 1
fi

# Extract the comma-separated list of customer:date pairs
input="$1"

# Split the input into an array using comma as the delimiter
IFS=',' read -a customer_dates <<< "$input"

# Loop through the customer:date pairs
for customer_date in "${customer_dates[@]}"; do
    # Extract customer and date from each pair
    customer=$(echo "$customer_date" | cut -d':' -f1)
    date=$(echo "$customer_date" | cut -d':' -f2)

    # Execute the command with extracted values
    python business_review.py --customer="$customer" --date="$date"
done
