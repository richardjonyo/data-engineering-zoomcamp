import pandas as pd

def test_pandas():
    # Sample data
    data = {'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 22],
            'City': ['New York', 'London', 'Paris']}

    # Create a pandas DataFrame
    df = pd.DataFrame(data)

    # Display the DataFrame
    print(df)

if __name__ == "__main__":
    test_pandas()
