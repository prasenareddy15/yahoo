import sys

def process_input(input_string):
    # Example of processing the input
    return f"Processed input: {input_string}"

if __name__ == "__main__":
    input_string = sys.argv[1]  # Get the input passed as an argument
    result = process_input(input_string)
    print(result)  # Output that Flask will capture
