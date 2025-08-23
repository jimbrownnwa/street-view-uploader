from utils.sheets import read_sheet

def main():
    print("Running Street View Tool...")
    data = read_sheet()
    print(f"Read {len(data)} rows from sheet.")

if __name__ == "__main__":
    main()
