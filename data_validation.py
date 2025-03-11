# imports
import pandas as pd
import pandera as pa
from pandera import Column, Check
import os

# define schema
alarm_schema = pa.DataFrameSchema({
    "alarm_id": Column(int, nullable=False),
    "timestamp": Column(str, Check(lambda x: x.str.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$") | x.str.match(r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$")), nullable=False),
    "site_id": Column(int),
    "alarm_code": Column(str, Check.str_matches(r"^ALM\d+$"), nullable=False),
    "severity": Column(str, Check.isin(["Critical", "Major", "Minor", "Warning"]), nullable=False),
    "status": Column(str, Check.isin(["New", "Acknowledged", "Resolved"]), nullable=False)
})

# validate CSV data
def validate_csv(file_path):
    df = pd.read_csv(file_path)
    file_name = os.path.basename(file_path) # to get the name of the file
    output_file = "validation_errors.txt" # create a .txt file to write the errors into

    # use fillna function to fill in missing values because Pandera’s nullable=False does not catch NaN values by default for string columns
    df.fillna({"timestamp": "No string has been Entered"}, inplace=True)
    df.fillna({"alarm_code": "No string has been Entered"}, inplace=True)
    df.fillna({"severity": "No string has been Entered"}, inplace=True) 
    df.fillna({"status": "No string has been Entered"}, inplace=True)

    try:
        alarm_schema.validate(df, lazy=True) # enable lazy validation to catch all errors at once
    except pa.errors.SchemaErrors as e:  # use "SchemaErrors" not "SchemaError"
        failure_cases = e.failure_cases
        if not failure_cases.empty:
            with open(output_file, "w") as file:  # Open the file for writing
                file.write(f"Validation errors found in {file_name}:\n")
                
                for _, error in failure_cases.iterrows():
                    index = error["index"]
                    failure_case = error["failure_case"]
                    failing_row = df.iloc[index]
                    
                    for column in alarm_schema.columns:
                        if failure_case == failing_row[column]:
                            file.write(f"\nColumn '{column}' failed validation:\n")
                            file.write(f"  alarm_id: {failing_row['alarm_id']}\n")
                            file.write(f"  Failure: {failure_case}\n\n")
                            break

    # sanity check in case file can not be opened                            
    except Exception as ex:
        print(f"An error occurred: {ex}")


if __name__ == "__main__":
    validate_csv(r"C:\Users\slore\Desktop\Python Projects\Data_Integrity-Project/data.csv")