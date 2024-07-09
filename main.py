import pandas as pd
import dask.dataframe as dd
import logging
import streamlit as st
import importlib
from concurrent.futures import ThreadPoolExecutor

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def read_excel_file(uploaded_file):
    try:
        # Read Excel file using pandas
        excel_file = pd.ExcelFile(uploaded_file)
        logging.info("Excel file uploaded successfully.")
        return excel_file
    except Exception as e:
        logging.error(f"Error reading the Excel file: {e}")
        st.error(f"Error reading the Excel file: {e}")
        return None

def load_sheet_data(excel_file, selected_sheet):
    try:
        # Read the selected sheet into a pandas dataframe
        # Since headers are on the second row (index 1), we set header=1
        df = pd.read_excel(excel_file, sheet_name=selected_sheet, header=1, engine='openpyxl')
        # Convert all column names to lower case and strip whitespaces
        df.columns = df.columns.str.lower().str.strip()
        # Optionally convert pandas DataFrame to Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=3)
        logging.info(f"Sheet '{selected_sheet}' loaded successfully.")
        return ddf
    except Exception as e:
        logging.error(f"Error reading the sheet '{selected_sheet}': {e}")
        st.error(f"Error reading the sheet '{selected_sheet}': {e}")
        return None

def process_data(df):
    try:
        # Convert columns of type 'object' to lower case and strip, excluding the 'date' column
        columns_to_convert = [col for col in df.columns if df[col].dtype == 'object' and col != 'date']
        df[columns_to_convert] = df[columns_to_convert].apply(
            lambda col: col.str.lower().str.strip()
        )
        logging.info("Data processed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error processing the data: {e}")
        st.error(f"Error processing the data: {e}")
        return None

def filter_by_month(df, month):
    try:
        df_filtered = df[df['month'] == month]
        logging.info(f"Data filtered by month '{month}' successfully.")
        return df_filtered
    except Exception as e:
        logging.error(f"Error filtering data by month: {e}")
        st.error(f"Error filtering data by month: {e}")
        return None

def apply_business_logic(df_filtered, selected_sheet):
    business_logic_sheets = {
        # Define business logic sheets here (for brevity not included in this message)
    }

    business_logic_module = None
    for module_name, sheets in business_logic_sheets.items():
        if selected_sheet in sheets:
            business_logic_module = module_name
            break

    if business_logic_module:
        try:
            module = importlib.import_module(business_logic_module)
            business_logic_function = getattr(module, business_logic_module)
            business_logic_function(df_filtered)
            logging.info(f"Business logic '{business_logic_module}' applied successfully.")
        except Exception as e:
            logging.error(f"Error applying business logic: {e}")
            st.error(f"Error applying business logic: {e}")
    else:
        st.write("No business logic defined for this sheet.")
        logging.warning("No business logic defined for the selected sheet.")

def main():
    st.set_page_config(page_title="Monthly MIS Checker", layout="wide")
    st.title("MIS Reviewer :chart_with_upwards_trend:")

    uploaded_file = st.sidebar.file_uploader('Upload Excel file', type=['xlsx', 'xls'])
    if uploaded_file:
        st.write("File Uploaded Successfully.")  # Debug print
        try:
            with ThreadPoolExecutor() as executor:
                future_excel_file = executor.submit(read_excel_file, uploaded_file)
                excel_file = future_excel_file.result()

                if excel_file:
                    st.write("Excel file loaded.")  # Debug print
                    sheet_names = excel_file.sheet_names
                    selected_sheet = st.sidebar.selectbox('Select a sheet to display', sheet_names)

                    future_df = executor.submit(load_sheet_data, excel_file, selected_sheet)
                    df = future_df.result()

                    if df is not None:
                        st.write("Dataframe loaded.", df.head())  # Debug print
                        df = process_data(df)

                        if df is not None and 'month' in df.columns:
                            month = st.sidebar.selectbox("Select the month for review", df['month'].unique())

                            future_df_filtered = executor.submit(filter_by_month, df, month)
                            df_filtered = future_df_filtered.result()

                            if df_filtered is not None:
                                st.write("Filtered Data", df_filtered.head())  # Debug print
                                apply_business_logic(df_filtered, selected_sheet)
                            else:
                                st.error("Error filtering data by month.")
                        else:
                            st.write("No 'month' column found in this sheet.")
                    else:
                        st.error("Failed to load data from sheet.")
                else:
                    st.error("Failed to read Excel file.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.write("Please upload an Excel file to proceed.")

if __name__ == "__main__":
    main()
