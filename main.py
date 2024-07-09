import pandas as pd
import streamlit as st
import importlib

# Set up Streamlit page config
st.set_page_config(page_title="Monthly MIS Checker", layout="wide")

def main():
    st.title("MIS Reviewer :chart_with_upwards_trend:")
    
    # Allow user to upload an Excel file
    uploaded_file = st.sidebar.file_uploader('Upload Excel file', type=['xlsx', 'xls'])

    if not uploaded_file:
        st.write("Please upload an Excel file to proceed.")
        return

    # Cache the uploaded file to avoid re-uploading
    @st.cache_data
    def load_excel(file):
        return pd.ExcelFile(file)

    try:
        excel_file = load_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading the Excel file: {e}")
        return

    sheet_names = excel_file.sheet_names
    selected_sheet = st.sidebar.selectbox('Select a sheet to display', sheet_names)

    @st.cache_data
    def load_sheet(file, sheet_name):
        df = pd.read_excel(file, sheet_name=sheet_name, header=1)
        df.columns = df.columns.str.lower().str.strip()
        columns_to_convert = df.columns.difference(['date'])
        df[columns_to_convert] = df[columns_to_convert].apply(
            lambda col: col.str.lower().str.strip() if col.dtype == 'object' else col
        )
        return df

    try:
        df = load_sheet(uploaded_file, selected_sheet)
    except Exception as e:
        st.error(f"Error processing the data: {e}")
        return

    if 'month' not in df.columns:
        st.write("No 'month' column found in this sheet.")
        return

    month = st.sidebar.selectbox("Select the month for review", df['month'].unique())
    df_filtered = df[df['month'] == month]

    # Define sheet name lists for each business logic
    business_logic_sheets = {
        "business_logic_1": ["Postman"],
        "business_logic_2": ["Pratilipi"],
        "business_logic_3": ["Quzizz", "Synergy", "Amadeus", "Awfis"],
        "business_logic_4": ["Medtrix", "Odessa", "MG Eli Lilly", "Scaler-Prequin"],
        "business_logic_5": ["Gojek", "Microchip Main Meal"],
        "business_logic_6": ["HD Works"],
        "business_logic_7": ["MPL"],
        "business_logic_8": ["Tonbo", "Tadano Escorts", "Siemens - Tuckshop", "Dynasty", "Citrix Driver's Lunch & Dinner", "sharefile", "NewDynasty"],
        "business_logic_9": ["Rippling", "Tessolve"],
        "business_logic_10": ["MPL - Infinity Plates", "Tekion.", "Groww Koramangala", "Groww VTP", "MIQ", "Groww Mumbai", "Ather Mumbai", "Epam"],
        "business_logic_11": ["Telstra MainMeal(Cash & Carry)"],
        "business_logic_12": ["Eli Lilly Wallet", "Sheet1"], 
        "business_logic_13": ["Sinch", "O9 Solutions"],
        "business_logic_14": ["RAKUTEN-2", "Clario"],
        "business_logic_15": ["Waters Main Meal"],
        "business_logic_16": ["Quest Company Paid"],
        "business_logic_17": ["Waters Tuck Shop"],
        "business_logic_18": ["H&M"],
        "business_logic_19": ["Lam Research", "Corning", "PhonePe"],
        "business_logic_20": ["Micochip Juice Junction"],
        "business_logic_21": ["Ather BLR"],
        "business_logic_22": ["Ather Plant 1.", "Ather Plant 2.", "SAEL Delhi", "Gojek."],
        "business_logic_23": ["STRIPE MIS", "TEA-Breakfast"],
        "business_logic_24": ["FRUIT N JUICE MIS"],
        "business_logic_25": ["Siemens", "Toasttab", "Gartner"],
        "business_logic_26": ["DTCC Wallet"],
        "business_logic_27": ["Siemens_Pune"],
        "business_logic_28": ["CSG-Pune"],
        "business_logic_29": ["Salesforce-GGN"],
        "business_logic_30": ["Salesforce - Jaipur"],
        "business_logic_31": ["Ather - Main Meal"],
        "business_logic_32": ["Siemens."], 
        "business_logic_33": ["Postman.", "Citrix-Tuckshop"],
        "business_logic_34": ["Sinch Lunch"],
        "business_logic_35": ["Sinch Dinner"],
        "business_logic_36": ["STRYKER MIS - '2024"],
        "business_logic_37": ["EGL"],
        "business_logic_38": ["Truecaller"],
        "business_logic_39": ["Sharefile Wallet"],
        "business_logic_40": ["Gold Hill-Main Meal", "Goldhill Juice Junction.", "Healthineer International", "Priteck - Main meal", "Pritech park Juice junction"],
        "business_logic_41": ["Siemens-BLR", "Siemens Juice Counter"],
        "business_logic_42": ["Heathineer Factory"],
        "business_logic_43": ["Airtel Center", "Airtel Plot 5", "Airtel NOC Non veg", "Airtel international"],
        "business_logic_44": ["Tekion"],
        "business_logic_45": ["HD Works(HYD)"],
        "business_logic_46": ["Airtel Noida"],
        "business_logic_47": ["Airtel NOC"],
        "business_logic_48": ["Airtel-Jaya"],

        "event_logic_1": ["Telstra Event.", "Telstra Event", "Events"],
        "event_logic_2": ["Eli Lilly Event"],
        "event_logic_3": ["Waters Event"],
        "event_logic_4": ["Icon-event-Bangalore", "Sinch Event sheet", "infosys Event+ Additional Sales", "Other Events.", "Telstra Event sheet", "MPL-Delhi"],
        "event_logic_5": ["Other Events"],
        "event_logic_6": ["Lam Research Event"],
        "event_logic_7": ["ICON CHN EVENT"],
        "event_logic_8": ["other Event MIS"],
        "event_logic_9": ["Amazon PNQ Events -"],

        "other_revenues": [""]
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
        except Exception as e:
            st.error(f"Error applying business logic: {e}")
    else:
        st.write("No business logic defined for this sheet.")

if __name__ == "__main__":
    main()
