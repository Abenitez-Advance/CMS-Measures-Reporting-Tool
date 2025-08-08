import pandas as pd
import re

# Settings
RELEVANT_SHEETS = ["Client", "Ahpscreening", "Goalshortterm", "Ahpdischarge", "Interaction", "Interaction_referral"]
# Default start date for metrics calculations
DEFAULT_START_DATE = '2023-01-01'
# Default end date for metrics calculations
DEFAULT_END_DATE = '2023-12-31'
# default path to the Excel file
DEFAULT_EXCEL_PATH = 'data/metrics_data.xlsx'
DEFAULT_OUTPUT_PATH = 'data/metrics_output.csv'
# - Enrollment status values (list):
ENROLLED_STATUSES = ['Engaged']
SERVICES_PROVIDED = ["Care Coordination", "Referral to Services", "Education Provided", "Services Provided- Ongoing work with client"] # Define the services that count for Metric #8 and #9
# ====- Metric #1 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC1_SHEET = 'Client'
# - Date filter column:
METRIC1_DATE_COL = 'Client_CreateStamp'
# - Referral type column; This column is checked for any non-null, non-empty value to count a referral.
METRIC1_REFERRALTYPE_COL = 'ClientOption_WhatTypeOfReferralIsThis'

# ====- Metric #2 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC2_SHEET = 'Client'
# - Date filter column:
METRIC2_DATE_COL = 'Client_CreateStamp'
# - client Id column:
METRIC2_CLIENTID_COL = 'Client_Id'
# - Referral type column; This column is checked for any non-null, non-empty value to count a referral.
METRIC2_REFERRALTYPE_COL = 'ClientOption_WhatTypeOfReferralIsThis'
# - Duplicate Record checking Column; The column we will check for the METRIC2_DUPLICATE_VALUE
METRIC2_DUPLICATE_COL = 'ClientOption_AhpClientStatus'
# - Duplicate Record value; The value we will check for in the METRIC2_DUPLICATE_COL to exclude those records from the count.
METRIC2_DUPLICATE_VALUE = 'Inactive-Duplicate Record'

# ====- Metric #3 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC3_SHEET = 'Client'
# - Client ID column:
METRIC3_CLIENTID_COL = 'Client_Id'
# - Date filter column:
METRIC3_DATE_COL = 'Client_CreateStamp'
# - Enrollment status column:
METRIC3_STATUS_COL = 'ClientOption_CareConnectStatus'
# - Edit stamp column (for sorting):
METRIC3_EDITSTAMP_COL = 'Client_EditStamp'

# ====- Metric #4 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC4_SHEET = 'Ahpscreening'
# - Client ID column:
METRIC4_CLIENTID_COL = 'Client_Id'
# - Edit stamp column:
METRIC4_EDITSTAMP_COL = 'Ahpscreening_EditStamp'
# - Cantrils Ladder columns:
METRIC4_CL1_COL = 'AhpscreeningOption_WellbeingCantrilsLadder1'
METRIC4_CL2_COL = 'AhpscreeningOption_WellbeingCantrilsLadder2'

# ====- Metric #5 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC5_SHEET = 'Ahpscreening'
# - Client ID column:
METRIC5_CLIENTID_COL = 'Client_Id'
# - SDOH assessment date column:
METRIC5_SDOH_DATE_COL = 'AhpscreeningSystem_DateAcceptedcompleted'

# ====- Metric #6 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC6_SHEET = 'Client'
# - Client ID column:
METRIC6_CLIENTID_COL = 'Client_Id'
# - Enrollment status column:
METRIC6_STATUS_COL = 'ClientOption_CareConnectStatus'
# - Enrollment status values (list):
METRIC6_STATUS_VALUES = ['Engaged', 'Enrolled (Assigned)']
# - Edit stamp column:
METRIC6_EDITSTAMP_COL = 'Client_EditStamp'
# - Opt-in date column:
METRIC6_OPTIN_DATE_COL = 'ClientSystem_CcOptinDate'

# ====- Metric #7 Settings -====:
# - Main sheet name for the metric in the Excel file:
METRIC7_SHEET = 'Interaction_referral'
# - Client ID column:
METRIC7_CLIENTID_COL = 'InteractionReferral_ReferralsModule_client_id'
# - Taxonomy name column:
METRIC7_TAXONOMY_COL = 'InteractionReferralTaxonomy_Taxonomy_external_term_name'
# - Referral date column:
METRIC7_REFERRAL_DATE_COL = 'InteractionReferral_ReferralsModule_referral_status_requested_date'

# ====- Metric #8 & #9 Settings -====:
# - Main sheet names:
METRIC8_CLIENT_SHEET = 'Client'
METRIC8_INTERACTION_SHEET = 'Interaction'
METRIC8_AHPSCREENING_SHEET = 'Ahpscreening'
# - Client ID column:
METRIC8_CLIENTID_COL = 'Client_Id'
# - Referral date column:
METRIC8_REFERRAL_DATE_COL = 'ClientSystem_CcProgramReferralDate'
# - Interaction outcome column:
METRIC8_OUTCOME_COL = 'InteractionOption_ContactOutcome'
# - Interaction date column:
METRIC8_INTERACTION_DATE_COL = 'Interaction_CreateStamp'
# - SDOH assessment date column:
METRIC8_SDOH_DATE_COL = 'AhpscreeningSystem_DateAcceptedcompleted'

# ====- Metric #10-16 Settings -====:
# These are calculated from previous metrics and do not require sheet/column settings.
 
# Metric #1
# Number of Inbound Referrals into the CCH (CCO-1)
# Date start filter column: ClientSystem_CcProgramReferralDate
# Date end filter column: ClientSystem_CcProgramReferralDate
# Relevant columns in excel:
# - Sheet: Client
# - Client_Id
# - ClientOption_WhatTypeOfReferralIsThis
# - Client_CreateStamp
#  in order to count a referral, we count up all records that have a value in the ClientOption_WhatTypeOfReferralIsThis column
def calculate_inbound_referrals(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
    """
    Number of Inbound Referrals into the CCH (CCO-1)
    Counts all records that have a non-null value in the ClientOption_WhatTypeOfReferralIsThis column.
    Optionally filters by Client_CreateStamp if present in df and date filtering is desired.
    """
    if METRIC1_REFERRALTYPE_COL not in df.columns:
        return 0
    # filter by client Create Stamp within the date range
    if METRIC1_DATE_COL in df.columns:
        df[METRIC1_DATE_COL] = pd.to_datetime(df[METRIC1_DATE_COL], errors='coerce')
        df = df[(df[METRIC1_DATE_COL] >= start_date) & (df[METRIC1_DATE_COL] <= end_date)]
    # Only count rows with a non-null, non-empty referral type
    filtered = df[df[METRIC1_REFERRALTYPE_COL].notnull() & (df[METRIC1_REFERRALTYPE_COL].astype(str).str.strip() != '')]
    return len(filtered)


# Metric #2
# Number of unique Individuals Referred into the CCH (NCCCH add-on)
# Date start filter column: ClientSystem_CcProgramReferralDate
# Date end filter column: ClientSystem_CcProgramReferralDate
# Relevant columns in excel:
# - Sheet: Client
# - Client_Id
# - ClientOption_WhatTypeOfReferralIsThis
# - ClientSystem_CcOptinDate (maybe)
# - ClientOption_AhpClientStatus
# - Possible Options:
# - ["Active"]
# - ["Inactive"]
# - ["Inactive-Duplicate Record"]
# - ["New"]
#  ASSUMPTION: the ClientOption_AhpClientStatus Column value will never be null/empty, if it is then we will not count that row.
# in order to count a unique individual, we want to track the first instance of each Client_Id where ClientOption_AhpClientStatus is not null/empty so that we count each individual only once even if they are referred multiple times. We also want to ignore any Client_Id's that have a ClientOption_AhpClientStatus of "Inactive-Duplicate Record".
def calculate_unique_individuals_referred(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
    """
    Number of unique Individuals Referred into the CCH (NCCCH add-on)
    Counts unique Client_Id values where ClientOption_WhatTypeOfReferralIsThis is not null/empty.
    """
    required_cols = [METRIC2_CLIENTID_COL, METRIC2_REFERRALTYPE_COL, METRIC2_DUPLICATE_COL]
    for col in required_cols:
        if col not in df.columns:
            print(f'[DEBUG] Required column missing for Metric #2: {col}')
            return 0
        # Ensure date filtering if applicable
    if METRIC2_DATE_COL in df.columns:
        df[METRIC2_DATE_COL] = pd.to_datetime(df[METRIC2_DATE_COL], errors='coerce')
        df = df[(df[METRIC2_DATE_COL] >= start_date) & (df[METRIC2_DATE_COL] <= end_date)]
    filtered = df[
        df[METRIC2_REFERRALTYPE_COL].notnull() &
        (df[METRIC2_REFERRALTYPE_COL].astype(str).str.strip() != '') &
        df[METRIC2_DUPLICATE_COL].notnull() &
        (df[METRIC2_DUPLICATE_COL].astype(str).str.strip() != '') &
        (df[METRIC2_DUPLICATE_COL].astype(str).str.strip() != METRIC2_DUPLICATE_VALUE)
    ]
    print(f"[DEBUG] Metric #2: Filtered rows with non-null/empty referral type and valid status: {len(filtered)}")
    unique_clients = filtered[METRIC2_CLIENTID_COL].nunique()
    print(f"[DEBUG] Metric #2: Unique Client_Id count (excluding '{METRIC2_DUPLICATE_VALUE}'): {unique_clients}")
    return unique_clients


# Metric #3
# Number of Enrolled Clients (CCO-2)
# Relevant columns in excel:
# - Sheet: Client
# - ClientOption_CareConnectStatus (program status)
# - Client_Id
# - Client_EditStamp
# - ClientSystem_CcOptinDate (date of enrollment)
# Date start filter column: Client_EditStamp
# Date end filter column: ClientSystem_CcOptinDate
# For this metric we want to determine the status of a client by finding that most recent ClientOption_CareConnectStatus value for each Client_Id, and then counting the number of unique Client_Id values that have a status of "Enrolled (Assigned)", or "Engaged".
# potential statuses: 
# [1459 => "Enrolled (Unassigned)", 1460 => "Enrolled (Assigned)", 1461 => "Outreach", 1462 => "Engaged", 1463 => "Not Enrolled", 1778 => "Pending Discharge- CCO Manager", 1779 => "Pending Discharge- Lead", 1780 => "Discharged (AHP Only)", 1963 => "Discharged (AHP Only-Outreach)", 1964 => "Discharged (AHP Only-Engaged)", 1995 => "Referral"]
# ASSUMPTION: Each row of data in the client tab of the sheet is a unique record of an interaction such that we can track changes in status over time.
def calculate_enrolled_clients(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple:
    """
    Number of Enrolled Clients (CCO-2)
    For each Client_Id, find the most recent ClientOption_CareConnectStatus (by Client_EditStamp).
    Count unique Client_Id values where the latest status contains any of the enrolled status strings.
    Returns (count, list_of_enrolled_client_ids)
    """
    if METRIC3_CLIENTID_COL not in df.columns or METRIC3_STATUS_COL not in df.columns or METRIC3_EDITSTAMP_COL not in df.columns:
        return 0, []
    # Only restrict by Client_CreateStamp <= end_date
    if METRIC3_DATE_COL in df.columns:
        df[METRIC3_DATE_COL] = pd.to_datetime(df[METRIC3_DATE_COL], errors='coerce')
        df = df[df[METRIC3_DATE_COL] <= end_date]
    df_sorted = df.copy()
    df_sorted[METRIC3_EDITSTAMP_COL] = pd.to_datetime(df_sorted[METRIC3_EDITSTAMP_COL], errors='coerce')
    # Sort by Client_Id and Client_EditStamp ascending, so first is earliest
    df_sorted = df_sorted.sort_values([METRIC3_CLIENTID_COL, METRIC3_EDITSTAMP_COL], ascending=[True, True])
    earliest_status = df_sorted.drop_duplicates(subset=[METRIC3_CLIENTID_COL], keep='first')
    # Statuses to count as enrolled (contains match)
    mask = earliest_status[METRIC3_STATUS_COL].astype(str).apply(
        lambda x: any(status.lower() in x.lower() for status in ENROLLED_STATUSES)
    )
    enrolled_clients = earliest_status[mask][METRIC3_CLIENTID_COL].unique().tolist()
    return len(enrolled_clients), enrolled_clients


# Metric #4
# Number of Enrolled Clients from Priority Population (NCCCH add-on)
# may be easier to get the list of enrolled clients first from calculate_enrolled_clients, then use that list of client Id's and assign to each client a population category based upon a formula using the answers to the Cantrils Ladder questions.
# Categories and Formulas:
# Thriving: AhpscreeningOption_WellbeingCantrilsLadder1 >= 7 and AhpscreeningOption_WellbeingCantrilsLadder2 >= 8;
# Suffering: AhpscreeningOption_WellbeingCantrilsLadder1 <= 4 and AhpscreeningOption_WellbeingCantrilsLadder2 <= 4;
# Struggling: All other cases;
# Relevant incoming data:
# List of enrolled clients from calculate_enrolled_clients
# Relevant columns in excel:
# - Sheet: Ahpscreening
# - Client_Id
# - Ahpscreening_EditStamp
# - AhpscreeningOption_WellbeingCantrilsLadder1
# - AhpscreeningOption_WellbeingCantrilsLadder2
# Date start filter column: WE are using the list of enrolled clients, so we do not need to filter by date here.
# Date end filter column: We assume that the clients coming in are already filtered by the date
# For this metric we want to find the earliest(even outside the date range) Ahpscreening_EditStamp for each client and then use that to determine their population category.
def calculate_enrolled_clients_priority_population(df: pd.DataFrame, listOfEnrolledClients: list) -> int:
    """
    Number of Enrolled Clients from Priority Population (NCCCH add-on)
    For each enrolled client in listOfEnrolledClients, find their earliest Ahpscreening_EditStamp record where both Cantrils Ladder columns are non-blank.
    Use Cantrils Ladder scores to determine category (Thriving, Suffering, Struggling).
    Return the count of clients in Suffering or Struggling categories (priority population).
    """
    # Filter to only rows for enrolled clients
    if METRIC4_CLIENTID_COL not in df.columns or METRIC4_EDITSTAMP_COL not in df.columns or METRIC4_CL1_COL not in df.columns or METRIC4_CL2_COL not in df.columns:
        return 0
    df_clients = df[df[METRIC4_CLIENTID_COL].isin(listOfEnrolledClients)].copy()
    if df_clients.empty:
        return 0
    # Remove rows where either Cantrils Ladder column is blank or NaN
    df_clients = df_clients[df_clients[METRIC4_CL1_COL].notnull() &
                           (df_clients[METRIC4_CL1_COL].astype(str).str.strip() != '') &
                           df_clients[METRIC4_CL2_COL].notnull() &
                           (df_clients[METRIC4_CL2_COL].astype(str).str.strip() != '')]
    if df_clients.empty:
        return 0
    # Convert to datetime for sorting
    df_clients[METRIC4_EDITSTAMP_COL] = pd.to_datetime(df_clients[METRIC4_EDITSTAMP_COL], errors='coerce')
    # Get earliest screening for each client (with valid Cantrils Ladder data)
    df_clients = df_clients.sort_values([METRIC4_CLIENTID_COL, METRIC4_EDITSTAMP_COL])
    first_screenings = df_clients.drop_duplicates(subset=[METRIC4_CLIENTID_COL], keep='first')
    # Extract first digit from Cantrils Ladder columns (handles list-like strings)
    first_screenings['CL1_num'] = first_screenings[METRIC4_CL1_COL].apply(extract_first_digit)
    first_screenings['CL2_num'] = first_screenings[METRIC4_CL2_COL].apply(extract_first_digit)
    # Use helper to determine category
    def is_priority(row):
        cat = cantrils_ladder_category(row['CL1_num'], row['CL2_num'])
        return cat in ["Suffering", "Struggling"]
    priority_clients = first_screenings[first_screenings.apply(is_priority, axis=1)]
    return len(priority_clients)


# Metric #5
# Number of Enrolled Clients with an SDOH assessment (CCO-3)
# Incoming data:
# List of enrolled clients from calculate_enrolled_clients
# Relevant columns in excel:
# - Sheet: Ahpscreening
# - AhpscreeningSystem_DateAcceptedcompleted
# Date start filter column: We assume that the clients coming in are already filtered by the date range of the enrolled clients, so we do not need to filter by date here.
# Date end filter column: We assume that the clients coming in are already filtered by the date range of the enrolled clients, so we do not need to filter by date here.
# For this metric we want to see if the client has a valid date value in the AhpscreeningSystem_DateAcceptedcompleted column.
def calculate_enrolled_clients_with_sdoh_assessment(df: pd.DataFrame, listOfEnrolledClients: list) -> int:
    """
    Number of Enrolled Clients with an SDOH assessment (CCO-3)
    For each enrolled client in listOfEnrolledClients, check if they have a valid (non-null, non-empty, parseable) AhpscreeningSystem_DateAcceptedcompleted value.
    Returns the count of such clients.
    """
    if METRIC5_CLIENTID_COL not in df.columns or METRIC5_SDOH_DATE_COL not in df.columns:
        return 0
    # Filter to only rows for enrolled clients
    df_clients = df[df[METRIC5_CLIENTID_COL].isin(listOfEnrolledClients)].copy()
    if df_clients.empty:
        return 0
    # Convert to datetime, keep only valid dates
    df_clients[METRIC5_SDOH_DATE_COL] = pd.to_datetime(
        df_clients[METRIC5_SDOH_DATE_COL], errors='coerce')
    # Count unique clients with at least one valid assessment date
    valid = df_clients.dropna(subset=[METRIC5_SDOH_DATE_COL])
    unique_clients = valid[METRIC5_CLIENTID_COL].nunique()
    return unique_clients


# Metric #6:
# Number of Newly Enrolled Clients.
# Like metric #3, however we want to only count those with a cc_optin_date value after the start date but before the end date.
# - Sheet: Client
# - ClientOption_CareConnectStatus (program status)
# - Client_Id
# - Client_EditStamp
# - ClientSystem_CcOptinDate (date of enrollment)
# Date start filter column: ClientSystem_CcOptinDate
# Date end filter column: ClientSystem_CcOptinDate
# also returns a list of Client_Id's that were newly enrolled during the date range.
def calculate_new_enrolled_clients(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple:
    """
    Number of Newly Enrolled Clients (NCCCH add-on)
    Like metric #3, but only count those with a ClientSystem_CcOptinDate value after start_date and before end_date.
    Returns (count, list_of_newly_enrolled_client_ids)
    """
    if METRIC6_CLIENTID_COL not in df.columns or METRIC6_STATUS_COL not in df.columns or METRIC6_EDITSTAMP_COL not in df.columns or METRIC6_OPTIN_DATE_COL not in df.columns:
        return 0, []
    # filter by client Create Stamp within the date range
    if METRIC6_OPTIN_DATE_COL in df.columns:
        df[METRIC6_OPTIN_DATE_COL] = pd.to_datetime(df[METRIC6_OPTIN_DATE_COL], errors='coerce')
        df = df[(df[METRIC6_OPTIN_DATE_COL] >= start_date) & (df[METRIC6_OPTIN_DATE_COL] <= end_date)]
    # Convert dates
    df_sorted = df.copy()
    df_sorted[METRIC6_EDITSTAMP_COL] = pd.to_datetime(df_sorted[METRIC6_EDITSTAMP_COL], errors='coerce')
    # Sort by Client_Id and Client_EditStamp descending, so first is most recent
    df_sorted = df_sorted.sort_values([METRIC6_CLIENTID_COL, METRIC6_EDITSTAMP_COL], ascending=[True, False])
    # Drop duplicates to keep only the most recent status per client
    latest_status = df_sorted.drop_duplicates(subset=[METRIC6_CLIENTID_COL], keep='first')
    # Statuses to count as enrolled (contains match)
    mask = (
        latest_status[METRIC6_STATUS_COL].astype(str).apply(
            lambda x: any(status.lower() in x.lower() for status in METRIC6_STATUS_VALUES)
        ) &
        latest_status[METRIC6_OPTIN_DATE_COL].notnull() &
        (latest_status[METRIC6_OPTIN_DATE_COL] >= start_date) &
        (latest_status[METRIC6_OPTIN_DATE_COL] <= end_date)
    )
    new_enrolled_clients = latest_status[mask][METRIC6_CLIENTID_COL].unique().tolist()
    return len(new_enrolled_clients), new_enrolled_clients


# Metric #7:
# Number of outbound referrals from the CCH to Each HRSN Services category
# Relevant columns in excel:
# - Sheet: Interaction_referral
# - InteractionReferralTaxonomy_Taxonomy_external_term_name (The human Readable taxonomy name of the HRSN)
# - DATE start filter column: InteractionReferral_ReferralsModule_referral_status_requested_date
# - DATE end filter column: InteractionReferral_ReferralsModule_referral_status_requested_date
# ASSUMPTION: the InteractionReferral_ReferralsModule_referral_status_requested_date column is the date the referral was made. 
# We want to create a dictionary; for each unique name in the InteractionReferralTaxonomy_Taxonomy_external_term_name and populate its values, counting the number of rows for each, if the taxonomy ID is blank we want to still include it with a None id, and an "Uncategorized" name.

def calculate_outbound_referrals_type(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> dict:
    """
    Number of outbound referrals from the CCH to Each HRSN Services category (CLS-1)
    Returns a dictionary with category names as keys and counts as values.
    If the taxonomy name is blank, counts as 'Uncategorized'.
    """
    if METRIC7_CLIENTID_COL not in df.columns or METRIC7_TAXONOMY_COL not in df.columns or METRIC7_REFERRAL_DATE_COL not in df.columns:
        return {}
    # Filter by referral date within the date range
    df[METRIC7_REFERRAL_DATE_COL] = pd.to_datetime(df[METRIC7_REFERRAL_DATE_COL], errors='coerce')
    df = df[(df[METRIC7_REFERRAL_DATE_COL] >= start_date) & (df[METRIC7_REFERRAL_DATE_COL] <= end_date)]
    hrsn_referralsDict = {}
    for _, row in df.iterrows():
        # Only count if client id is present and not blank
        client_id = row[METRIC7_CLIENTID_COL]
        if pd.isnull(client_id) or str(client_id).strip() == '':
            continue
        taxonomy_name = row.get(METRIC7_TAXONOMY_COL, None)
        if pd.isnull(taxonomy_name) or str(taxonomy_name).strip() == '':
            category = 'Uncategorized'
        else:
            category = str(taxonomy_name).strip()
        if category not in hrsn_referralsDict:
            hrsn_referralsDict[category] = 1
        else:
            hrsn_referralsDict[category] += 1
    return hrsn_referralsDict



# Metric #8:
# Number of newly enrolled clients connected to CBCC services within 7 days of referral.
# Relevant columns in excel:
# - Sheet: interaction
# - InteractionOption_ContactOutcome
# - Interaction_CreateStamp (date of interaction)
# - Sheet: Client
# - ClientOption_CareConnectStatus (program status)
# - Client_Id
# - Client_EditStamp
# - ClientSystem_CcOptinDate (date of enrollment)
# - ClientSystem_CcProgramReferralDate (date of referral)
# - Sheet: Ahpscreening
# - AhpscreeningOption_WellbeingCantrilsLadder1
# if the column InteractionOption_ContactOutcome contains the text "Services Provided" and the date of interaction is within seven days of the date of referral we count that row as a successful connection.
# For simplicity we take in the list of newly enrolled clients, newly_enrolled_clients.
# date start filter column: ClientSystem_CcOptinDate
# date end filter column: ClientSystem_CcOptinDate
# # updated the logic is as follows:
# - New Logic for detrmining if a client is connected to CBCC services: 
# - We create a set of clientsServed, add their id to the set if the value in InteractionOption_ContactOutcome is one of the values in the constant SERVICES_PROVIDED and the client ID exists in newly_enrolled_client_ids, AND the value in Interaction_CreateStamp for that record is within the 7 days since enrollment.
# - Once completed running through the rows we then check the values in the column AhpscreeningSystem_DateAcceptedcompleted, any rows with a value that are within the 7 day period AND where the client id is in the newly_enrolled_clients list we count as a successful connection.
def calculate_newly_enrolled_clients_connected_to_cbcc_7_days(client_df: pd.DataFrame, interaction_df: pd.DataFrame, ahpscreening_df: pd.DataFrame, newly_enrolled_client_ids: list) -> int:
    """
    Number of newly enrolled clients connected to CBCC services within 7 days of referral.
    For each client in newly_enrolled_client_ids, count as connected if:
    - There is an interaction in interaction_df where METRIC8_OUTCOME_COL is in SERVICES_PROVIDED, the client ID is in newly_enrolled_client_ids, and the METRIC8_INTERACTION_DATE_COL is within 7 days of METRIC8_REFERRAL_DATE_COL.
    OR
    - There is a row in ahpscreening_df where METRIC8_SDOH_DATE_COL is within 7 days of METRIC8_REFERRAL_DATE_COL and client ID is in newly_enrolled_client_ids.
    Returns the number of unique clients meeting these criteria.
    """
    if not newly_enrolled_client_ids:
        print('[DEBUG] Metric #8: No newly enrolled client IDs provided.')
        return 0
    referral_dates = client_df.set_index(METRIC8_CLIENTID_COL)[METRIC8_REFERRAL_DATE_COL].to_dict()
    clients_served = set()
    print(f"[DEBUG] Metric #8: Number of Newly enrolled clients: {len(newly_enrolled_client_ids)}")
    # --- Interactions logic ---
    if (
        METRIC8_CLIENTID_COL in interaction_df.columns and
        METRIC8_OUTCOME_COL in interaction_df.columns and
        METRIC8_INTERACTION_DATE_COL in interaction_df.columns
    ):
        interaction_df[METRIC8_INTERACTION_DATE_COL] = pd.to_datetime(interaction_df[METRIC8_INTERACTION_DATE_COL], errors='coerce')
        for _, row in interaction_df.iterrows():
            client_id = row[METRIC8_CLIENTID_COL]
            if client_id in newly_enrolled_client_ids and client_id in referral_dates:
                referral_date = pd.to_datetime(referral_dates[client_id], errors='coerce')
                if pd.notnull(referral_date):
                    interaction_date = row[METRIC8_INTERACTION_DATE_COL]
                    if pd.notnull(interaction_date) and (interaction_date - referral_date).days <= 7:
                        outcome = row[METRIC8_OUTCOME_COL]
                        if outcome in SERVICES_PROVIDED:
                            clients_served.add(client_id)
    # --- Ahpscreening logic ---
    if (
        METRIC8_CLIENTID_COL in ahpscreening_df.columns and
        METRIC8_SDOH_DATE_COL in ahpscreening_df.columns
    ):
        ahpscreening_df[METRIC8_SDOH_DATE_COL] = pd.to_datetime(ahpscreening_df[METRIC8_SDOH_DATE_COL], errors='coerce')
        for _, row in ahpscreening_df.iterrows():
            client_id = row[METRIC8_CLIENTID_COL]
            if client_id in newly_enrolled_client_ids and client_id in referral_dates:
                referral_date = pd.to_datetime(referral_dates[client_id], errors='coerce')
                if pd.notnull(referral_date):
                    sdoh_date = row[METRIC8_SDOH_DATE_COL]
                    if pd.notnull(sdoh_date) and (sdoh_date - referral_date).days <= 7:
                        clients_served.add(client_id)
    print(f"[DEBUG] Metric #8: Total unique clients connected in 7 days: {len(clients_served)}")
    return len(clients_served)


# Metric #9:
# Number of newly enrolled clients connected to CBCC services within 30 days of referral.
# Same as metric #8 but with a 30 day window instead of 7 days.
def calculate_newly_enrolled_clients_connected_to_cbcc_30_days(client_df: pd.DataFrame, interaction_df: pd.DataFrame, ahpscreening_df: pd.DataFrame, newly_enrolled_client_ids: list) -> int:
    """
    Number of newly enrolled clients connected to CBCC services within 30 days of referral.
    For each client in newly_enrolled_client_ids, count as connected if:
    - There is an interaction in interaction_df where METRIC8_OUTCOME_COL is in SERVICES_PROVIDED, the client ID is in newly_enrolled_client_ids, and the METRIC8_INTERACTION_DATE_COL is within 30 days of METRIC8_REFERRAL_DATE_COL.
    OR
    - There is a row in ahpscreening_df where METRIC8_SDOH_DATE_COL is within 30 days of METRIC8_REFERRAL_DATE_COL and client ID is in newly_enrolled_client_ids.
    Returns the number of unique clients meeting these criteria.
    """
    if not newly_enrolled_client_ids:
        print('[DEBUG] Metric #9: No newly enrolled client IDs provided.')
        return 0
    referral_dates = client_df.set_index(METRIC8_CLIENTID_COL)[METRIC8_REFERRAL_DATE_COL].to_dict()
    clients_served = set()
    print(f"[DEBUG] Metric #9: Number of Newly enrolled clients: {len(newly_enrolled_client_ids)}")
    # --- Interactions logic ---
    if (
        METRIC8_CLIENTID_COL in interaction_df.columns and
        METRIC8_OUTCOME_COL in interaction_df.columns and
        METRIC8_INTERACTION_DATE_COL in interaction_df.columns
    ):
        for _, row in interaction_df.iterrows():
            client_id = row[METRIC8_CLIENTID_COL]
            if client_id in newly_enrolled_client_ids and client_id in referral_dates:
                referral_date = pd.to_datetime(referral_dates[client_id], errors='coerce')
                if pd.notnull(referral_date):
                    interaction_date = pd.to_datetime(row[METRIC8_INTERACTION_DATE_COL], errors='coerce')
                    if pd.notnull(interaction_date) and (interaction_date - referral_date).days <= 30:
                        outcome = row[METRIC8_OUTCOME_COL]
                        if outcome in SERVICES_PROVIDED:
                            clients_served.add(client_id)
    # --- Ahpscreening logic ---
    if (
        METRIC8_CLIENTID_COL in ahpscreening_df.columns and
        METRIC8_SDOH_DATE_COL in ahpscreening_df.columns
    ):
        for _, row in ahpscreening_df.iterrows():
            client_id = row[METRIC8_CLIENTID_COL]
            if client_id in newly_enrolled_client_ids and client_id in referral_dates:
                referral_date = pd.to_datetime(referral_dates[client_id], errors='coerce')
                if pd.notnull(referral_date):
                    sdoh_date = pd.to_datetime(row[METRIC8_SDOH_DATE_COL], errors='coerce')
                    if pd.notnull(sdoh_date) and (sdoh_date - referral_date).days <= 30:
                        clients_served.add(client_id)
    print(f"[DEBUG] Metric #9: Total unique clients connected in 30 days: {len(clients_served)}")
    return len(clients_served)


# Metric #10:
# Percent of individuals referred to the CCH who are enrolled in the CCH.
# Numretor: Metric#6
# Denominator: Metric#2
def calculate_enrollment_percentage(num_enrolled: int, num_referred: int) -> float:
    if num_referred == 0:
        return 0.0
    return (num_enrolled / num_referred) * 100

# Metric #11:
# Percent of enrolled clients from priority populations.
# Numerator: Metric#4
# Denominator: Metric#3
def calculate_priority_population_percentage(num_priority: int, num_enrolled: int) -> float:
    if num_enrolled == 0:
        return 0.0
    return (num_priority / num_enrolled) * 100

# Metric #12:
# numerator: Metric#5
# denominator: Metric#3
# Percent of enrolled clients with an SDOH assessment.
def calculate_sdoh_assessment_percentage(num_with_assessment: int, num_enrolled: int) -> float:
    if num_enrolled == 0:
        return 0.0
    return (num_with_assessment / num_enrolled) * 100

# metric #13:
# Percent of newly enrolled clients connected to CBCC services through the CCH within 7 days of referral.
# numerator: Metric#8
# denominator: Metric#6
def calculate_percent_newly_enrolled_clients_connected_to_cbcc_7_days(num_connected: int, num_enrolled: int) -> float:
    if num_enrolled == 0:
        return 0.0
    return (num_connected / num_enrolled) * 100

# metric #14:
# Percent of newly enrolled clients connected to CBCC services through the CCH within 30 days of referral.
# numerator: Metric#9
# denominator: Metric#6
def calculate_percent_newly_enrolled_clients_connected_to_cbcc_30_days(num_connected: int, num_enrolled: int) -> float:
    if num_enrolled == 0:
        return 0.0
    return (num_connected / num_enrolled) * 100

# Metric #15:
# Percent of identified client needs that were successfully met.
# Relevant columns in excel:
# - Sheet: Goalshortterm
# - Goalshortterm_Status
# - GoalshorttermOption_GoalClosureStatus
# - GoalshorttermSystem_StgDateCreated
# - GoalshorttermSystem_StgDateCompleted
# Start date filter columns: GoalshorttermSystem_StgDateCreated, GoalshorttermSystem_StgDateCompleted
# End date filter columns: GoalshorttermSystem_StgDateCompleted, GoalshorttermSystem_StgDateCreated
# We want to track any that have the goal closure status of "Met" or "Partially Met" and then divide that by the total number of goals created during the date range.
def calculate_identified_client_needs_met(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> float:
    print("[DEBUG] Starting calculate_identified_client_needs_met")
    if 'Goalshortterm_Status' not in df.columns or 'GoalshorttermOption_GoalClosureStatus' not in df.columns or 'GoalshorttermSystem_StgDateCreated' not in df.columns or 'GoalshorttermSystem_StgDateCompleted' not in df.columns:
        print("[DEBUG] Required columns missing!")
        return 0.0
    # Filter by date range
    df['GoalshorttermSystem_StgDateCreated'] = pd.to_datetime(df['GoalshorttermSystem_StgDateCreated'], errors='coerce')
    df['GoalshorttermSystem_StgDateCompleted'] = pd.to_datetime(df['GoalshorttermSystem_StgDateCompleted'], errors='coerce')
    # filtered_df = df[(df['GoalshorttermSystem_StgDateCreated'] >= start_date) & (df['GoalshorttermSystem_StgDateCompleted'] <= end_date)]
    filtered_df = df[(df['GoalshorttermSystem_StgDateCreated'] >= start_date) & (df['GoalshorttermSystem_StgDateCreated'] <= end_date)]           
    if filtered_df.empty:
        return 0.0
    # Use contains check for 'Met' or 'Partially Met'
    met_mask = filtered_df['GoalshorttermOption_GoalClosureStatus'].astype(str).str.contains('Met', case=False, na=False) | \
               filtered_df['GoalshorttermOption_GoalClosureStatus'].astype(str).str.contains('Partially Met', case=False, na=False)
    met_goals = filtered_df[met_mask]
    if len(filtered_df) == 0:
        return 0.0
    percent_met = (len(met_goals) / len(filtered_df)) * 100
    return percent_met


# Helper function
# Get clients discharged during the date range
# relevant columns in excel:
# - Sheet: Interaction
# - Client_Id
# - InteractionOption_ContactOutcome
# - Interaction_CreateStamp
# # Date start filter column: Interaction_CreateStamp
# # Date end filter column: Interaction_CreateStamp
# We want to find all clients where the contact outcome contains the text discharged and the interaction create stamp is within the date range. We will return a list of Client_Id's that were discharged during the date range.
def get_discharged_clients(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> list:
    """
    Returns a list of Client_Id's that were discharged during the date range.
    A client is considered discharged if InteractionOption_ContactOutcome contains 'discharged' (case-insensitive)
    and Interaction_CreateStamp is within the date range.
    """
    if 'Client_Id' not in df.columns or \
       'InteractionOption_ContactOutcome' not in df.columns or \
       'Interaction_CreateStamp' not in df.columns:
        return []
    df_filtered = df[df['InteractionOption_ContactOutcome'].astype(str).str.contains('discharged', case=False, na=False)].copy()
    df_filtered['Interaction_CreateStamp'] = pd.to_datetime(df_filtered['Interaction_CreateStamp'], errors='coerce')
    in_range = df_filtered[(df_filtered['Interaction_CreateStamp'] >= start_date) & (df_filtered['Interaction_CreateStamp'] <= end_date)]
    return in_range['Client_Id'].dropna().unique().tolist()
    

# Metric #16:
# Percent of Discharged Clients Reporting Improved Wellbeing
# Relevant columns in excel:
# - Sheet: Ahpscreening
# - Client_Id
# - Ahpscreening_CreateStamp
# - AhpscreeningOption_WellbeingCantrilsLadder1
# - AhpscreeningOption_WellbeingCantrilsLadder2
# We will take in a list of discharged clients, We will determine if they have at least two cantrils ladder scores, if they do we use them in our logic, if not we skip that client and do not include them in the total count.
# We will use the earliest Ahpscreening_CreateStamp for each client to determine their wellbeing category at intake, and the latest one to determine their wellbeing category at discharge.
# if the client wellbeing category improved from intake to discharge, we count that as a success, and we will return the percentage of clients who improved.
def calculate_discharged_clients_wellbeing_improvement(df: pd.DataFrame, discharged_clients: list) -> float:
    """
    Percent of Discharged Clients Reporting Improved Wellbeing (NCCCH add-on)
    For each discharged client, use the earliest Ahpscreening_CreateStamp for intake and latest for discharge.
    Use cantrils_ladder_category to determine category at intake and discharge.
    If category improves (Suffering/Struggling -> Thriving/Struggling -> Thriving), count as improved.
    Returns the percent of discharged clients with improved wellbeing.
    """
    if not discharged_clients:
        print("[DEBUG] No discharged clients provided.")
        return 0.0
    required_cols = [
        'Client_Id',
        'Ahpscreening_CreateStamp',
        'AhpscreeningOption_WellbeingCantrilsLadder1',
        'AhpscreeningOption_WellbeingCantrilsLadder2'
    ]
    for col in required_cols:
        if col not in df.columns:
            print(f"[DEBUG] Required column missing: {col}")
            return 0.0
    df_clients = df[df['Client_Id'].isin(discharged_clients)].copy()
    if df_clients.empty:
        print("[DEBUG] No matching discharged clients in Ahpscreening sheet.")
        return 0.0
    # Remove rows where either Cantrils Ladder column is blank or NaN
    df_clients = df_clients[df_clients['AhpscreeningOption_WellbeingCantrilsLadder1'].notnull() &
                           (df_clients['AhpscreeningOption_WellbeingCantrilsLadder1'].astype(str).str.strip() != '') &
                           df_clients['AhpscreeningOption_WellbeingCantrilsLadder2'].notnull() &
                           (df_clients['AhpscreeningOption_WellbeingCantrilsLadder2'].astype(str).str.strip() != '')]
    if df_clients.empty:
        print("[DEBUG] No valid screenings with both Cantrils Ladder columns present.")
        return 0.0
    df_clients['Ahpscreening_CreateStamp'] = pd.to_datetime(df_clients['Ahpscreening_CreateStamp'], errors='coerce')
    df_clients['CL1_num'] = df_clients['AhpscreeningOption_WellbeingCantrilsLadder1'].apply(extract_first_digit)
    df_clients['CL2_num'] = df_clients['AhpscreeningOption_WellbeingCantrilsLadder2'].apply(extract_first_digit)

    improved_count = 0
    total_count = 0
    print(f"[DEBUG] Total discharged clients to process: {len(df_clients['Client_Id'].unique())}")
    if len(df_clients['Client_Id'].unique()) == 0:
        print("[DEBUG] No discharged clients to process.")
        return 0.0
    for client_id, group in df_clients.groupby('Client_Id'):
        group = group.sort_values('Ahpscreening_CreateStamp')
        intake = group.iloc[0]
        discharge = group.iloc[-1]
        intake_cat = cantrils_ladder_category(intake['CL1_num'], intake['CL2_num'])
        discharge_cat = cantrils_ladder_category(discharge['CL1_num'], discharge['CL2_num'])
        # Only count if both categories are known
        if intake_cat == "Unknown" or discharge_cat == "Unknown":
            continue
        # Define improvement: Suffering < Struggling < Thriving
        cat_order = {"Suffering": 0, "Struggling": 1, "Thriving": 2}
        if cat_order.get(discharge_cat, -1) > cat_order.get(intake_cat, -1):
            improved_count += 1
        total_count += 1
    print(f"[DEBUG] Improved count: {improved_count}, Total count: {total_count}")
    if total_count == 0:
        return 0.0
    return (improved_count / total_count) * 100


def extract_first_digit(value) -> int | None:
    """
    Extracts the first integer found in a given value.

    This function takes any input, converts it to a string, and uses a regular
    expression to find the first sequence of digits. It is designed to handle
    various data formats, including strings (e.g., "7 - Doing Well"), list-like
    strings (e.g., '["7"]'), and missing values (NaN).

    Args:
        value: The input value from which to extract a digit.

    Returns:
        An integer if a digit is found, otherwise None.
    """
    if pd.isnull(value):
        return None
    # Search for the first sequence of one or more digits in the string representation of the value
    match = re.search(r'\d+', str(value))
    return int(match.group(0)) if match else None

def cantrils_ladder_category(q1: int | None, q2: int | None) -> str:
    """
    Determines a client's well-being category based on two Cantril's Ladder scores.

    The categories are defined as:
    - Thriving: Score 1 is >= 7 AND Score 2 is >= 8.
    - Suffering: Score 1 is <= 4 AND Score 2 is <= 4.
    - Struggling: All other cases.
    - Unknown: If either score is not available.

    Args:
        q1: The numerical score for the first Cantril's Ladder question.
        q2: The numerical score for the second Cantril's Ladder question.

    Returns:
        A string representing the category: "Thriving", "Suffering", "Struggling", or "Unknown".
    """
    if q1 is None or q2 is None or isinstance(q1, str) or isinstance(q2, str):
        return "Unknown"
    if q1 >= 7 and q2 >= 8:
        return "Thriving"
    elif q1 <= 4 and q2 <= 4:
        return "Suffering"
    else:
        return "Struggling"






def calculate_all_metrics(dfDict: dict, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """
    Calculate all required AHP metrics from the input DataFrame.
    Returns a DataFrame with one row per metric and columns: Metric, Value, Description.
    """
    metrics = []
    inbound_referrals = calculate_inbound_referrals(dfDict[METRIC1_SHEET], start_date, end_date)
    metrics.append({
        'Metric': 'Number of Inbound Referrals into the CCH',
        'Value': inbound_referrals,
        'Description': 'Unique inbound referrals into the CCH.'
    })
    metrics.append({
        'Metric': 'Number of unique Individuals Referred into the CCH',
        'Value': calculate_unique_individuals_referred(dfDict['Client'], start_date, end_date),
        'Description': 'Unique individuals referred into the CCH.'
    })
    number_enrolled, enrolled_clients = calculate_enrolled_clients(dfDict['Client'], start_date, end_date)
    metrics.append({
        'Metric': 'Number of Enrolled Clients',
        'Value': number_enrolled,
        'Description': 'Unique clients enrolled in the CCH.'
    })
    num_priority_population = calculate_enrolled_clients_priority_population(dfDict['Ahpscreening'], enrolled_clients)
    metrics.append({
        'Metric': 'Number of Enrolled Clients from Priority Population',
        'Value': num_priority_population,
        'Description': 'Enrolled clients from priority populations based on Cantrils Ladder scores.'
    })
    num_with_sdoh_assessment = calculate_enrolled_clients_with_sdoh_assessment(dfDict['Ahpscreening'], enrolled_clients)
    metrics.append({
        'Metric': 'Number of Enrolled Clients with an SDOH assessment',
        'Value': num_with_sdoh_assessment,
        'Description': 'Enrolled clients who have completed an SDOH assessment.'
    })
    num_newly_enrolled, newly_enrolled_clients = calculate_new_enrolled_clients(dfDict['Client'], start_date, end_date)
    metrics.append({
        'Metric': 'Number of Newly Enrolled Clients',
        'Value': num_newly_enrolled,
        'Description': 'Unique clients newly enrolled in the CCH during the reporting period.'
    })
    # Metric #7: Outbound referrals by HRSN category
    outbound_referrals_by_type = calculate_outbound_referrals_type(dfDict['Interaction_referral'], start_date, end_date)
    for category, count in outbound_referrals_by_type.items():
        metrics.append({
            'Metric': f'Number of Outbound Referrals to HRSN Services: {category}',
            'Value': count,
            'Description': f'Total outbound referrals made from the CCH to HRSN services in category: {category}.'
        })
    num_connected_in_7_days = calculate_newly_enrolled_clients_connected_to_cbcc_7_days(dfDict['Client'], dfDict['Interaction'], dfDict['Ahpscreening'], newly_enrolled_clients)
    metrics.append({
        'Metric': 'Number of newly enrolled clients connected to CBCC services within 7 days of referral',
        'Value': num_connected_in_7_days,
        'Description': 'Clients who were newly enrolled in the CCH and connected to CBCC services within 7 days of referral.'
    })
    num_connected_in_30_days = calculate_newly_enrolled_clients_connected_to_cbcc_30_days(dfDict['Client'], dfDict['Interaction'], dfDict['Ahpscreening'], newly_enrolled_clients)
    metrics.append({
        'Metric': 'Number of newly enrolled clients connected to CBCC services within 30 days of referral',
        'Value': num_connected_in_30_days,
        'Description': 'Clients who were newly enrolled in the CCH and connected to CBCC services within 30 days of referral.'
    })
    metrics.append({
        'Metric': 'Percent of individuals referred to the CCH who are enrolled in the CCH.',
        'Value': calculate_enrollment_percentage(number_enrolled, inbound_referrals),
        'Description': 'Percentage of individuals referred to the CCH who are enrolled in the CCH.'
    })
    metrics.append({
        'Metric': 'Percent of enrolled clients from priority populations.',
        'Value': calculate_priority_population_percentage(num_priority_population, number_enrolled),
        'Description': 'Percentage of enrolled clients who are from priority populations.'
    })
    metrics.append({
        'Metric': 'Percent of enrolled clients with an SDOH assessment.',
        'Value': calculate_sdoh_assessment_percentage(num_with_sdoh_assessment, number_enrolled),
        'Description': 'Enrolled clients who have completed an SDOH assessment.'
    })
    metrics.append({
        'Metric': 'Percent of newly enrolled clients connected to CBCC services within 7 days of referral.',
        'Value': calculate_percent_newly_enrolled_clients_connected_to_cbcc_7_days(num_connected_in_7_days, num_newly_enrolled),
        'Description': 'Percentage of newly enrolled clients connected to CBCC services within 7 days of referral.'
    })
    metrics.append({
        'Metric': 'Percent of newly enrolled clients connected to CBCC services within 30 days of referral.',
        'Value': calculate_percent_newly_enrolled_clients_connected_to_cbcc_30_days(num_connected_in_30_days, num_newly_enrolled),
        'Description': 'Percentage of newly enrolled clients connected to CBCC services within 30 days of referral.'
    })
    identified_needs_met = calculate_identified_client_needs_met(dfDict['Goalshortterm'], start_date, end_date)
    metrics.append({
        'Metric': 'Percent of identified client needs that were successfully met.',
        'Value': identified_needs_met,
        'Description': 'Percentage of identified client needs that were successfully met during the reporting period.'
    })
    discharged_clients = get_discharged_clients(dfDict['Interaction'], start_date, end_date)
    metrics.append({
        'Metric': 'Percent of Discharged Clients Reporting Improved Wellbeing',
        'Value': calculate_discharged_clients_wellbeing_improvement(dfDict['Ahpscreening'], discharged_clients),
        'Description': 'Percentage of discharged clients who reported improved wellbeing based on Cantrils Ladder scores.'
    })
    return pd.DataFrame(metrics)