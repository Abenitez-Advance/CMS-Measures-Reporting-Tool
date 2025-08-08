import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import os
from datetime import datetime

# Placeholder for metric calculation logic
from metrics import calculate_all_metrics, RELEVANT_SHEETS

def select_input_file():
    file_path = filedialog.askopenfilename(
        title="Select Excel File",
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    input_entry.delete(0, tk.END)
    input_entry.insert(0, file_path)

def select_output_file():
    file_path = filedialog.asksaveasfilename(
        title="Save CSV As",
        defaultextension=".csv",
        filetypes=[("CSV Files", "*.csv")]
    )
    output_entry.delete(0, tk.END)
    output_entry.insert(0, file_path)

def run_report():
    input_path = input_entry.get()
    output_path = output_entry.get()
    start_date_str = start_date_entry.get()
    end_date_str = end_date_entry.get()
    if not os.path.isfile(input_path):
        messagebox.showerror("Error", "Input file does not exist.")
        return
    try:
        # Read all relevant sheets into a dictionary of DataFrames, skip metadata row 2 so first row is header and data starts at row 3
        sheet_names = RELEVANT_SHEETS
        all_sheets = pd.read_excel(input_path, engine="openpyxl", header=0, skiprows=[1], sheet_name=None)
        data = {name: all_sheets[name] for name in sheet_names if name in all_sheets}
        # Parse date range if provided, but do not filter here
        # Always provide valid pd.Timestamp for start/end date (use wide range if not provided)
        if start_date_str and end_date_str:
            try:
                pd_start_date = pd.to_datetime(datetime.strptime(start_date_str, "%Y-%m-%d"))
                pd_end_date = pd.to_datetime(datetime.strptime(end_date_str, "%Y-%m-%d"))
            except Exception as e:
                messagebox.showerror("Error", f"Invalid date format: {e}")
                return
        else:
            pd_start_date = pd.Timestamp.min
            pd_end_date = pd.Timestamp.max
        metrics_df = calculate_all_metrics(data, pd_start_date, pd_end_date)
        metrics_df.to_csv(output_path, index=False)
        messagebox.showinfo("Success", f"Report saved to {output_path}")
    except Exception as e:
        messagebox.showerror("Error", str(e))

root = tk.Tk()
root.title("AHP Metrics Reporting Tool")

frame = tk.Frame(root, padx=10, pady=10)
frame.pack()

# Input file selection
input_label = tk.Label(frame, text="Input Excel File:")
input_label.grid(row=0, column=0, sticky="e")
input_entry = tk.Entry(frame, width=40)
input_entry.grid(row=0, column=1)
input_button = tk.Button(frame, text="Browse", command=select_input_file)
input_button.grid(row=0, column=2)

# Output file selection
output_label = tk.Label(frame, text="Output CSV File:")
output_label.grid(row=1, column=0, sticky="e")
output_entry = tk.Entry(frame, width=40)
output_entry.grid(row=1, column=1)
output_button = tk.Button(frame, text="Browse", command=select_output_file)
output_button.grid(row=1, column=2)

# Date range selection
start_date_label = tk.Label(frame, text="Start Date (YYYY-MM-DD):")
start_date_label.grid(row=2, column=0, sticky="e")
start_date_entry = tk.Entry(frame, width=20)
start_date_entry.grid(row=2, column=1, sticky="w")

end_date_label = tk.Label(frame, text="End Date (YYYY-MM-DD):")
end_date_label.grid(row=3, column=0, sticky="e")
end_date_entry = tk.Entry(frame, width=20)
end_date_entry.grid(row=3, column=1, sticky="w")

# Run button
run_button = tk.Button(frame, text="Generate Report", command=run_report, width=20)
run_button.grid(row=4, column=0, columnspan=3, pady=10)

# Prefill fields for testing
input_entry.insert(0, "C:/Users/Admin/Documents/AHPReporting/test_export7_10_25.xlsx")
output_entry.insert(0, "C:/Users/Admin/Documents/AHPReporting/Output3.csv")
start_date_entry.insert(0, "2025-01-01")
end_date_entry.insert(0, "2026-01-01")

root.mainloop()
