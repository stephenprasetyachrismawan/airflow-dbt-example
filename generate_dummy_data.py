"""
Healthcare Data Warehouse - Simplified Dummy Data Generator
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os
import warnings

warnings.filterwarnings('ignore')

fake = Faker('id_ID')
Faker.seed(42)
np.random.seed(42)

OUTPUT_DIR = 'data/raw'
os.makedirs(OUTPUT_DIR, exist_ok=True)

MASTER_ROWS = 20
TRANSACTION_ROWS = 100

print("=" * 80)
print("HEALTHCARE DATA WAREHOUSE - DUMMY DATA GENERATOR")
print("=" * 80)

# ============================================================================
# 1. STG_EHP__PATN (Patient Master)
# ============================================================================
print("\n[1/16] Generating STG_EHP__PATN (Patients)...")
patients = []
for i in range(MASTER_ROWS):
    patients.append({
        'PAT_ID': f"PAT{str(i+1).zfill(5)}",
        'F_NAME': fake.first_name(),
        'L_NAME': fake.last_name(),
        'M_NAME': fake.first_name(),
        'GEN_CD': np.random.choice(['M', 'F']),
        'GEN_DES': np.random.choice(['Male', 'Female']),
        'DOB': fake.date_of_birth(minimum_age=5, maximum_age=90),
        'CON_NO': fake.phone_number()[:15],
        'EM_ADD': fake.email(),
        'EC_F_NAME': fake.first_name(),
        'EC_L_NAME': fake.last_name(),
        'EC_CON_NO': fake.phone_number()[:15],
        'B_TYPE': np.random.choice(['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']),
        'MAR_ST': np.random.choice(['Single', 'Married', 'Divorced', 'Widowed']),
        'NAT_ID_N': fake.numerify(text='################'),
        'NAT_ID_E': fake.date_between(start_date='-20y', end_date='+5y')
    })

df_patients = pd.DataFrame(patients)
df_patients.to_csv(f'{OUTPUT_DIR}/STG_EHP__PATN.csv', index=False)
print(f"   OK - {len(df_patients)} rows")

# ============================================================================
# 2. STG_EHP__DPMT (Department Master)
# ============================================================================
print("[2/16] Generating STG_EHP__DPMT (Departments)...")
dept_names = ['Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Obstetrics',
              'Oncology', 'Emergency', 'ICU', 'Radiology', 'Pathology',
              'Surgery', 'Internal Medicine', 'Psychiatry', 'Dermatology', 'ENT',
              'Ophthalmology', 'Urology', 'Gastroenterology', 'Pulmonology', 'Rheumatology']

departments = []
for i in range(MASTER_ROWS):
    departments.append({
        'DEP_ID': f"DEP{str(i+1).zfill(4)}",
        'DEP_NAME': dept_names[i % len(dept_names)],
        'DEP_DES': f"{dept_names[i % len(dept_names)]} Department",
        'DEP_LOC': f"Block {chr(65 + (i % 5))}, Floor {(i % 3) + 1}"
    })

df_departments = pd.DataFrame(departments)
df_departments.to_csv(f'{OUTPUT_DIR}/STG_EHP__DPMT.csv', index=False)
print(f"   OK - {len(df_departments)} rows")

# ============================================================================
# 3. STG_EHP__STFF (Staff Master)
# ============================================================================
print("[3/16] Generating STG_EHP__STFF (Staff)...")
roles = [('DOC', 'Doctor'), ('NRS', 'Nurse'), ('PHM', 'Pharmacist'), ('LAB', 'Technician'), ('ADM', 'Administrative')]
statuses = [('ACT', 'Active'), ('INA', 'Inactive')]

staff = []
for i in range(MASTER_ROWS * 2):
    role_cd, role_des = roles[i % len(roles)]
    stat_cd, stat_des = statuses[i % len(statuses)]
    staff.append({
        'STF_ID': f"STF{str(i+1).zfill(5)}",
        'F_NAME': fake.first_name(),
        'L_NAME': fake.last_name(),
        'GEN_CD': np.random.choice(['M', 'F']),
        'GEN_DES': np.random.choice(['Male', 'Female']),
        'DT_BRT': fake.date_of_birth(minimum_age=25, maximum_age=65),
        'DEP_ID': f"DEP{str((i % MASTER_ROWS) + 1).zfill(4)}",
        'ROLE_CD': role_cd,
        'ROLE_DES': role_des,
        'CON_NO': fake.phone_number()[:15],
        'EM_ADD': fake.email(),
        'STAT_CD': stat_cd,
        'STAT_DES': stat_des,
        'HIRE_DATE': fake.date_between(start_date='-15y'),
        'ROLE_DATE': fake.date_between(start_date='-10y'),
        'SAL_YR': np.random.randint(30000000, 150000000),
        'SAL_MN': np.random.randint(2500000, 12500000)
    })

df_staff = pd.DataFrame(staff)
df_staff.to_csv(f'{OUTPUT_DIR}/STG_EHP__STFF.csv', index=False)
print(f"   OK - {len(df_staff)} rows")

# ============================================================================
# 4. STG_EHP__ROMS (Room Master)
# ============================================================================
print("[4/16] Generating STG_EHP__ROMS (Rooms)...")
room_purposes = [('IPT', 'Inpatient'), ('OPT', 'Outpatient'), ('OPS', 'Surgery')]
room_types = [('STD', 'Standard'), ('PRM', 'Premium'), ('ICU', 'ICU')]

rooms = []
for i in range(MASTER_ROWS * 3):
    purp_cd, purp_des = room_purposes[i % len(room_purposes)]
    rtype_cd, rtype_des = room_types[i % len(room_types)]
    rooms.append({
        'ROM_ID': f"ROM{str(i+1).zfill(5)}",
        'DEP_ID': f"DEP{str((i % MASTER_ROWS) + 1).zfill(4)}",
        'PRPS_CD': purp_cd,
        'PRPS_DES': purp_des,
        'RTYPE_CD': rtype_cd,
        'RTYPE_DES': rtype_des,
        'CPCT_NO': np.random.randint(1, 4)
    })

df_rooms = pd.DataFrame(rooms)
df_rooms.to_csv(f'{OUTPUT_DIR}/STG_EHP__ROMS.csv', index=False)
print(f"   OK - {len(df_rooms)} rows")

# ============================================================================
# 5. STG_EHP__MDCN (Medicine Master)
# ============================================================================
print("[5/16] Generating STG_EHP__MDCN (Medicines)...")
med_types = [('ANT', 'Antibiotic'), ('PAI', 'Painkiller'), ('VIT', 'Vitamin'),
             ('HYP', 'Hypertension'), ('DIA', 'Diabetes'), ('AST', 'Asthma')]
med_forms = [('TAB', 'Tablet'), ('CAP', 'Capsule'), ('INJ', 'Injection'), ('LIQ', 'Liquid')]
med_status = [('AVL', 'Available'), ('OOS', 'Out of Stock')]

medicines = []
for i in range(MASTER_ROWS * 2):
    type_cd, type_des = med_types[i % len(med_types)]
    form_cd, form_des = med_forms[i % len(med_forms)]
    mstat_cd, mstat_des = med_status[i % len(med_status)]
    medicines.append({
        'MED_ID': f"MED{str(i+1).zfill(5)}",
        'MED_NAME': f"{fake.word().capitalize()} {fake.word().capitalize()}",
        'TYPE_CD': type_cd,
        'TYPE_DES': type_des,
        'FORM_CD': form_cd,
        'FORM_DES': form_des,
        'STRN_NO': fake.numerify(text='STR-######'),
        'STRN_UN': np.random.choice(['mg', 'ml', 'IU', 'mcg']),
        'COST_UN': np.random.randint(1000, 100000),
        'MSTAT_CD': mstat_cd,
        'MSTAT_DES': mstat_des
    })

df_medicines = pd.DataFrame(medicines)
df_medicines.to_csv(f'{OUTPUT_DIR}/STG_EHP__MDCN.csv', index=False)
print(f"   OK - {len(df_medicines)} rows")

# ============================================================================
# 6. STG_EHP__ALGY (Allergy Master)
# ============================================================================
print("[6/16] Generating STG_EHP__ALGY (Allergies)...")
allergy_names = ['Penicillin', 'Aspirin', 'Iodine', 'Latex', 'Peanut', 'Shellfish', 'Sulfonamides', 'NSAIDs', 'Eggs', 'Milk']
allergies = []
for i in range(10):
    allergies.append({
        'ALG_ID': f"ALG{str(i+1).zfill(4)}",
        'ALG_NAME': allergy_names[i]
    })

df_allergies = pd.DataFrame(allergies)
df_allergies.to_csv(f'{OUTPUT_DIR}/STG_EHP__ALGY.csv', index=False)
print(f"   OK - {len(df_allergies)} rows")

# ============================================================================
# 7. STG_EHP__INSR (Insurance Master)
# ============================================================================
print("[7/16] Generating STG_EHP__INSR (Insurance Policies)...")
ins_types = [('HLT', 'Health'), ('ACC', 'Accident'), ('LIF', 'Life')]
ins_status = [('ACT', 'Active'), ('EXP', 'Expired'), ('CAN', 'Cancelled')]

insurance = []
for i in range(TRANSACTION_ROWS // 2):
    itype_cd, itype_des = ins_types[i % len(ins_types)]
    istat_cd, istat_des = ins_status[i % len(ins_status)]
    start_date = fake.date_between(start_date='-3y')
    insurance.append({
        'POL_NO': f"POL{fake.numerify(text='##########')}",
        'PAT_ID': f"PAT{str((i % MASTER_ROWS) + 1).zfill(5)}",
        'COM_NAME': f"{fake.word().capitalize()} Insurance",
        'ITYPE_CD': itype_cd,
        'ITYPE_DES': itype_des,
        'POL_ST': start_date,
        'POL_ED': start_date + timedelta(days=365),
        'ISTAT_CD': istat_cd,
        'ISTAT_DES': istat_des
    })

df_insurance = pd.DataFrame(insurance)
df_insurance.to_csv(f'{OUTPUT_DIR}/STG_EHP__INSR.csv', index=False)
print(f"   OK - {len(df_insurance)} rows")

# ============================================================================
# 8. STG_EHP__PTAL (Patient Allergy)
# ============================================================================
print("[8/16] Generating STG_EHP__PTAL (Patient Allergies)...")
ptal = []
for i in range(TRANSACTION_ROWS // 3):
    svr = np.random.choice(['MLD', 'MOD', 'SVR'])
    svr_des = {'MLD': 'Mild', 'MOD': 'Moderate', 'SVR': 'Severe'}[svr]
    rct = np.random.choice(['RSH', 'SWL', 'ANA'])
    rct_des = {'RSH': 'Rash', 'SWL': 'Swelling', 'ANA': 'Anaphylaxis'}[rct]
    ptal.append({
        'PAT_ID': f"PAT{str((i % MASTER_ROWS) + 1).zfill(5)}",
        'ALG_ID': f"ALG{str((i % 10) + 1).zfill(4)}",
        'SVR_CD': svr,
        'SVR_DES': svr_des,
        'RCT_CD': rct,
        'RCT_DES': rct_des
    })

df_ptal = pd.DataFrame(ptal).drop_duplicates(subset=['PAT_ID', 'ALG_ID'])
df_ptal.to_csv(f'{OUTPUT_DIR}/STG_EHP__PTAL.csv', index=False)
print(f"   OK - {len(df_ptal)} rows")

# ============================================================================
# 9. STG_EHP__MEDT (Medical Team)
# ============================================================================
print("[9/16] Generating STG_EHP__MEDT (Medical Teams)...")
medt = []
for i in range(TRANSACTION_ROWS // 2):
    stf_id = f"STF{str((i % len(df_staff)) + 1).zfill(5)}"
    role_cd = df_staff.iloc[i % len(df_staff)]['ROLE_CD']
    role_des = df_staff.iloc[i % len(df_staff)]['ROLE_DES']
    medt.append({
        'MEDT_ID': f"MEDT{str(i+1).zfill(5)}",
        'TEAM_NO': f"TEM{str(i+1).zfill(5)}",
        'STF_ID': stf_id,
        'ROLE_CD': role_cd,
        'ROLE_DES': role_des
    })

df_medt = pd.DataFrame(medt)
df_medt.to_csv(f'{OUTPUT_DIR}/STG_EHP__MEDT.csv', index=False)
print(f"   OK - {len(df_medt)} rows")

# ============================================================================
# 10. STG_EHP__VIST (Visit)
# ============================================================================
print("[10/16] Generating STG_EHP__VIST (Visits)...")
visit_status = [('COM', 'Completed'), ('ONV', 'Ongoing'), ('CAN', 'Cancelled')]
visit_types = [('IPT', 'Inpatient'), ('OPT', 'Outpatient'), ('ERM', 'Emergency')]

visits = []
for i in range(TRANSACTION_ROWS):
    vstat_cd, vstat_des = visit_status[i % len(visit_status)]
    vtype_cd, vtype_des = visit_types[i % len(visit_types)]
    vis_en = fake.date_time_between(start_date='-1y')
    vis_ex = vis_en + timedelta(hours=np.random.randint(1, 24))
    visits.append({
        'REFR_NO': f"REF{fake.numerify(text='##########')}",
        'PAT_ID': f"PAT{str((i % MASTER_ROWS) + 1).zfill(5)}",
        'MEDT_ID': f"MEDT{str((i % len(df_medt)) + 1).zfill(5)}",
        'VIS_EN': vis_en,
        'VIS_EX': vis_ex,
        'VSTAT_CD': vstat_cd,
        'VSTAT_DES': vstat_des,
        'VTYPE_CD': vtype_cd,
        'VTYPE_DES': vtype_des,
        'ROM_ID': f"ROM{str((i % len(df_rooms)) + 1).zfill(5)}"
    })

df_visits = pd.DataFrame(visits)
df_visits.to_csv(f'{OUTPUT_DIR}/STG_EHP__VIST.csv', index=False)
print(f"   OK - {len(df_visits)} rows")

# ============================================================================
# 11. STG_EHP__DIAG (Diagnosis)
# ============================================================================
print("[11/16] Generating STG_EHP__DIAG (Diagnoses)...")
diag_codes = ['A00', 'A01', 'A15', 'B20', 'C34', 'E10', 'E11', 'E14', 'F32', 'H26', 'I10', 'J45', 'K21', 'L89', 'M79', 'N18']
diag_status = [('CNF', 'Confirmed'), ('TEN', 'Tentative')]

diagnoses = []
for i in range(TRANSACTION_ROWS):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    rom_id = df_visits.iloc[i % len(df_visits)]['ROM_ID']
    dstat_cd, dstat_des = diag_status[i % len(diag_status)]
    diagnoses.append({
        'REFR_NO': refr_no,
        'DIG_TOT': np.random.randint(1, 4),
        'DIG_EN': fake.date_time_between(start_date='-1y'),
        'DIG_SEQ': np.random.randint(1, 4),
        'DIG_CD': diag_codes[i % len(diag_codes)],
        'DIG_DES': f"Diagnosis {fake.word()}",
        'DSTAT_CD': dstat_cd,
        'DSTAT_DES': dstat_des,
        'DTYPE_CD': 'PRI' if i % 3 == 0 else 'SEC',
        'DTYPE_DES': 'Primary' if i % 3 == 0 else 'Secondary',
        'ROM_ID': rom_id
    })

df_diagnoses = pd.DataFrame(diagnoses)
df_diagnoses.to_csv(f'{OUTPUT_DIR}/STG_EHP__DIAG.csv', index=False)
print(f"   OK - {len(df_diagnoses)} rows")

# ============================================================================
# 12. STG_EHP__TRTM (Treatment)
# ============================================================================
print("[12/16] Generating STG_EHP__TRTM (Treatments)...")
trtm_types = [('MED', 'Medication'), ('SUR', 'Surgery'), ('PHY', 'Physiotherapy')]
trtm_status = [('PLN', 'Planned'), ('ONG', 'Ongoing'), ('COM', 'Completed')]

treatments = []
for i in range(TRANSACTION_ROWS):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    ttype_cd, ttype_des = trtm_types[i % len(trtm_types)]
    tstat_cd, tstat_des = trtm_status[i % len(trtm_status)]
    treatments.append({
        'REFR_NO': refr_no,
        'TRTM_TOT': np.random.randint(1, 5),
        'TRTM_EN': fake.date_time_between(start_date='-1y'),
        'TTYPE_CD': ttype_cd,
        'TTYPE_DES': ttype_des,
        'TRTM_CD': f"TRT{fake.numerify(text='####')}",
        'TSTAT_CD': tstat_cd,
        'TRTM_DES': f"Treatment {fake.word()}",
        'TSTAT_DES': tstat_des,
        'TRTM_SEQ': np.random.randint(1, 4)
    })

df_treatments = pd.DataFrame(treatments)
df_treatments.to_csv(f'{OUTPUT_DIR}/STG_EHP__TRTM.csv', index=False)
print(f"   OK - {len(df_treatments)} rows")

# ============================================================================
# 13. STG_EHP__TMMD (Treatment Medicine)
# ============================================================================
print("[13/16] Generating STG_EHP__TMMD (Treatment Medicines)...")
dosages = [('5ML', '5ML'), ('10ML', '10ML'), ('100MG', '100MG'), ('500MG', '500MG')]
frequencies = [('OD', 'Once Daily'), ('BD', 'Twice Daily'), ('TDS', 'Thrice Daily')]
routes = [('ORL', 'Oral'), ('INJ', 'Injection'), ('TOP', 'Topical')]

tmmd = []
for i in range(TRANSACTION_ROWS):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    med_id = f"MED{str((i % len(df_medicines)) + 1).zfill(5)}"
    dos_cd, dos_des = dosages[i % len(dosages)]
    freq_cd, freq_des = frequencies[i % len(frequencies)]
    rute_cd, rute_des = routes[i % len(routes)]
    tmmd.append({
        'REFR_NO': refr_no,
        'MEDS_TOT': np.random.randint(1, 4),
        'MEDS_SEQ': np.random.randint(1, 4),
        'MED_ID': med_id,
        'QTY_NO': np.random.randint(1, 10),
        'DOS_CD': dos_cd,
        'DOS_DES': dos_des,
        'FREQ_CD': freq_cd,
        'FREQ_DES': freq_des,
        'RUTE_CD': rute_cd,
        'RUTE_DES': rute_des,
        'UNIT_DOS': np.random.choice(['mg', 'ml', 'tablet', 'capsule'])
    })

df_tmmd = pd.DataFrame(tmmd)
df_tmmd.to_csv(f'{OUTPUT_DIR}/STG_EHP__TMMD.csv', index=False)
print(f"   OK - {len(df_tmmd)} rows")

# ============================================================================
# 14. STG_EHP__BILL (Billing)
# ============================================================================
print("[14/16] Generating STG_EHP__BILL (Billing)...")
bill_status = [('OPN', 'Open'), ('PD', 'Paid'), ('PDC', 'Paid in part')]

billings = []
for i in range(TRANSACTION_ROWS):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    pat_id = df_visits.iloc[i % len(df_visits)]['PAT_ID']
    bstat_cd, bstat_des = bill_status[i % len(bill_status)]
    billings.append({
        'REFR_NO': refr_no,
        'PAT_ID': pat_id,
        'BILL_DATE': fake.date_between(start_date='-1y'),
        'BILL_AMT': np.random.randint(500000, 50000000),
        'BSTAT_CD': bstat_cd,
        'BSTAT_DES': bstat_des,
        'BTYPE_CD': 'RGL' if i % 2 == 0 else 'MED'
    })

df_billings = pd.DataFrame(billings)
df_billings.to_csv(f'{OUTPUT_DIR}/STG_EHP__BILL.csv', index=False)
print(f"   OK - {len(df_billings)} rows")

# ============================================================================
# 15. STG_EHP__MDBL (Medical Billing)
# ============================================================================
print("[15/16] Generating STG_EHP__MDBL (Medical Billing)...")
mdbl = []
for i in range(TRANSACTION_ROWS // 2):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    pat_id = df_visits.iloc[i % len(df_visits)]['PAT_ID']
    bstat_cd, bstat_des = bill_status[i % len(bill_status)]
    mdbl.append({
        'REFR_NO': refr_no,
        'PAT_ID': pat_id,
        'BILL_DATE': fake.date_between(start_date='-1y'),
        'BILL_AMT': np.random.randint(100000, 10000000),
        'BSTAT_CD': bstat_cd,
        'BSTAT_DES': bstat_des,
        'BTYPE_CD': 'DOC'
    })

df_mdbl = pd.DataFrame(mdbl)
df_mdbl.to_csv(f'{OUTPUT_DIR}/STG_EHP__MDBL.csv', index=False)
print(f"   OK - {len(df_mdbl)} rows")

# ============================================================================
# 16. STG_EHP__PMNT (Payment)
# ============================================================================
print("[16/16] Generating STG_EHP__PMNT (Payments)...")
payment_methods = [('CSH', 'Cash'), ('CRD', 'Credit Card'), ('CHK', 'Cheque'), ('INS', 'Insurance')]
payment_status = [('RCD', 'Received'), ('PND', 'Pending'), ('RJT', 'Rejected')]

payments = []
for i in range(TRANSACTION_ROWS):
    refr_no = df_visits.iloc[i % len(df_visits)]['REFR_NO']
    pat_id = df_visits.iloc[i % len(df_visits)]['PAT_ID']
    meth_cd, meth_des = payment_methods[i % len(payment_methods)]
    pstat_cd, pstat_des = payment_status[i % len(payment_status)]
    payments.append({
        'REFR_NO': refr_no,
        'PAT_ID': pat_id,
        'BTYPE_CD': 'RGL' if i % 2 == 0 else 'MED',
        'TOT_PMNT': np.random.randint(100000, 50000000),
        'PMNT_TMS': np.random.randint(1, 4),
        'PMNT_NO': f"PMT{fake.numerify(text='##########')}",
        'PMNT_DATE': fake.date_between(start_date='-1y'),
        'PMNT_AMT': np.random.randint(100000, 25000000),
        'METH_CD': meth_cd,
        'METH_DES': meth_des,
        'PSTAT_CD': pstat_cd,
        'PSTAT_DES': pstat_des
    })

df_payments = pd.DataFrame(payments)
df_payments.to_csv(f'{OUTPUT_DIR}/STG_EHP__PMNT.csv', index=False)
print(f"   OK - {len(df_payments)} rows")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY - Dummy Data Generation Complete")
print("=" * 80)

summary_data = {
    'Table': [
        'STG_EHP__PATN', 'STG_EHP__DPMT', 'STG_EHP__STFF', 'STG_EHP__ROMS',
        'STG_EHP__MDCN', 'STG_EHP__ALGY', 'STG_EHP__INSR', 'STG_EHP__PTAL',
        'STG_EHP__MEDT', 'STG_EHP__VIST', 'STG_EHP__DIAG', 'STG_EHP__TRTM',
        'STG_EHP__TMMD', 'STG_EHP__BILL', 'STG_EHP__MDBL', 'STG_EHP__PMNT'
    ],
    'Rows': [
        len(df_patients), len(df_departments), len(df_staff), len(df_rooms),
        len(df_medicines), len(df_allergies), len(df_insurance), len(df_ptal),
        len(df_medt), len(df_visits), len(df_diagnoses), len(df_treatments),
        len(df_tmmd), len(df_billings), len(df_mdbl), len(df_payments)
    ]
}

summary_df = pd.DataFrame(summary_data)
print("\n" + summary_df.to_string(index=False))

total = sum(summary_df['Rows'])
print(f"\nTotal CSV files: 16")
print(f"Total records generated: {total} rows")
print(f"Output directory: {OUTPUT_DIR}/")
print("=" * 80)
