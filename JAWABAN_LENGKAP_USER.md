# Jawaban Lengkap: NIK/NIP Matching & Employee ID Handling

## ❓ Pertanyaan User

> Sebelumnya kan NIK/NIP di cek kolom `employee_no_subholding` di tabel employees, jika ada maka tidak perlu create record employees baru tapi employee_id perlu di ambil untuk push data lainnya nanti ke tabel yang memerlukan employee_id. Jika ternyata NIK/NIP tidak ada di tabel employees baru create employee_id dan push (ini cara kamu kan?)

---

## ✅ JAWABAN: YA, BENAR! Dan Sudah di-Implement

Mari breakdown detail flow yang sudah saya fix:

### 1️⃣ CEK NIK/NIP di Column employee_no_subholding

**Step 1: Transform Stage**
```python
# File: src/transformer/utils.py
# Function: enrich_with_employee_id()

# Query tabel employees
cur.execute(
    "SELECT employee_no_subholding, employee_id "
    "FROM employees "
    "WHERE employee_no_subholding IS NOT NULL"
)

# Get semua mapping dari DB
db_map = {
    str(row["employee_no_subholding"]).strip(): str(row["employee_id"])
    for row in rows
}
```

**Result:**
```
DB employees table:
┌──────────────────────┬─────────────────────────┐
│ employee_no_subhold  │ employee_id             │
├──────────────────────┼─────────────────────────┤
│ 123456789            │ aaaa-bbbb-cccc-dddd-ee  │ ← Existing
│ 987654321            │ xxxx-yyyy-zzzz-wwww-qq  │ ← Existing
│ 111111111            │ pppp-qqqq-rrrr-ssss-tt  │ ← Existing
└──────────────────────┴─────────────────────────┘
```

---

### 2️⃣ JIKA ADA: Ambil employee_id, JANGAN create baru

**Match Logic:**
```python
for nik in all_niks:
    if nik in db_map:
        nik_to_id[nik] = db_map[nik]  # ← USE EXISTING!
        matched_count += 1
```

**Example:**
```
Excel Input:
┌────────┬──────────────────┐
│ NIK/NIP│ NAMA             │
├────────┼──────────────────┤
│ 123456 │ John Doe         │
│ 654321 │ Jane Smith       │
└────────┴──────────────────┘

Step 1: Check NIK 123456 in DB
  DB query: SELECT * FROM employees 
            WHERE employee_no_subholding = '123456'
  Result: FOUND! employee_id = 'aaaa-bbbb-cccc-dddd-ee'
  
Step 2: Check NIK 654321 in DB
  DB query: SELECT * FROM employees 
            WHERE employee_no_subholding = '654321'
  Result: NOT FOUND
  
Final Mapping:
  '123456' → 'aaaa-bbbb-cccc-dddd-ee' (dari DB, existing)
  '654321' → 'xxxx-yyyy-zzzz-wwww-qq' (generate UUID baru)
```

**Logging:**
```
[Transformer] Ditemukan 2 NIK/NIP unik untuk lookup.
[Transformer] DB lookup: 1 employee NIK/NIP ditemukan di tabel employees.
[Transformer] employee_id lookup result:
  - 1 NIK/NIP matched dari DB (gunakan existing employee_id)
  - 1 NIK/NIP TIDAK ada di DB (generate UUID baru)
```

---

### 3️⃣ JIKA TIDAK ADA: Create employee_id (UUID) dan push

**Generate UUID:**
```python
import uuid

# Jika NIK tidak ketemu di DB
new_employee_id = str(uuid.uuid4())  
# Result: 'xxxx-yyyy-zzzz-wwww-qq'

# Assign ke mapping
nik_to_id['654321'] = new_employee_id
```

**Push ke Employees Table:**
```
LOAD Stage:

Step 1: Insert Employees table FIRST (enforced order)
  ├─ employee_id 'aaaa-bbbb-...' (existing) → UPSERT
  └─ employee_id 'xxxx-yyyy-...' (new UUID) → INSERT

Raw SQL:
  INSERT INTO employees (employee_id, employee_no_subholding, fullname, ...)
  VALUES 
    ('aaaa-bbbb-cccc-dddd-ee', '123456', 'John Doe', ...),  ← UPSERT
    ('xxxx-yyyy-zzzz-wwww-qq', '654321', 'Jane Smith', ...)  ← INSERT
  ON CONFLICT (employee_id) DO UPDATE SET ...;

Result: 2 rows affected
  - 1 UPDATE (existing employee_id with new data)
  - 1 INSERT (new employee_id)
```

---

### 4️⃣ AMBIL employee_id untuk Push Data Lainnya

**Step 1: Query Ulang Setelah Employees Insert**
```python
# File: src/loader/pg_utils.py
# Function: get_existing_employee_ids()

SELECT employee_id FROM employees

Result:
existing_employee_ids = {
  'aaaa-bbbb-cccc-dddd-ee',
  'xxxx-yyyy-zzzz-wwww-qq',
  'pppp-qqqq-rrrr-ssss-tt',
  ...
}
```

**Step 2: Use untuk Validasi Dependent Tables**
```python
# Sebelum insert riwayat_pekerjaan, riwayat_pendidikan, dll
# Validasi setiap row punya employee_id yang VALID

for each row in riwayat_pekerjaan:
    if row['employee_id'] in existing_employee_ids:
        ✓ VALID → INSERT
    else:
        ❌ INVALID → DROP & LOG
```

**Example:**
```
riwayat_pekerjaan DataFrame (15 rows):
┌──────────────────────┬──────────┐
│ employee_id          │ jabatan  │
├──────────────────────┼──────────┤
│ aaaa-bbbb-cccc-dd... │ Manager  │ ← ✓ Valid (exists)
│ xxxx-yyyy-zzzz-ww... │ Staff    │ ← ✓ Valid (exists)
│ invalid-uuid-123     │ Analyst  │ ← ❌ Invalid (NOT exists)
│ aaaa-bbbb-cccc-dd... │ Intern   │ ← ✓ Valid (exists)
│ ...                  │ ...      │
└──────────────────────┴──────────┘

After Validation:
  Valid rows: 14
  Invalid rows (dropped): 1
  
  INSERT 14 valid rows to riwayat_pekerjaan
```

---

## 🎯 Complete Data Flow (dari awal sampai akhir)

```
STAGE 1: EXTRACT
═══════════════════
  Excel File
    ├─ Raw Data sheet → DataFrame with NIK/NIP
    └─ Assesment sheet → DataFrame with NIK/NIP

STAGE 2: TRANSFORM
═══════════════════
  Step A: fill_missing_nik()
    └─ Isi NIK kosong dengan "unk-5001", "unk-5002", ...
    
  Step B: enrich_with_employee_id() ← KUNCI!
    │
    ├─ Query DB: SELECT employee_no_subholding, employee_id
    │           FROM employees
    │
    ├─ For each NIK/NIP in Excel:
    │   ├─ If matches employee_no_subholding → USE existing ID
    │   │  Log: "1 NIK/NIP matched dari DB"
    │   │
    │   └─ Else → GENERATE UUID baru
    │      Log: "1 NIK/NIP TIDAK ada di DB"
    │
    └─ Result: Semua DataFrame sudah punya employee_id column
       
       Mapping: NIK → employee_id
       ├─ '123456' → 'aaaa-bbbb-...' (from DB)
       ├─ '654321' → 'xxxx-yyyy-...' (new UUID)
       ├─ 'unk-5001' → 'pppp-qqqq-...' (new UUID)
       └─ ...

STAGE 3: LOAD
═══════════════════
  Enforced Load Order:
  
  STEP 1: employees table (PARENT)
  ├─ Input: employees DataFrame (with employee_id column)
  ├─ UPSERT:
  │   - UPDATE existing employee_id
  │   - INSERT new employee_id
  ├─ ✓ Commit
  │
  └─ Query DB: SELECT employee_id FROM employees
     Result: existing_employee_ids = {all IDs in DB}
  
  STEP 2-5: Dependent tables (riwayat_pendidikan, riwayat_pekerjaan, etc.)
  ├─ For each dependent table:
  │   ├─ Validate: Check each row's employee_id vs existing_employee_ids
  │   ├─ Filter: Keep only rows with VALID employee_id
  │   ├─ Log: Drop count + details
  │   └─ INSERT valid rows only
  │
  └─ Result: ALL ROWS have valid FK ✓

RESULT
═══════════════════
  ✅ NO FOREIGN KEY VIOLATIONS
  ✅ Clean, validated data in ALL tables
  ✅ Proper audit trail in logs
```

---

## 📊 Specific Scenarios

### Scenario 1: Existing Employee (jangan create baru)
```
Excel: NIK = '123456', NAMA = 'John Doe'

DB Check:
  SELECT employee_id FROM employees 
  WHERE employee_no_subholding = '123456'
  
  Result: employee_id = 'aaaa-bbbb-cccc-dddd-ee'

Action: ✓ USE EXISTING
  └─ Don't create new record
  └─ Use 'aaaa-bbbb-cccc-dddd-ee' for all dependent tables

Logging:
  [Transformer] 1 NIK/NIP matched dari DB (gunakan existing employee_id)
```

### Scenario 2: New Employee (create baru)
```
Excel: NIK = '999999', NAMA = 'Jane Smith'

DB Check:
  SELECT employee_id FROM employees 
  WHERE employee_no_subholding = '999999'
  
  Result: (no rows)

Action: ✓ CREATE NEW
  ├─ Generate UUID: 'xxxx-yyyy-zzzz-wwww-qq'
  ├─ Assign to mapping
  └─ Will be INSERTed when employees table is loaded

Logging:
  [Transformer] 1 NIK/NIP TIDAK ada di DB (generate UUID baru)
```

### Scenario 3: NIK/NIP Typo/Mismatch (before vs after)
```
BEFORE (matching by NAMA - WRONG):
─────────────────────────────────
  Excel: NAMA = 'John Do' (typo!)
  
  DB: NAMA = 'John Doe'
  
  Match: No match (typo mismatch)
  
  Action: Generate new UUID
  
  Problem: ❌ Duplicate employee!

AFTER (matching by NIK/NIP - CORRECT):
──────────────────────────────────────
  Excel: NIK/NIP = '123456', NAMA = 'John Do' (typo, tapi OK)
  
  DB: NIK/NIP = '123456', NAMA = 'John Doe'
  
  Match: ✓ Matched by NIK/NIP! (NAMA typo doesn't matter)
  
  Action: Use existing employee_id
  
  Result: ✅ No duplicate!
```

---

## ⚙️ Implementation Details

**File: src/transformer/utils.py**
```
Function: enrich_with_employee_id()
- Line 200-260
- Match: NIK/NIP → employee_no_subholding
- Query: SELECT employee_no_subholding, employee_id FROM employees
- Result: Add employee_id column to all DataFrames
```

**File: src/loader/pg_utils.py**
```
New Functions:
1. get_existing_employee_ids(conn) → set
   - Query: SELECT employee_id FROM employees
   - Result: Set of valid employee_ids in DB

2. validate_and_filter_employee_ids(df, existing_ids, dataset_name) → (df, count)
   - Filter: Keep rows with valid employee_id
   - Drop: Rows with invalid employee_id
   - Result: Cleaned DataFrame + drop count

Updated Function:
3. save_to_postgres(datasets)
   - Before: Random order, no validation
   - After: Enforced order + post-insert validation
   - Steps:
     1. employees first
     2. Query existing_employee_ids
     3. Validate dependent tables
     4. Insert validated rows
```

---

## 📋 Checklist: Apa yang sudah done

- [x] Match by NIK/NIP (not NAMA)
  - [x] Query `employee_no_subholding` instead of `fullname`
  - [x] Better accuracy & stability

- [x] Ambil existing employee_id
  - [x] If NIK/NIP matches → use employee_id dari DB
  - [x] Don't create duplicate employee

- [x] Create new employee_id
  - [x] If NIK/NIP doesn't match → generate UUID baru
  - [x] Push ke employees table

- [x] Push data ke table lain
  - [x] Enforced load order (employees first)
  - [x] Query existing_employee_ids after employees insert
  - [x] Validate each dependent table
  - [x] Insert only valid rows (FK constraint OK)

- [x] Logging & Monitoring
  - [x] Track matched vs unmatched NIK/NIP
  - [x] Log dropped rows with details
  - [x] Audit trail untuk troubleshooting

---

## 🚀 Ready to Test!

Sekarang ETL sudah siap dengan:
1. ✅ Correct matching strategy (NIK/NIP)
2. ✅ Proper employee_id handling
3. ✅ Enforced load order
4. ✅ Foreign key validation
5. ✅ Detailed logging

Untuk test:
```bash
python main.py input/your_file.xlsx
# atau via API:
# POST /upload dengan file Excel
```

Monitor logs untuk:
```
[Transformer] Ditemukan 100 NIK/NIP unik untuk lookup.
[Transformer] DB lookup: 95 employee NIK/NIP ditemukan di tabel employees.
[Transformer] employee_id lookup result:
  - 95 NIK/NIP matched dari DB
  - 5 NIK/NIP TIDAK ada di DB
```

Jika semua status ✓ → ETL siap ke production! 🎉
