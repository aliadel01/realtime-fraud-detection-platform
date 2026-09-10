import pandas as pd

# ---------- Load ----------
trans = pd.read_csv("data/train_transaction.csv")
trans = trans.sort_values("TransactionDT").reset_index(drop=True)

# ==================================================
# 1. USER REFERENCE — current (latest) state per card1
# ==================================================
user_attr_cols = ["card2", "card3", "card4", "card5", "card6",
                   "addr1", "addr2", "P_emaildomain", "R_emaildomain"]

user_reference = (
    trans[["card1"] + user_attr_cols + ["TransactionDT"]]
    .dropna(subset=["card1"])
    .sort_values("TransactionDT")
    .groupby("card1")
    .last()  # latest known value per card1
    .reset_index()
    .drop(columns="TransactionDT")
)
user_reference.insert(0, "user_reference_sk", range(1, len(user_reference) + 1))

# ==================================================
# 2. MERCHANT DATA — current fraud rate per ProductCD
# ==================================================
merchant_data = (
    trans.groupby("ProductCD")
    .agg(fraud_rate=("isFraud", "mean"))
    .reset_index()
)
merchant_data.insert(0, "merchant_data_sk", range(1, len(merchant_data) + 1))

# ==================================================
# 3. TRANSACTION FACT — FK to both dims
# ==================================================
user_fk_map = user_reference.set_index("card1")["user_reference_sk"]
merch_fk_map = merchant_data.set_index("ProductCD")["merchant_data_sk"]

transaction_fact = trans.drop(columns=user_attr_cols + ["ProductCD"])
transaction_fact["user_reference_sk"] = transaction_fact["card1"].map(user_fk_map)
transaction_fact["merchant_data_sk"] = transaction_fact["ProductCD"].map(merch_fk_map)

# ---------- Save ----------
transaction_fact.to_csv("data/train_transaction_fact.csv", index=False)
user_reference.to_csv("data/user_reference.csv", index=False)
merchant_data.to_csv("data/merchant_data.csv", index=False)

print("transaction_fact:", transaction_fact.shape)
print("user_reference:", user_reference.shape)
print("merchant_data:", merchant_data.shape)