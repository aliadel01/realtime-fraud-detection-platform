import polars as pl
import pandas as pd
import numpy as np
import xgboost as xgb
import gc
from sklearn.metrics import f1_score, precision_score, recall_score

# ---------- Step 1: Merge ----------
trans_cols = [
    "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
    "ProductCD", "card1", "card2", "card3", "card4", "card5", "card6",
    "addr1", "addr2", "P_emaildomain", "R_emaildomain"
]
ident_cols = ["TransactionID", "id_01", "id_02", "DeviceType", "DeviceInfo"]

path_trans = "/content/drive/MyDrive/detection_data/train_transaction.csv"
path_ident = "/content/drive/MyDrive/detection_data/train_identity.csv"

trans_pl = pl.read_csv(path_trans, columns=trans_cols)
ident_pl = pl.read_csv(path_ident, columns=ident_cols)

df = (
    trans_pl.join(ident_pl, on="TransactionID", how="left")
    .sort("TransactionDT")
    .to_pandas()
)
del trans_pl, ident_pl
gc.collect()

# ---------- Step 2: Time setup ----------
df["TransactionDT_dt"] = pd.to_datetime(df["TransactionDT"], unit="s", origin="2017-11-30")
df = df.sort_values("TransactionDT_dt").reset_index(drop=True)
df["_row_id"] = df.index  # stable unique id — needed for safe merge-back after rolling

# ---------- Step 3: Rolling features (FIXED — safe merge via _row_id) ----------
def add_rolling_features(df, key="card1", windows=("1h", "24h")):
    df = df.copy()
    for w in windows:
        def _roll(g):
            g = g.sort_values("TransactionDT_dt").set_index("TransactionDT_dt")
            cnt = g["TransactionAmt"].rolling(w, closed="left").count()
            summ = g["TransactionAmt"].rolling(w, closed="left").sum()
            return pd.DataFrame({
                "_row_id": g["_row_id"].values,
                f"{key}_txn_count_{w}": cnt.values,
                f"{key}_amt_sum_{w}": summ.values,
            })

        rolled = df.groupby(key, group_keys=False).apply(_roll)
        df = df.merge(rolled, on="_row_id", how="left")
        df[f"{key}_amt_mean_{w}"] = df[f"{key}_amt_sum_{w}"] / df[f"{key}_txn_count_{w}"].replace(0, np.nan)

    return df

df = add_rolling_features(df)

# std only for 24h — priority feature, extra rolling call
def add_rolling_std(df, key="card1", window="24h"):
    def _roll(g):
        g = g.sort_values("TransactionDT_dt").set_index("TransactionDT_dt")
        std = g["TransactionAmt"].rolling(window, closed="left").std()
        return pd.DataFrame({"_row_id": g["_row_id"].values, f"{key}_amt_std_{window}": std.values})
    rolled = df.groupby(key, group_keys=False).apply(_roll)
    return df.merge(rolled, on="_row_id", how="left")

df = add_rolling_std(df)

# time since last txn — computed ONCE on full sorted df, before split (continuity preserved)
df["card1_time_since_last"] = df.groupby("card1")["TransactionDT"].diff()

# ratio feature — current amt vs rolling mean
df["amt_to_mean_24h_ratio"] = df["TransactionAmt"] / df["card1_amt_mean_24h"].replace(0, np.nan)

df = df.drop(columns="_row_id")

# ---------- Step 4: Time split ----------
split_point = df["TransactionDT"].quantile(0.8)
train = df[df["TransactionDT"] <= split_point].copy()
test = df[df["TransactionDT"] > split_point].copy()

# ---------- Step 5: Categorical encode — ONE consistent mapping, train fit → test transform ----------
cat_cols = train.select_dtypes(include=["object"]).columns.tolist()
for c in cat_cols:
    train[c] = train[c].astype("category")
    test[c] = pd.Categorical(test[c], categories=train[c].cat.categories)  # same category set, same codes

drop_cols = ["TransactionID", "isFraud", "TransactionDT", "TransactionDT_dt"]
X_train = train.drop(columns=drop_cols)
y_train = train["isFraud"]
X_test = test.drop(columns=drop_cols)
y_test = test["isFraud"]

ratio = (y_train == 0).sum() / (y_train == 1).sum()

# ---------- Step 6: Train ----------
model = xgb.XGBClassifier(
    scale_pos_weight=ratio,
    enable_categorical=True,
    tree_method="hist",
    eval_metric="aucpr",
    n_estimators=200,
    max_depth=6,
    random_state=42,
)
model.fit(X_train, y_train)
preds = model.predict(X_test)

print(f"F1: {f1_score(y_test, preds):.4f} | Precision: {precision_score(y_test, preds):.4f} | Recall: {recall_score(y_test, preds):.4f}")

# ---------- Step 7: ONNX export — reuse SAME category mapping, no re-derivation ----------
!pip install onnxmltools onnxconverter-common skl2onnx onnxruntime
import sys
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType

X_train_encoded = X_train.copy()
X_test_encoded = X_test.copy()
for c in cat_cols:
    X_train_encoded[c] = X_train_encoded[c].cat.codes          # uses train's category order
    X_test_encoded[c] = pd.Categorical(X_test[c], categories=train[c].cat.categories).codes  # SAME order — fixes bug 4

feature_names = [f"f{i}" for i in range(X_train_encoded.shape[1])]
X_train_encoded.columns = feature_names
X_test_encoded.columns = feature_names

model_onnx_ready = xgb.XGBClassifier(
    scale_pos_weight=ratio, tree_method="hist", eval_metric="aucpr",
    n_estimators=200, max_depth=6, random_state=42,
)
model_onnx_ready.fit(X_train_encoded, y_train)

sys.setrecursionlimit(50000)
initial_type = [("float_input", FloatTensorType([None, X_train_encoded.shape[1]]))]
onnx_model = onnxmltools.convert_xgboost(model_onnx_ready, initial_types=initial_type)

with open("fraud_xgb_v0.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())

print("Saved fraud_xgb_v0.onnx")