import os
import json
import torch
import joblib
import pickle
import numpy as np
import pandas as pd

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from mlmodel import GraphSAGEChurn

SAVE_DIR = "../data/graph-save"
DEVICE = "cpu"

app = FastAPI(title="Churn GNN API")


# ---------- utils ----------

def load_vectorizer(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def build_node_text_embedding(screen, feature, action, vectorizer):
    text = f"{screen} {feature} {action}"
    return vectorizer.transform([text]).toarray().squeeze().astype(np.float32)


# ---------- load artifacts once ----------

checkpoint = torch.load(os.path.join(SAVE_DIR, "gnn_model.pth"), map_location=DEVICE)

model = GraphSAGEChurn(
    in_channels=checkpoint["config"]["in_channels"],
    hidden_channels=checkpoint["config"]["hidden_channels"],
    num_layers=checkpoint["config"]["num_layers"],
    dropout=checkpoint["config"]["dropout"],
).to(DEVICE)

model.load_state_dict(checkpoint["model_state_dict"])
model.eval()

scaler_x = joblib.load(os.path.join(SAVE_DIR, "scaler_x.pkl"))
edge_index_base = torch.load(os.path.join(SAVE_DIR, "edge_index.pt"), map_location=DEVICE)

features_df = pd.read_csv(
    os.path.join(SAVE_DIR, "node_features.csv"),
    index_col="node_id",
)

vectorizer = load_vectorizer(os.path.join(SAVE_DIR, "tfidf_vectorizer.pkl"))

mean_churn_rate = features_df["churn_rate"].mean()


# ---------- api schema ----------

class PredictRequest(BaseModel):
    node_id: str
    screen: str
    feature: str
    action: str


class PredictResponse(BaseModel):
    churn_rate: float
    churn_vs_mean_percent: float


# ---------- endpoint ----------

@app.post("/api/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    num_old_nodes = features_df.shape[0]

    # if not (0 <= req.existing_node_idx < num_old_nodes):
    #     raise HTTPException(
    #         status_code=400,
    #         detail=f"existing_node_idx must be in [0, {num_old_nodes - 1}]",
    #     )
    
    existing_node_idx = features_df.index.get_loc(req.node_id)

    # existing_node_idx = features_df[features_df['node_id'] == req.node_id].index

    # ---- build new node features ----
    text_emb = build_node_text_embedding(
        req.screen,
        req.feature,
        req.action,
        vectorizer,
    )

    new_node = features_df.mean().copy()

    for i, v in enumerate(text_emb):
        col = f"text_emb_{i}"
        if col in new_node:
            new_node[col] = v

    new_node_df = pd.DataFrame(
        [new_node],
        columns=features_df.columns,
    )

    # ---- stack features ----
    X_all = np.vstack([
        features_df.values,
        new_node_df.values,
    ])

    X_all_scaled = scaler_x.transform(X_all)
    x_tensor = torch.tensor(X_all_scaled, dtype=torch.float, device=DEVICE)

    # ---- add edge ----
    new_node_idx = num_old_nodes

    new_edges = torch.tensor(
        [
            [existing_node_idx, new_node_idx],
            [new_node_idx, existing_node_idx],
        ],
        dtype=torch.long,
        device=DEVICE,
    ).t()

    edge_index = torch.cat([edge_index_base, new_edges], dim=1)

    # ---- predict ----
    with torch.no_grad():
        out = model(x_tensor, edge_index)

    churn_rate = float(out[new_node_idx].item())

    churn_vs_mean_percent = (
        (churn_rate / mean_churn_rate) * 100
        if mean_churn_rate > 0
        else 0.0
    )

    return PredictResponse(
        churn_rate=churn_rate,
        churn_vs_mean_percent=churn_vs_mean_percent,
    )


# Адекватные запросы:
# {
#   "node_id": "137091633a6334ce94ced79cab6ec771",
#   "screen": "Новая заявка",
#   "feature": "Выбор квартиры",
#   "action": "Тап на квартиру"
# }

