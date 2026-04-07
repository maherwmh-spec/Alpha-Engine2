"""
dashboard/arabic_utils.py
=========================
Simplified English-only layout utilities for the Alpha-Engine2 dashboard.

Arabic support has been removed in favour of simplicity and stability.
All functions are kept as no-op pass-throughs so that any remaining
call-sites in app.py continue to work without modification.
"""
from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pass-through helpers (no Arabic processing)
# ---------------------------------------------------------------------------

def fix_arabic(text: Optional[str]) -> str:
    """Return text unchanged (Arabic support removed)."""
    return text or ""


def fix_arabic_series(series: pd.Series) -> pd.Series:
    """Return series unchanged (Arabic support removed)."""
    return series


def fix_arabic_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Return DataFrame unchanged (Arabic support removed)."""
    return df


def fix_arabic_list(items: List[str]) -> List[str]:
    """Return list unchanged (Arabic support removed)."""
    return list(items)


def is_arabic_supported() -> bool:
    """Always False -- Arabic libraries have been removed."""
    return False


def get_support_status() -> str:
    """Return a status string for the system info panel."""
    return "English-only mode (Arabic support removed)"


# ---------------------------------------------------------------------------
# Plotly layout helper
# ---------------------------------------------------------------------------

def arabic_plotly_layout(
    title: Optional[str] = None,
    xaxis_title: Optional[str] = None,
    yaxis_title: Optional[str] = None,
    **kwargs,
) -> dict:
    """
    Build a Plotly update_layout dict with clean English styling.

    Args:
        title      : chart title
        xaxis_title: x-axis label
        yaxis_title: y-axis label
        **kwargs   : extra params passed directly to update_layout

    Returns:
        dict suitable for fig.update_layout(**arabic_plotly_layout(...))
    """
    layout: dict = {
        "font": {
            "family": "Arial, 'Segoe UI', sans-serif",
            "size": 13,
        },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        **kwargs,
    }

    if title:
        layout["title"] = {
            "text": title,
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 16},
        }

    if xaxis_title:
        layout.setdefault("xaxis", {})["title"] = xaxis_title

    if yaxis_title:
        layout.setdefault("yaxis", {})["title"] = yaxis_title

    return layout
