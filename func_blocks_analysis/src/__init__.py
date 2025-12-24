"""
Пакет для анализа данных пользовательского поведения МКД приложения
"""

__version__ = "1.0.0"
__author__ = "Data Analysis Team"

from .data_preprocessing import DataPreprocessor
from .eda_analysis import EDAAnalyzer
from .sequence_mining import SequenceAnalyzer
from .visualization import VisualizationEngine

__all__ = [
    'DataPreprocessor',
    'EDAAnalyzer',
    'SequenceAnalyzer',
    'VisualizationEngine'
]
