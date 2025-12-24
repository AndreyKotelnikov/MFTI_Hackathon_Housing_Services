"""
Скрипт для применения funnel features к реальному датасету
"""

import argparse
import pandas as pd
from pathlib import Path
from funnel_features_extractor import FunnelFeaturesExtractor
from datetime import datetime


def process_dataset(input_path: str, 
                   json_path: str,
                   output_path: str = None,
                   chunksize: int = None):
    """
    Обработка датасета и добавление funnel features
    
    Args:
        input_path: Путь к входному CSV файлу с событиями
        json_path: Путь к JSON файлу с функциональными блоками
        output_path: Путь для сохранения результата (опционально)
        chunksize: Размер чанка для обработки больших файлов (опционально)
    
    Returns:
        DataFrame с добавленными колонками
    """
    print("=" * 80)
    print("ОБРАБОТКА ДАТАСЕТА - ДОБАВЛЕНИЕ FUNNEL FEATURES")
    print("=" * 80)
    
    start_time = datetime.now()
    
    # Проверка существования файлов
    if not Path(input_path).exists():
        raise FileNotFoundError(f"Файл не найден: {input_path}")
    if not Path(json_path).exists():
        raise FileNotFoundError(f"Файл не найден: {json_path}")
    
    # Инициализация экстрактора
    print(f"\n1. Инициализация экстрактора...")
    print(f"   JSON файл: {json_path}")
    extractor = FunnelFeaturesExtractor(json_path)
    
    blocks_info = extractor.get_block_info()
    print(f"   Загружено блоков: {len(blocks_info)}")
    print(f"   Будет создано колонок: {len(blocks_info) * 4}")
    
    # Загрузка данных
    print(f"\n2. Загрузка данных...")
    print(f"   Входной файл: {input_path}")
    
    if chunksize:
        print(f"   Режим: чанками по {chunksize} строк")
        # Для больших файлов - обработка по частям
        df_chunks = []
        for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunksize)):
            print(f"   Обработка чанка {i+1}...")
            chunk_transformed = extractor.transform(chunk)
            df_chunks.append(chunk_transformed)
        df = pd.concat(df_chunks, ignore_index=True)
    else:
        print(f"   Режим: загрузка всего файла")
        df = pd.read_csv(input_path)
        print(f"   Загружено строк: {len(df):,}")
        print(f"   Уникальных сессий: {df['global_session_id'].nunique():,}")
    
    # Применение преобразования
    print(f"\n3. Применение преобразования...")
    df_transformed = extractor.transform(df) if not chunksize else df
    
    # Статистика по результатам
    print(f"\n4. Статистика по результатам:")
    print(f"   Итоговое количество строк: {len(df_transformed):,}")
    print(f"   Итоговое количество колонок: {len(df_transformed.columns)}")
    print(f"   Добавлено новых колонок: {len(df_transformed.columns) - len(df.columns)}")
    
    # Статистика по блокам
    print(f"\n5. Топ-10 блоков по количеству взаимодействий:")
    all_count_cols = [col for col in df_transformed.columns if col.endswith('_count')]
    block_stats = []
    
    for col in all_count_cols:
        block_name = col.replace('_count', '')
        # Берем уникальные значения по сессиям
        sessions_data = df_transformed.groupby('global_session_id')[col].first()
        sessions_with_block = (sessions_data > 0).sum()
        total_actions = sessions_data.sum()
        avg_actions = sessions_data[sessions_data > 0].mean() if sessions_with_block > 0 else 0
        
        if sessions_with_block > 0:
            block_stats.append({
                'Блок': block_name,
                'Сессий': sessions_with_block,
                'Всего действий': int(total_actions),
                'Среднее на сессию': round(avg_actions, 2)
            })
    
    stats_df = pd.DataFrame(block_stats).sort_values('Сессий', ascending=False)
    print(stats_df.head(10).to_string(index=False))
    
    # Сохранение результата
    if output_path is None:
        # Генерируем имя выходного файла
        input_file = Path(input_path)
        output_path = str(input_file.parent / f"{input_file.stem}_with_funnels{input_file.suffix}")
    
    print(f"\n6. Сохранение результата...")
    print(f"   Выходной файл: {output_path}")
    df_transformed.to_csv(output_path, index=False)
    print(f"   ✓ Успешно сохранено")
    
    # Итоговое время
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"\n7. Время обработки: {duration:.1f} секунд ({duration/60:.1f} минут)")
    
    print("\n" + "=" * 80)
    print("ОБРАБОТКА ЗАВЕРШЕНА")
    print("=" * 80)
    
    return df_transformed


def main():
    """Основная функция для запуска из командной строки"""
    parser = argparse.ArgumentParser(
        description='Добавление funnel features в датасет событий'
    )
    
    parser.add_argument(
        'input',
        type=str,
        help='Путь к входному CSV файлу с событиями'
    )
    
    parser.add_argument(
        '--json',
        type=str,
        default='/mnt/user-data/uploads/1766523980935_categorized_combinations_with_funnels.json',
        help='Путь к JSON файлу с функциональными блоками'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Путь для сохранения результата (по умолчанию: добавляется _with_funnels к имени входного файла)'
    )
    
    parser.add_argument(
        '--chunksize',
        type=int,
        default=None,
        help='Размер чанка для обработки больших файлов (например, 100000)'
    )
    
    args = parser.parse_args()
    
    # Обработка
    process_dataset(
        input_path=args.input,
        json_path=args.json,
        output_path=args.output,
        chunksize=args.chunksize
    )


if __name__ == '__main__':
    main()
