# MFTI_Hackathon_Housing_Services


## Запуск тестов

### Проверка базового очищенного датасета

Создать в проекта папку data, положить туда файл clean_data.csv.
Создать и активировать виртуальное окружение, установить зависимости, запустить `tests/test_churn_validation.py`

```bash
python3 -m venv ./venv
source venv/bin/activate
pip3 install -r requirements.txt

pytest tests/test_churn_validation.py --test-csv="data/clean_data.csv"
```

--- 