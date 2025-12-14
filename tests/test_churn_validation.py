import pandas as pd
import pytest

class TestChurnValidation:
    """Тесты валидации данных по оттоку пользователей"""

    def test_churn_steps_validation(self, test_data, churn_days):
        df = test_data.sort_values(["device_id", "event_dt"])
        violations = []

        for device_id, g in df.groupby("device_id", sort=False):
            churn_steps = g[g["is_churn"] == True]

            for _, s1 in churn_steps.iterrows():
                s2 = g[
                    (g["event_dt"] > s1["event_dt"]) &
                    ((g["event_dt"] - s1["event_dt"]).dt.days < churn_days)
                ]

                if not s2.empty:
                    violations.append({
                        "device_id": device_id,
                        "event_dt": s1["event_dt"],
                        "screen": s1["screen"],
                        "feature": s1["feature"],
                        "is_churn": True,
                    })
                    break

            if violations:
                break

        assert not violations, f"Найдены некорректные is_churn шаги: {violations}"
    
    def test_churn_always_finish(self, test_data):
        """2. Проверка что шаги оттока всегда финишные"""
        df = test_data
        invalid = df[
            (df["is_churn"] == True) &
            (df["is_finish"] == False)
        ]

        assert invalid.empty, (
            "Найдены шаги с is_churn=True и is_finish=False:\n"
            f"{invalid[['device_id', 'event_dt', 'screen', 'feature', 'is_churn', 'is_finish']].head()}"
        )
    
    def test_finish_not_always_churn(self, test_data):
        """3. Проверка что финишные шаги не всегда оттока"""
        df = test_data
        valid = df[
            (df["is_churn"] == False) &
            (df["is_finish"] == True)
        ]

        assert not valid.empty, (
            "Не найдено ни одного шага с is_finish=True и is_churn=False — "
            "финишные шаги всегда помечены как отток, что неверно"
        )
        
    def test_non_churn_steps_validation(self, test_data, churn_days, period_until):
        df = test_data.sort_values(["device_id", "event_dt"])
        violations = []

        # timezone safety
        if df["event_dt"].dt.tz is not None and period_until.tzinfo is None:
            period_until = period_until.tz_localize(df["event_dt"].dt.tz)

        for device_id, g in df.groupby("device_id", sort=False):
            # берём только non-churn шаги
            non_churn = g[g["is_churn"] == False]

            if non_churn.empty:
                continue

            # берём ПОСЛЕДНИЙ non-churn шаг
            s1 = non_churn.iloc[-1]

            # если он ещё "не дожил" до churn_days — пропускаем
            if (period_until - s1["event_dt"]).days <= churn_days:
                continue

            # проверяем, есть ли шаг в churn_days после него
            has_next = (
                (g["event_dt"] >= s1["event_dt"]) &
                ((g["event_dt"] - s1["event_dt"]).dt.days <= churn_days)
            ).any()

            if not has_next:
                violations.append({
                    "device_id": device_id,
                    "event_dt": s1["event_dt"],
                    "screen": s1["screen"],
                    "feature": s1["feature"],
                })
                break  # early exit — тесту достаточно одной ошибки

        assert not violations, (
            f"Найдены некорректные шаги с is_churn=False: {violations}"
        )
    
    def test_finish_steps_validation(self, test_data):
        """5. Проверка шагов отмеченных как финишные"""
        df = test_data.sort_values(["device_id", "session_id", "event_dt"])

        # максимальная дата события в каждой сессии
        last_event_dt = df.groupby(
            ["device_id", "session_id"], sort=False
        )["event_dt"].transform("max")

        # финишные шаги, которые не последние
        violations = df[
            (df["is_finish"] == True) &
            (df["event_dt"] != last_event_dt)
        ]

        assert violations.empty, (
            "Найдены некорректные финишные шаги: "
            f"{violations.head(1).to_dict(orient='records')}"
        )
    
    def test_non_finish_steps_validation(self, test_data):
        """6. Проверка шагов отмеченных как не финишные"""
        df = test_data.sort_values(["device_id", "session_id", "event_dt"])
        violations = []

        for (device_id, session_id), g in df.groupby(
            ["device_id", "session_id"], sort=False
        ):
            last = g.iloc[-1]

            # последний шаг в сессии не может быть non-finish
            if last["is_finish"] == False:
                violations.append({
                    "device_id": device_id,
                    "session_id": session_id,
                    "event_dt": last["event_dt"],
                    "screen": last["screen"],
                    "feature": last["feature"],
                })
                break

        assert not violations, (
            f"Найдены некорректные шаги с is_finish=False: {violations}"
        )
