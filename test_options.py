class TestOptions:
    def __init__(self, total_vouchers: int = 0):
        self._total_vouchers = total_vouchers

        self._to_sanatorium = 0
        self._to_sanatorium_percent = float(0)

        self._to_reserve = 0
        self._to_reserve_percent = float(0)

        self._to_exchange = 0
        self._to_exchange_percent = float(0)

        self._medical_units = 0
        self.to_medical_units = {}
        self.to_medical_units_percents = {}

    @property
    def total_vouchers(self):
        return self._total_vouchers

    @total_vouchers.setter
    def total_vouchers(self, value):
        self._total_vouchers = value

    @property
    def exists_vouchers(self):
        return self._total_vouchers - sum(
            [
                self._to_sanatorium,
                self._to_reserve,
                self._to_exchange,
                self.sum_to_medical_units
            ]
        )

    @property
    def to_sanatorium(self):
        return self._to_sanatorium

    @to_sanatorium.setter
    def to_sanatorium(self, value):
        self._to_sanatorium = value

    @property
    def to_sanatorium_max(self):
        return self._total_vouchers - (self._to_reserve + self._to_exchange + self.sum_to_medical_units)

    @property
    def to_sanatorium_percent(self):
        return round((self._to_sanatorium / self._total_vouchers * 100), 2)

    @property
    def to_reserve(self):
        return self._to_reserve

    @to_reserve.setter
    def to_reserve(self, value):
        self._to_reserve = value

    @property
    def to_reserve_max(self):
        return self._total_vouchers - (self._to_sanatorium + self._to_exchange + self.sum_to_medical_units)

    @property
    def to_reverse_percent(self):
        return round((self._to_reserve / self._total_vouchers * 100), 2)

    @property
    def to_exchange(self):
        return self._to_exchange

    @to_exchange.setter
    def to_exchange(self, value):
        self._to_exchange = value

    @property
    def to_exchange_max(self):
        return self._total_vouchers - (self._to_sanatorium + self._to_reserve + self.sum_to_medical_units)

    @property
    def to_exchange_percent(self):
        return round((self._to_exchange / self._total_vouchers * 100), 2)

    @property
    def medical_units(self):
        """Возвращает кол-во медсанчастей"""
        return self._medical_units

    @medical_units.setter
    def medical_units(self, value):
        """Устанавливает кол-во медсанчастей"""
        self._medical_units = value

    @property
    def to_medical_unit_max(self):
        """Вычисляем максимальное значение для распределения в конкретную МСЧ"""
        return self._total_vouchers - (
                    self._to_sanatorium + self._to_reserve + self._to_exchange + self.sum_to_medical_units)

    def to_medical_unit_percent(self, id: int):
        """Вычисляем процент путёвок в указанную МСЧ"""
        return round((self.to_medical_units[id] / self._total_vouchers * 100), 2)

    @property
    def sum_to_medical_units(self):
        return sum(self.to_medical_units.values())
