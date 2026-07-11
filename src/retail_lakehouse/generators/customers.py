"""Customer synthetic data generator."""

from __future__ import annotations

import pandas as pd

from retail_lakehouse.generators.base import BaseGenerator
from retail_lakehouse.utils.helpers import generate_id_sequence, weighted_choice


class CustomerGenerator(BaseGenerator):
    """Generate realistic customer master records."""

    def generate(self) -> pd.DataFrame:
        """
        Generate customer records with geographic and segment attributes.

        Returns:
            DataFrame aligned to ``retail.customers`` schema.
        """
        count = self.config.num_customers
        customer_ids = generate_id_sequence(1, count)

        records: list[dict[str, object]] = []
        signup_start = self.config.order_start_date
        signup_end = self.config.order_end_date

        for customer_id in customer_ids:
            country = self._choose_country()
            first_name = self.faker.first_name()
            last_name = self.faker.last_name()
            email = self._build_email(first_name, last_name, int(customer_id))
            signup_date = self.faker.date_between(
                start_date=signup_start,
                end_date=signup_end,
            )

            records.append(
                {
                    "customer_id": int(customer_id),
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "phone": self.faker.phone_number()[:30],
                    "country_code": country.code,
                    "country_name": country.name,
                    "city": self.faker.city(),
                    "postal_code": self.faker.postcode(),
                    "customer_segment": weighted_choice(
                        self.rng, self.config.customer_segments
                    ),
                    "signup_date": signup_date,
                    "is_active": bool(self.rng.random() > 0.05),
                }
            )

        df = pd.DataFrame.from_records(records)
        return self._add_audit_columns(df)

    def _choose_country(self):
        """Select a country using configured weights."""
        from retail_lakehouse.utils.helpers import choose_weighted

        return choose_weighted(
            self.rng,
            self.config.countries,
            [country.weight for country in self.config.countries],
        )

    @staticmethod
    def _build_email(first_name: str, last_name: str, customer_id: int) -> str:
        """Create a deterministic but realistic email address."""
        local_part = f"{first_name}.{last_name}.{customer_id}".lower().replace(" ", "")
        return f"{local_part}@example-retail.com"
