from ..gold.company_have_more_revenue.company_have_more_revenue_gold import \
    CompanyHaveMoreRevenueGold
from ..gold.employee_per_senator.employee_per_senator_gold import \
    EmployeePerSenatorGold
from ..gold.senators_spent_with_benefits.senators_spent_with_benefits_gold import \
    SenatorsSpentWithBenefitsGold
from ..utils.reponse_generator import generateResponse


class BaseGold():
    def __init__(self, spark, className) -> None:
        self.spark = spark
        self.className = className
    
    def execute(self):
        if self.className == "senators_spent_with_benefits":
            return SenatorsSpentWithBenefitsGold(spark=self.spark).execute()
        if self.className == "company_have_more_revenue":
            return CompanyHaveMoreRevenueGold(spark=self.spark).execute()
        if self.className == "employee_per_senator":
            return EmployeePerSenatorGold(spark=self.spark).execute()
        return generateResponse("ERROR", "Invalid parameter")
        