use hiveDB;

--2. Most employee is working in which department

--select Department, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY department ORDER BY 
--COUNT_EMP DESC;

--3. Highest number of job roles

--select JobRole, count(JobRole) as JobRole_Count from parquet_EmpAnalysis GROUP BY JobRole ORDER BY
--JobRole_Count DESC;

--4. Which gender have higher strength as workforce?

--select Gender, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY Gender ORDER BY COUNT_EMP DESC;

--5. Compare the marital status of employee and find the most frequent status.

--select MaritalStatus, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY MaritalStatus ORDER BY 
--COUNT_EMP desc;

--6. Mostly hired employee have qualification

--select Education, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY Education ORDER BY
--COUNT_EMP DESC;

--7. Find the count of employee from which education fields

--select EducationField, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY EducationField ORDER BY COUNT_EMP DESC;

--8. What is the job satisfaction level of employee?

--select JobSatisfaction, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY JobSatisfaction ORDER BY
--COUNT_EMP DESC;

--9. Does most of employee do overtime: Yes or No?

--select OverTime, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY OverTime ORDER BY
--COUNT_EMP DESC;

--10. Find Min & Max Salaried employees.

--SELECT EmployeeID, Income FROM parquet_EmpAnalysis order by Income;
--SELECT min(Income), max(Income) FROM parquet_EmpAnalysis;

--11. Does most of the employee do business travel? Find of the employees counts for each category

--select BusinessTravel, count(*) as COUNT_EMP from parquet_EmpAnalysis GROUP BY BusinessTravel ORDER BY
--COUNT_EMP DESC;

--12. Find the AVG salary of graduate employee.

--select Education, avg(Income) as Avg_Sal from parquet_EmpAnalysis GROUP BY Education ORDER BY
--Avg_Sal DESC;

--13. Find the employee qualification receiving salary lower than equal to avg. salary of all employee.

--select distinct(Education) from parquet_EmpAnalysis t1, (select avg(Income) as avg_sal from parquet_EmpAnalysis) t2 
--where t1.Income <= t2.avg_sal;

--14. When does the employee have highest chance of promotion in terms of working year?

--alter table parquet_EmpAnalysis add column promotion_group varchar(30);

--update parquet_EmpAnalysis 
--SET promotion_group = if ( YearsSinceLastPromotion <= 5, '<=5', 
--if (YearsSinceLastPromotion > 10, '10+', '6-10'));

--select promotion_group, COUNT(*) as count_num from hremployee GROUP BY promotion_group; 

--15. Highest attrition is in which department? Display this in percentage as well.

--select Department, sum(if(attrition = 'Yes',1 ,0)) as count_attr, 
round(SUM(if(attrition = 'Yes', 1, 0))/count(*), 2) as attr_rate from parquet_EmpAnalysis 
group by Department order by count_attr desc;

--16. Show marital status of Person having highest attrition rate

--select MaritalStatus, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'Yes', 1, 0))*100/count(*), 2) as attr_rate from
parquet_EmpAnalysis group by MaritalStatus order by count_attr desc;

