use MarketingCompaign
--select * from products
--Query : Categorize products based on their price
select 
ProductID,ProductName,Price,Category,
case when Price<50 then 'LOW'
when Price between 50 and 200 then 'Medium'
else 'High' 
end Price_category
from products

-- sql statement to join customers with dim_geography to enrich custopmer data with geographic information
SELECT 
	c.CustomerID,
	c.CustomerName,
	c.Email,
	c.Gender,
	c.age,
	g.Country,
	g.City
FROM  customers c LEFT JOIN   geography g On c.GeographyID=g.GeographyID

--SQL statement to clean whitespace issue in the ReviewText column
SELECT 
	ReviewID,
	CustomerID,
	ProductID,
	ReviewDate,
	Rating,
	replace(ReviewText,'  ',' ')
FROM customer_reviews

-- QUERY TO CLEAN AND NORMALIZE THE ENGAGEMENT DATA TABLE
with cte as (
SELECT 
EngagementID,
ContentID,
CampaignID,
ProductID,
UPPER(replace(ContentType,'SOCIALMEDIA','Social Media')) as content_type,
left(ViewsClicksCombined,charindex('-',viewsClicksCombined)-1) as views,
RIGHT(ViewsClicksCombined,LEN(ViewsClicksCombined) - CHARINDEX('-',ViewsClicksCombined)) as clicks,
Likes,
format(convert(date,EngagementDate),'dd-MM-yyyy') as EngagementDate
FROM engagement_data
where ContentType!='NEWSLETTER'
)select clicks,SUM(cast(clicks as int)) over() as total_click from cte 

-- Common Table Expression(CTE) to identify the duplicate records
with DuplicatedRecords as (
SELECT 
JourneyID,
CustomerID,
ProductID,
VisitDate,
Stage,
Action,
Duration,
ROW_NUMBER() over(partition by CustomerID,ProductID,VisitDate,Stage,Action order by journeyID ) as rn
FROM customer_journey)
-- indicate the duplicates record to understand data
select * from DuplicatedRecords where rn>1 order by JourneyID

-- second approach
SELECT 
CustomerID,
ProductID,
VisitDate,
Stage,
Action,
Duration,
count(JourneyID) as no_repetion
--ROW_NUMBER() over(partition by CustomerID,ProductID,VisitDate,Stage,Action order by journeyID ) as rn
FROM customer_journey
group by CustomerID,
ProductID,
VisitDate,
Stage,
Action,
Duration
having COUNT(*)=2

-- OUTER query To select the final cleansed and standardized data
select 
JourneyID,
CustomerID,
ProductID,
VisitDate,
Stage,
Action,
coalesce(Duration,avg_duration) as Duration
FROM
(SELECT --subquery to clean the data
JourneyID,
CustomerID,
ProductID,
VisitDate,
upper(Stage) as Stage,
Action,
Duration,
AVG(Duration) over(Partition by Visitdate) as Avg_duration,
ROW_NUMBER() over(partition by customerID,ProductID,Visitdate,upper(stage),Action
order by journeyID) as Row_num
FROM customer_journey) as subquery
where Row_num=1 


select * from customer_journey order by CustomerID,ProductID,VisitDate



select * from fact_customer_reviews_with_sentiment


select * from engagement_data










